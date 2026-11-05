"""异步单任务的有限限频重试：由处理线程轮询，不在处理线程 sleep。"""

from concurrent.futures import ThreadPoolExecutor
import math
import time

from api_client import RateLimitError, RateLimitWait


class NonblockingRetry:
    """支持单任务，限频按重试时间等待；需要调用方继续轮询。

    token 代表任务所属的连接代次或盘口轮次；任务函数自身必须设置网络
    超时。线程中正在运行的请求不能被强制取消，因此丢弃后其未结束前不可
    提交另一个任务。无 Retry-After 时不会猜测等待时间。
    """

    def __init__(self, task, *, max_attempts=3, max_age=60, attempt_timeout=None, executor=None,
                 monotonic=time.monotonic):
        if (type(max_attempts) is not int or max_attempts < 1
                or isinstance(max_age, bool) or not isinstance(max_age, (int, float))
                or not math.isfinite(max_age) or max_age <= 0
                or (attempt_timeout is not None and (not math.isfinite(attempt_timeout) or attempt_timeout <= 0))):
            raise ValueError('最大尝试次数或超时无效')
        self.task = task
        self.max_attempts = max_attempts
        self.max_age = max_age
        self.attempt_timeout = attempt_timeout
        self.monotonic = monotonic
        self._own_executor = executor is None
        self._executor = executor if executor is not None else ThreadPoolExecutor(max_workers=1)
        self._job = None
        self._closed = False

    def _clean_retired(self):
        if self._job is not None and self._job['retired']:
            future = self._job['future']
            if future is None or future.done():
                self._job = None

    def start(self, *, token, deadline=None):
        if self._closed:
            raise RuntimeError('重试器已关闭')
        self._clean_retired()
        if self._job is not None:
            raise RuntimeError('已有任务进行中')
        now = self.monotonic()
        if deadline is not None and (isinstance(deadline, bool) or not isinstance(deadline, (int, float))
                                     or not math.isfinite(deadline) or deadline <= now):
            raise ValueError('deadline 必须是未来的单调时钟时间')
        future = self._executor.submit(self.task)
        cutoff = min(now + self.max_age, deadline) if deadline is not None else now + self.max_age
        self._job = {'token': token, 'deadline': cutoff, 'attempts': 1, 'attempt_started': now,
                     'future': future, 'next_retry': None, 'last_error': None, 'retired': False}

    def _finish(self, status, **details):
        job = self._job
        result = {'status': status, 'token': job['token'], 'attempts': job['attempts'], **details}
        future = job['future']
        if future is not None and not future.done():
            job['retired'] = True
            future.cancel()
            self._clean_retired()
        else:
            self._job = None
        return result

    @staticmethod
    def _error(exc):
        return {'type': type(exc).__name__, 'message': str(exc),
                'status': getattr(exc, 'status', getattr(exc, 'code', None)),
                'retry_after': getattr(exc, 'retry_after', getattr(exc, 'remaining', None))}

    def poll(self, *, token, active=True, deadline=None):
        """返回 pending/retry_wait/success/error/exhausted/discarded 或 None。"""
        if self._closed:
            raise RuntimeError('重试器已关闭')
        self._clean_retired()
        job = self._job
        if job is None or job['retired']:
            return None
        if not active or token != job['token']:
            return self._finish('discarded', reason='连接或盘口轮次已失效', error=job['last_error'])
        if deadline is not None:
            job['deadline'] = min(job['deadline'], deadline)
        now = self.monotonic()
        if now >= job['deadline'] and (job['future'] is None or not job['future'].done()):
            return self._finish('exhausted', reason='超过截止时间', error=job['last_error'])

        if job['future'] is None:
            if job['next_retry'] >= job['deadline']:
                return self._finish('exhausted', reason='截止时间早于下次可重试时间', error=job['last_error'])
            remaining = max(0.0, job['next_retry'] - now)
            if remaining > 0:
                return {'status': 'retry_wait', 'token': token, 'attempts': job['attempts'],
                        'remaining': remaining, 'error': job['last_error']}
            try:
                job['future'] = self._executor.submit(self.task)
                job['attempts'] += 1
                job['attempt_started'] = now
            except Exception as exc:
                return self._finish('error', error=self._error(exc), cause=exc)
            return {'status': 'pending', 'token': token, 'attempts': job['attempts']}

        if not job['future'].done():
            if self.attempt_timeout is not None and now - job['attempt_started'] >= self.attempt_timeout:
                return self._finish('exhausted', reason='单次请求超时', error=job['last_error'])
            return {'status': 'pending', 'token': token, 'attempts': job['attempts']}
        try:
            result = job['future'].result()
        except (RateLimitError, RateLimitWait) as exc:
            error = self._error(exc)
            job['last_error'] = error
            delay = error['retry_after']
            if (isinstance(delay, bool) or not isinstance(delay, (int, float))
                    or not math.isfinite(delay) or delay < 0):
                return self._finish('error', error=error, cause=exc, reason='限频等待时间未知或无效')
            if job['attempts'] >= self.max_attempts or now + delay >= job['deadline']:
                return self._finish('exhausted', error=error, cause=exc, reason='重试次数或截止时间不足')
            job['future'] = None
            job['next_retry'] = now + delay
            return {'status': 'retry_wait', 'token': token, 'attempts': job['attempts'],
                    'remaining': delay, 'error': error, 'cause': exc}
        except Exception as exc:
            return self._finish('error', error=self._error(exc), cause=exc)
        return self._finish('success', result=result)

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._job is not None and self._job['future'] is not None:
            self._job['future'].cancel()
        if self._own_executor:
            self._executor.shutdown(wait=False, cancel_futures=True)
