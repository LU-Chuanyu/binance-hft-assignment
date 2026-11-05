"""异步请求交易所快照，由处理线程在可信轮次内核对已知价位。"""

from concurrent.futures import ThreadPoolExecutor
import time

from book_validation import compare_exchange_snapshot


class OnlineValidation:
    """单个后台快照任务；submit/poll 由同一盘口处理线程调用。

    fetch_snapshot 必须自行设置有限网络超时；超期任务若已开始则不能强制
    终止，结果被忽略，完成前禁止启动下一任务，避免积压并发请求。
    """

    def __init__(self, fetch_snapshot, *, max_age=15, executor=None, monotonic=time.monotonic):
        if max_age <= 0:
            raise ValueError('max_age 必须为正')
        self.fetch_snapshot = fetch_snapshot
        self.max_age = max_age
        self.monotonic = monotonic
        self._own_executor = executor is None
        self._executor = executor if executor is not None else ThreadPoolExecutor(max_workers=1)
        self._job = None
        self._closed = False

    def _cleanup_retired(self):
        if self._job is not None and self._job['retired'] and self._job['future'].done():
            self._job = None

    def submit(self, *, epoch):
        if self._closed:
            raise RuntimeError('校验器已关闭')
        if type(epoch) is not int or epoch < 1:
            raise ValueError('epoch 必须是正整数')
        self._cleanup_retired()
        if self._job is not None:
            raise RuntimeError('快照校验任务仍在进行中')
        started = self.monotonic()
        self._job = {'epoch': epoch, 'started': started,
                     'future': self._executor.submit(self.fetch_snapshot), 'retired': False}

    def _retire(self, status, reason):
        job = self._job
        job['retired'] = True
        job['future'].cancel()
        result = {'status': status, 'epoch': job['epoch'], 'reason': reason}
        self._cleanup_retired()
        return result

    def poll(self, provenance, *, contiguous):
        """不阻塞；返回 pending/matched/mismatch/unverifiable/discarded/error。

        matched 仅表示符合条件的价位一致，不表示整个交易所盘口一致。
        provenance 与 contiguous 必须由拥有本地盘口的处理线程提供。
        """
        if self._closed:
            raise RuntimeError('校验器已关闭')
        self._cleanup_retired()
        job = self._job
        if job is None or job['retired']:
            return None
        if provenance.epoch != job['epoch']:
            return self._retire('discarded', '盘口已进入新轮次，旧快照结果不能使用')
        if contiguous is not True:
            return self._retire('unverifiable', '本地盘口不具备可信连续状态')
        if self.monotonic() - job['started'] > self.max_age:
            return self._retire('unverifiable', '等待请求或本地进度对齐已超过时限')
        if not job['future'].done():
            return {'status': 'pending', 'epoch': job['epoch'], 'reason': '快照请求进行中'}
        try:
            snapshot = job['future'].result()
            snapshot_id = snapshot['lastUpdateId']
            if type(snapshot_id) is not int or snapshot_id < 0:
                raise ValueError('交易所快照编号无效')
            if snapshot_id > provenance.current_id:
                return {'status': 'pending', 'epoch': job['epoch'], 'snapshot_id': snapshot_id,
                        'local_id': provenance.current_id, 'reason': '等待本地盘口追上快照'}
            comparison = compare_exchange_snapshot(snapshot, provenance.known,
                provenance.last_changes, initial_id=provenance.initial_id,
                local_id=provenance.current_id, contiguous=contiguous)
        except Exception as exc:
            return self._retire('error', f'{type(exc).__name__}: {exc}')
        result = {'epoch': job['epoch'], 'snapshot_id': snapshot_id,
                  'local_id': provenance.current_id, 'snapshot': snapshot, **comparison}
        if not comparison['eligible'] or not comparison['compared']:
            result['status'] = 'unverifiable'
            result['reason'] = comparison.get('reason', '没有足够证据比较任何价位')
        elif comparison['matches']:
            result['status'] = 'matched'
        else:
            result['status'] = 'mismatch'
        self._job = None
        return result

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._job is not None:
            self._job['future'].cancel()
        if self._own_executor:
            self._executor.shutdown(wait=False, cancel_futures=True)
