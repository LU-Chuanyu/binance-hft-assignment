"""本机实时采集体验页；点击开始后才连接 Binance，页面数据来自真实日志。"""

import argparse
from collections import Counter, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import time


STREAM_KINDS = {'btcusdt@depth@100ms': 'depth', 'btcusdt@aggTrade': 'aggTrade',
                'btcusdt@bookTicker': 'bookTicker'}


class MonitorState:
    def __init__(self, duration):
        self.lock = threading.Lock()
        self.duration = duration
        self.state = 'ready'
        self.started = None
        self.ended = None
        self.path = None
        self.error = None
        self.counts = Counter({'depth': 0, 'aggTrade': 0, 'bookTicker': 0})
        self.messages = deque(maxlen=12)
        self.events = deque(maxlen=8)
        self.connection = {'connected': False, 'generation': 0}
        self.best = {}
        self.local_top = {}
        self.clock = None
        self.validation = None
        self.last_update_id = None
        self.checkpoints = 0
        self.gaps = 0
        self.snapshot_id = None
        self.collection_seconds = None
        self._disk_at = 0
        self._disk = {'files': 0, 'bytes': 0}

    def start(self, path):
        with self.lock:
            if self.state != 'ready':
                return False
            self.path = str(path)
            self.started = time.monotonic()
            self.state = 'connecting'
            return True

    def observe(self, row):
        kind = row['type']
        with self.lock:
            if kind == 'raw':
                envelope = json.loads(row['raw_text'])
                stream = envelope.get('stream')
                label = STREAM_KINDS.get(stream, stream or 'unknown')
                self.counts[label] += 1
                data = envelope.get('data', {})
                self.messages.appendleft({'receive_seq': row['receive_seq'], 'kind': label,
                    'received_ns': row['received_ns'], 'raw_text': row['raw_text'], 'data': data})
                if label == 'bookTicker':
                    self.best = {'bid_price': data.get('b'), 'bid_quantity': data.get('B'),
                                 'ask_price': data.get('a'), 'ask_quantity': data.get('A')}
            elif kind == 'connection_open':
                self.connection = {'connected': True, 'generation': row['generation']}
            elif kind == 'connection_lost':
                self.connection['connected'] = False
                self.gaps += 1
            elif kind in ('snapshot', 'snapshot_reset'):
                self.snapshot_id = row['snapshot']['lastUpdateId']
            elif kind == 'depth_action':
                self.last_update_id = row['after_id']
                if row['action'] == 'applied' and self.state in ('connecting', 'ready'):
                    self.state = 'running'
            elif kind == 'book_checkpoint':
                self.checkpoints += 1
                self.last_update_id = row['last_update_id']
                self.local_top = row['top']
            elif kind in ('depth_gap', 'checkpoint_gap'):
                self.gaps += 1
            elif kind == 'clock_sample':
                self.clock = {k: row.get(k) for k in ('status', 'usable', 'rtt_ns', 'offset_ns', 'error')}
            elif kind == 'book_validation':
                self.validation = {k: row.get(k) for k in ('status', 'compared', 'reason')}
            elif kind == 'end':
                self.state = 'finishing' if row['status'] == 'success' else 'failed'
                self.error = row.get('reason')
                a, b = row.get('started_monotonic_ns'), row.get('stopped_monotonic_ns')
                if a is not None and b is not None:
                    self.collection_seconds = (b - a) / 1e9
            if kind not in ('raw', 'depth_action', 'trade_live', 'book_checkpoint'):
                self.events.appendleft({'type': kind, 'status': row.get('status'),
                    'detail': row.get('reason') or row.get('error') or '', 'at': time.time()})

    def finish(self):
        with self.lock:
            self.state = 'finished'
            self.ended = time.monotonic()

    def fail(self, error):
        with self.lock:
            self.state = 'failed'
            self.error = f'{type(error).__name__}: {error}'
            self.ended = time.monotonic()

    def snapshot(self):
        now = time.monotonic()
        with self.lock:
            if self.path and now - self._disk_at >= 1:
                files = list(Path(self.path).rglob('*.csv.gz'))
                self._disk = {'files': len(files), 'bytes': sum(p.stat().st_size for p in files)}
                self._disk_at = now
            elapsed = 0 if self.started is None else (self.ended or now) - self.started
            return {'state': self.state, 'duration': self.duration, 'elapsed': round(elapsed, 1),
                    'path': self.path, 'error': self.error, 'counts': dict(self.counts),
                    'total_messages': sum(self.counts.values()), 'messages': list(self.messages),
                    'events': list(self.events), 'connection': dict(self.connection),
                    'best': dict(self.best), 'local_top': self.local_top, 'clock': self.clock,
                    'validation': self.validation, 'last_update_id': self.last_update_id,
                    'checkpoints': self.checkpoints, 'gaps': self.gaps,
                    'snapshot_id': self.snapshot_id, 'collection_seconds': self.collection_seconds,
                    'disk': dict(self._disk)}


class MonitorOutput:
    def __init__(self, output, state):
        self.output, self.state = output, state

    def write(self, text):
        row = json.loads(text)
        result = self.output.write(text)
        # 提交后台存储成功后再更新界面；已发布文件另外统计。
        self.state.observe(row)
        return result


class MonitorController:
    def __init__(self, duration, output_root):
        self.state = MonitorState(duration)
        self.output_root = Path(output_root)
        self.stream = None
        self.thread = None
        self.lock = threading.Lock()

    def start(self):
        with self.lock:
            if self.state.state != 'ready':
                return False
            path = self.output_root / f'实时体验_{time.time_ns()}'
            if not self.state.start(path):
                return False
            self.thread = threading.Thread(target=self._capture, args=(path,), name='live-demo', daemon=True)
            self.thread.start()
            return True

    def _capture(self, path):
        from api_client import PublicAPIClient
        from integrated_collector import collect
        from partition_journal import PartitionJournal
        from partition_store import PartitionStore
        from reconnect_stream import ReconnectingStream
        from trade_flow import TradeFlow

        market = None
        try:
            path.mkdir(parents=True, exist_ok=False)
            api = PublicAPIClient()
            self.stream = ReconnectingStream()
            market = PartitionStore(path / 'market')
            with PartitionJournal(path / 'journal') as output:
                collect(self.stream, lambda: api.depth(limit=1000).data,
                    TradeFlow(lambda first, limit: api.aggtrades(first, limit).data),
                    MonitorOutput(output, self.state), market_store=market,
                    clock_query=api.server_time, validation_query=lambda: api.depth(limit=1000).data,
                    duration=self.state.duration)
            self.state.finish()
        except BaseException as exc:
            self.state.fail(exc)
        finally:
            if self.stream is not None:
                self.stream.close()
            if market is not None:
                try:
                    market.close()
                except Exception as exc:
                    self.state.fail(exc)

    def close(self):
        if self.stream is not None:
            self.stream.close()


HTML = r'''<!doctype html>
<html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Binance 真实行情 · 采集现场</title>
<style>
:root{color-scheme:dark;--bg:#10131a;--panel:#191e28;--line:#2a3241;--text:#eef2f8;--muted:#9ba9bd;--yellow:#f4ce53;--green:#75d9ad;--red:#f59b9b;--blue:#8db7fc}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(ellipse at top right,#243040 0,transparent 47%),var(--bg);font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;color:var(--text)}main{max-width:1350px;margin:auto;padding:30px 30px 44px}.eyebrow{font-size:12px;letter-spacing:2px;color:var(--yellow);font-weight:700}header{display:flex;justify-content:space-between;gap:24px;align-items:center}h1{font-size:30px;margin:8px 0 9px;letter-spacing:-.5px}.subtitle{color:var(--muted);font-size:14px;line-height:1.7}.controls{display:flex;gap:10px;align-items:center;white-space:nowrap}button{border:1px solid var(--line);background:#242c39;color:var(--text);border-radius:9px;padding:12px 18px;font-weight:650;cursor:pointer}button.primary{background:var(--yellow);border-color:var(--yellow);color:#17191c}button:disabled{opacity:.5;cursor:default}.badge{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--line);border-radius:30px;padding:8px 12px;color:var(--muted);font-size:12px}.dot{width:7px;height:7px;border-radius:50%;background:#788599}.dot.live{background:var(--green);box-shadow:0 0 12px #75d9ad88}.statusbar{margin:24px 0 18px;padding:15px 18px;background:#1c2430;border:1px solid var(--line);border-radius:12px;display:flex;justify-content:space-between;gap:20px;align-items:center}.statusbar strong{font-size:15px}.statusbar small{display:block;margin-top:5px;color:var(--muted)}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.card,.panel{background:#191e28e8;border:1px solid var(--line);border-radius:13px}.card{padding:18px 20px}.label{color:var(--muted);font-size:12px}.number{font-size:34px;letter-spacing:-1px;font-variant-numeric:tabular-nums;font-weight:650;margin:9px 0 4px}.card .detail{font-size:12px;color:var(--muted)}.card:nth-child(2) .number{color:var(--blue)}.card:nth-child(3) .number{color:var(--green)}.card:nth-child(4) .number{color:var(--yellow)}.grid{display:grid;grid-template-columns:minmax(0,1.6fr) minmax(310px,1fr);gap:18px;margin-top:18px}.panel{padding:20px}.panel-head{display:flex;justify-content:space-between;gap:15px;align-items:center;margin-bottom:15px}.panel-head h2{font-size:16px;margin:0}.panel-head span{color:var(--muted);font-size:11px}.pipeline{display:flex;align-items:center;gap:8px;margin:0 0 17px;flex-wrap:wrap}.step{font-size:11px;background:#242c39;border-radius:6px;padding:7px 10px;color:var(--muted)}.arrow{color:#65758b}.stream{display:inline-block;padding:4px 8px;border-radius:5px;font-size:11px;min-width:76px;text-align:center;background:#27374c;color:var(--blue)}.stream.aggTrade{background:#233c34;color:var(--green)}.stream.bookTicker{background:#403a24;color:var(--yellow)}.empty{color:var(--muted);font-size:14px;text-align:center;padding:80px 25px;line-height:1.9}.feed{max-height:530px;overflow:auto}.message{padding:12px 0;border-top:1px solid var(--line)}.msg-head{display:flex;justify-content:space-between;gap:8px;align-items:center;font-size:11px;color:var(--muted)}.msg-summary{margin:8px 0 0;font-size:12px;line-height:1.65;color:#ced8e6;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;word-break:break-word}details{margin-top:7px}summary{font-size:11px;color:var(--muted);cursor:pointer}pre{margin:9px 0 3px;white-space:pre-wrap;word-break:break-all;line-height:1.6;color:#c5d2e3;background:#121822;padding:12px;border-radius:8px;font-size:11px}.row{display:flex;justify-content:space-between;gap:12px;align-items:start;padding:12px 0;border-bottom:1px solid var(--line);font-size:12px}.row:last-child{border-bottom:0}.value{text-align:right;font-variant-numeric:tabular-nums;color:#e7edf7;max-width:65%;word-break:break-all}.hint{font-size:11px;color:var(--muted);line-height:1.7;margin:12px 0 0}.good{color:var(--green)}.warn{color:var(--yellow)}.bad{color:var(--red)}.bidask{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:15px}.price{background:#121822;border-radius:8px;padding:13px}.price strong{font-size:22px;display:block;margin:6px 0;font-variant-numeric:tabular-nums}.price small{font-size:11px;color:var(--muted)}.bid strong{color:var(--green)}.ask strong{color:var(--red)}.path{font-family:ui-monospace,SFMono-Regular,monospace;font-size:10px;line-height:1.7;word-break:break-all;color:var(--muted);background:#121822;border-radius:8px;padding:11px;margin-top:13px}.error{display:none;background:#482d32;color:#ffd7d7;padding:14px;border-radius:9px;margin-top:14px;font-size:12px;white-space:pre-wrap}.footer{font-size:11px;color:var(--muted);margin-top:20px;line-height:1.8}@media(max-width:900px){header{align-items:start;flex-direction:column}.grid{grid-template-columns:1fr}.cards{grid-template-columns:repeat(2,1fr)}main{padding:24px 18px}.number{font-size:28px}.statusbar{align-items:start;flex-direction:column}}
</style>
<main><header><div><div class="eyebrow">KID PROJECT / MARKET DATA LAB</div><h1>真实行情，正在进入你的电脑。</h1><div class="subtitle">BTC / USDT · Binance 现货公开行情 · 点击开始后才建立真实连接</div></div><div class="controls"><button id="start" class="primary">开始 60 秒体验</button><button id="pause" disabled>暂停画面</button></div></header>
<div class="statusbar"><div><strong id="statusTitle">准备就绪，等待开始</strong><small id="statusText">这不是历史回放。消息到达、处理、保存都会在下面显示。</small></div><div class="badge"><i id="dot" class="dot"></i><span id="timer">尚未连接</span></div></div>
<div class="cards"><div class="card"><div class="label">真实接收消息 · 总计</div><div class="number" id="total">0</div><div class="detail">已进入处理与保存流程</div></div><div class="card"><div class="label">DEPTH · 盘口变化</div><div class="number" id="depth">0</div><div class="detail">100ms 更新流，可含多个价位</div></div><div class="card"><div class="label">AGGTRADE · 聚合成交</div><div class="number" id="aggTrade">0</div><div class="detail">实际发生成交时推送</div></div><div class="card"><div class="label">BOOK TICKER · 买一卖一</div><div class="number" id="bookTicker">0</div><div class="detail">最优价格或数量更新时推送</div></div></div>
<div class="grid"><section class="panel"><div class="panel-head"><h2>消息现场</h2><span id="feedLabel">最近 12 条真实原文 · 每 0.5 秒更新画面</span></div><div class="pipeline"><span class="step">Binance 推送</span><span class="arrow">→</span><span class="step">本机接收与打时</span><span class="arrow">→</span><span class="step">处理 / 排队写盘</span><span class="arrow">→</span><span class="step">压缩分片</span></div><div class="feed" id="feed"><div class="empty">点击「开始 60 秒体验」<br>这里将出现你电脑此刻收到的原始消息。<br><small>采集频率高于画面刷新率，画面只展示最近 12 条。</small></div></div></section>
<aside><section class="panel"><div class="panel-head"><h2>最新收到的买一 / 卖一</h2><span>来自 Book Ticker</span></div><div class="bidask"><div class="price bid"><div class="label">买一 · USDT</div><strong id="bid">—</strong><small id="bidQty">数量 — BTC</small></div><div class="price ask"><div class="label">卖一 · USDT</div><strong id="ask">—</strong><small id="askQty">数量 — BTC</small></div></div><div class="row"><span class="label">连接代次</span><span class="value" id="connection">等待连接</span></div><div class="row"><span class="label">本地盘口进度</span><span class="value" id="bookId">等待快照</span></div><div class="row"><span class="label">已保存盘口摘要</span><span class="value" id="checkpoints">0</span></div><div class="row"><span class="label">在线盘口对账</span><span class="value" id="validation">尚未触发（默认 30 秒一次）</span></div><div class="row"><span class="label">校时请求</span><span class="value" id="clock">尚未完成</span></div><p class="hint">看到的是接收端真实数据；盘口进度来自程序的处理记录。在线对账只比较有足够证据的价位。</p></section><section class="panel" style="margin-top:18px"><div class="panel-head"><h2>文件正在落盘</h2><span>仅统计已发布文件</span></div><div class="row"><span class="label">压缩分片</span><span class="value" id="files">0 个</span></div><div class="row"><span class="label">已发布文件大小</span><span class="value" id="bytes">0 B</span></div><div class="row"><span class="label">断线 / 缺口 / 摘要遗漏记录</span><span class="value" id="gaps">0</span></div><p class="hint">后台按批量和时间写盘，所以文件数量会阶梯式增长。接收到消息不等于它已立即落到磁盘。</p><div class="path" id="path">开始后显示本次数据目录。</div></section></aside></div><div class="error" id="error"></div><div class="footer">仅使用 Binance 公开行情，不需要 API Key。暂停画面只暂停显示，实际采集继续。采集结束后页面保留结果；短录结束还需要回放与文件对账，不能单凭此页认定作业通过。</div></main>
<script>
let paused=false, latest=null;
const $=id=>document.getElementById(id), fmt=n=>Number(n||0).toLocaleString('en-US');
function bytes(n){if(n<1024)return n+' B';if(n<1048576)return(n/1024).toFixed(1)+' KB';return(n/1048576).toFixed(2)+' MB'}
function price(v){return v==null?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:8})}
function el(tag,cls,text){const x=document.createElement(tag);if(cls)x.className=cls;if(text!==undefined)x.textContent=text;return x}
function describe(m){let d=m.data;if(m.kind==='depth')return `更新范围 ${d.U} → ${d.u} · 买盘变化 ${d.b?.length||0} 档 / 卖盘变化 ${d.a?.length||0} 档`;if(m.kind==='aggTrade')return `成交 #${d.a} · ${d.p} USDT × ${d.q} BTC`;return `买一 ${d.b} × ${d.B} · 卖一 ${d.a} × ${d.A}`}
function render(s){latest=s;if(paused)return;['depth','aggTrade','bookTicker'].forEach(k=>$(k).textContent=fmt(s.counts[k]));$('total').textContent=fmt(s.total_messages);$('start').textContent=`开始 ${s.duration} 秒体验`;$('start').disabled=s.state!=='ready';$('pause').disabled=s.state==='ready';$('dot').className='dot'+(['connecting','running','finishing'].includes(s.state)?' live':'');
const titles={ready:'准备就绪，等待开始',connecting:'正在建立连接与同步盘口',running:'正在接收真实行情',finishing:'正在完成写盘',finished:'本次采集已结束，数据已交给文件存储',failed:'本次采集发生错误'};$('statusTitle').textContent=titles[s.state]||s.state;$('timer').textContent=s.state==='ready'?'尚未连接':`启动后 ${s.elapsed.toFixed(1)} 秒`;$('statusText').textContent=s.state==='finished'?`同步后实际采集 ${s.collection_seconds?.toFixed(3)??'—'} 秒；下面保留的是本次真实记录。`:s.state==='running'?'数字来自程序此刻处理的真实消息；点击一条消息可以展开原文。':s.state==='connecting'?'建立连接后，先缓存消息，再通过查询接口获取快照。':'画面展示接收、同步、校时和已发布文件的实际状态。';
$('connection').textContent=s.connection.connected?`第 ${s.connection.generation} 次连接`:'尚未连接 / 已断开';$('bookId').textContent=s.last_update_id??(s.snapshot_id?`快照 ${s.snapshot_id}`:'等待快照');$('checkpoints').textContent=fmt(s.checkpoints);$('files').textContent=fmt(s.disk.files)+' 个';$('bytes').textContent=bytes(s.disk.bytes);$('gaps').textContent=fmt(s.gaps);$('path').textContent=s.path||'开始后显示本次数据目录。';$('bid').textContent=price(s.best.bid_price);$('ask').textContent=price(s.best.ask_price);$('bidQty').textContent='数量 '+(s.best.bid_quantity??'—')+' BTC';$('askQty').textContent='数量 '+(s.best.ask_quantity??'—')+' BTC';
if(s.clock){const c=s.clock;let r=c.rtt_ns==null?'':` · RTT ${(c.rtt_ns/1e6).toFixed(1)} ms`;$('clock').textContent=c.status==='ok'?(c.usable?'当前规则内可用':'当前规则内不可用')+r:c.status+r;$('clock').className='value '+(c.usable?'good':'warn')};if(s.validation){let v=s.validation;$('validation').textContent=({matched:'可比价位一致',mismatch:'发现不一致',unverifiable:'证据不足',discarded:'旧轮次结果已弃用',error:'请求或校验错误'}[v.status]||v.status)+(v.compared!=null?` · ${v.compared} 档`:'');$('validation').className='value '+(v.status==='matched'?'good':'warn')};$('error').style.display=s.error?'block':'none';$('error').textContent=s.error||'';
if(s.messages.length){const feed=$('feed');feed.replaceChildren();s.messages.forEach(m=>{let item=el('article','message'),head=el('div','msg-head');head.append(el('span','stream '+m.kind,m.kind));head.append(el('span','',`接收 #${m.receive_seq} · ${new Date(m.received_ns/1e6).toLocaleTimeString('zh-CN',{hour12:false})}`));item.append(head,el('div','msg-summary',describe(m)));let det=el('details'),sum=el('summary','','展开 Binance 原始消息');let pre=el('pre');try{pre.textContent=JSON.stringify(JSON.parse(m.raw_text),null,2)}catch{pre.textContent=m.raw_text}det.append(sum,pre);item.append(det);feed.append(item)})}}
$('start').onclick=async()=>{$('start').disabled=true;try{let r=await fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});let d=await r.json();if(!r.ok)throw Error(d.error||'无法启动');render(d)}catch(e){$('error').style.display='block';$('error').textContent=e.message}};
$('pause').onclick=()=>{paused=!paused;$('pause').textContent=paused?'恢复画面':'暂停画面';$('feedLabel').textContent=paused?'画面已暂停 · 实际录制仍继续':'最近 12 条真实原文 · 每 0.5 秒更新画面';if(!paused&&latest)render(latest)};
async function poll(){try{let r=await fetch('/api/status',{cache:'no-store'});render(await r.json())}catch(e){$('statusTitle').textContent='无法连接本机监控服务';$('statusText').textContent=e.message}setTimeout(poll,500)}poll();
</script></html>'''


def handler_for(controller):
    class Handler(BaseHTTPRequestHandler):
        def send(self, status, body, content_type):
            payload = body.encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(payload)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path == '/':
                self.send(200, HTML, 'text/html; charset=utf-8')
            elif self.path == '/api/status':
                self.send(200, json.dumps(controller.state.snapshot(), ensure_ascii=False), 'application/json; charset=utf-8')
            else:
                self.send(404, 'Not found', 'text/plain; charset=utf-8')

        def do_POST(self):
            if self.path != '/api/start':
                self.send(404, '{}', 'application/json')
                return
            origin = self.headers.get('Origin')
            if origin is not None and origin != f'http://{self.headers.get("Host")}':
                self.send(403, '{"error":"仅允许本机页面操作"}', 'application/json; charset=utf-8')
                return
            length = int(self.headers.get('Content-Length', '0'))
            if length > 1024:
                self.send(413, '{}', 'application/json')
                return
            self.rfile.read(length)
            if not controller.start():
                self.send(409, '{"error":"本次体验已经启动，不能重复开始"}', 'application/json; charset=utf-8')
                return
            self.send(200, json.dumps(controller.state.snapshot(), ensure_ascii=False), 'application/json; charset=utf-8')

        def log_message(self, format, *args):
            pass
    return Handler


def main(argv=None):
    parser = argparse.ArgumentParser(description='本机真实行情采集体验页，点击页面按钮后才开始')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--duration-seconds', type=float, default=60)
    parser.add_argument('--output-root', type=Path, default=Path.home() / 'Documents' / 'KID_Binance_练习数据')
    args = parser.parse_args(argv)
    if not 0 < args.duration_seconds <= 3600:
        parser.error('体验时长必须大于0且不超过3600秒')
    controller = MonitorController(args.duration_seconds, args.output_root)
    server = ThreadingHTTPServer(('127.0.0.1', args.port), handler_for(controller))
    print(f'实时监控页：http://127.0.0.1:{server.server_port}', flush=True)
    print('页面处于待开始；点击按钮后才建立真实行情连接。', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        controller.close()
        server.server_close()


if __name__ == '__main__':
    main()
