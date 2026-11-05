"""从已完成 JSONL 教学日志生成 Markdown 报告和有数据支持的图。"""

import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import time

from quality_report import ClockWindow, summarize_file


def _create_plots(summary, samples, output):
    # 少于一分钟的观察或不足两个已观察分钟，不画容易误导的曲线。
    plot_minutes = (summary['observed_elapsed_seconds'] is not None
                    and summary['observed_elapsed_seconds'] >= 60
                    and len(summary['minute_counts_utc']) >= 2)
    # 使用 20 条作为展示直方图的最低样本数，并在报告中明确不是充分性证明。
    plot_latency = len(samples) >= 20
    if not plot_minutes and not plot_latency:
        return False, False, None
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        return False, False, 'matplotlib 未安装，无法生成图；请安装依赖后重试。'

    if plot_minutes:
        times = [datetime.strptime(minute, '%Y-%m-%dT%H:%MZ') for minute in summary['minute_counts_utc']]
        values = list(summary['minute_counts_utc'].values())
        figure, axis = plt.subplots(figsize=(8, 4))
        axis.plot(times, values, marker='o')
        axis.set_title('Recorded messages per observed minute (UTC)')
        axis.set_xlabel('Minute (UTC)')
        axis.set_ylabel('Message count')
        figure.autofmt_xdate()
        figure.tight_layout()
        figure.savefig(output / 'messages_per_minute.png', dpi=150)
        plt.close(figure)
    if plot_latency:
        figure, axis = plt.subplots(figsize=(8, 4))
        axis.hist(samples, bins=min(30, max(5, int(len(samples) ** 0.5))))
        axis.set_title('Estimated event-to-receipt time difference')
        axis.set_xlabel('Difference (ms); includes clock uncertainty')
        axis.set_ylabel('Message count')
        figure.tight_layout()
        figure.savefig(output / 'latency_distribution.png', dpi=150)
        plt.close(figure)
    return plot_minutes, plot_latency, None


def write_report(path, output_dir, *, clock=None, run_id=None):
    """输出新目录中的报告，拒绝覆盖已有目录。返回 Markdown 路径。"""
    summary = summarize_file(path, clock=clock, run_id=run_id, include_samples=True)
    types = Counter(summary['record_types'])
    samples = [value for values in summary['latency_samples_ms'].values() for value in values]
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    minute_plot, latency_plot, plot_error = _create_plots(summary, samples, output)
    counts = summary['counts']
    duration = summary['observed_elapsed_seconds']
    recorded_duration = summary['recorded_duration_seconds']
    clock_connections = summary['clock_connection_counts']
    journal_bytes = summary['file_bytes']
    market_bytes = summary['market_file_bytes']
    total_bytes = journal_bytes + market_bytes if market_bytes is not None else None
    lines = [
        '# 行情采集基础质量报告', '',
        f"数据文件：`{summary['file']}`", '',
        f"数据来源：{summary['source_type']}；回放与检查点一致不证明交易所原始行情完整。", '',
        f'盘口同步后采集计时：{recorded_duration:.3f} 秒' if recorded_duration is not None else
        '盘口同步后采集计时：N/A（日志未保存起止时刻）',
        f"首末消息观察跨度：{duration:.3f} 秒" if duration is not None else '首末消息观察跨度：N/A',
        f"日志文件大小：{journal_bytes} 字节；系统分配空间：{summary['allocated_disk_bytes']} 字节。",
        (f"分类行情文件大小：{market_bytes} 字节；系统分配空间：{summary['market_allocated_disk_bytes']} 字节。"
         if market_bytes is not None else '分类行情文件大小：N/A（来源中没有唯一对应的 market 分片）。'),
        f'本次数据文件合计：{total_bytes} 字节。' if total_bytes is not None else '', '',
        '| 实时原始消息 | 条数 | 观察平均条数/秒 |', '| --- | ---: | ---: |',
    ]
    rates = summary['observed_rate_per_second'] or {}
    for kind, count in counts.items():
        rate = f'{rates[kind]:.3f}' if kind in rates else 'N/A'
        lines.append(f'| {kind} | {count} | {rate} |')
    lines += ['', '### 已观察到的每分钟消息数（UTC）', '', '| 分钟 | 条数 |', '| --- | ---: |']
    for minute, count in summary['minute_counts_utc'].items():
        lines.append(f'| {minute} | {count} |')
    lines += ['', '速率按首末消息时间跨度计算。首尾分钟按实际观察到的部分统计，比较频率时应优先看完整分钟。', '',
              f"盘口缺口记录：{summary['gap_count']}；重置：{summary['reset_count']}；"
              f"应用：{summary['applied_depth']}；忽略：{summary['ignored_depth']}；"
              f"结束时待处理：{summary['pending_depth']}。", '',
              f"断线记录：{types['connection_lost']}" if 'connection_open' in types else '断线统计：N/A（日志未记录连接事件）',
              (f"补回成交记录：{types['trade_backfill']}，其中 {summary['backfill_estimated_count']} 条带估计接收时间。")
              if ('trade_live' in types or 'trade_repair' in types or 'trade_backfill' in types)
              else '补回成交统计：N/A（日志未记录成交补查流程）', '',
              ('在线盘口对账：' + '，'.join(f'{status} {count}' for status, count in summary['validation_counts'].items())
               + '。matched 表示有证据可比的档位一致。' if any(summary['validation_counts'].values()) else
               '在线盘口对账：N/A（日志无对账结果）。'), '',
              (f"逐档回放校验通过率：{summary['checkpoints_passed']}/{summary['checkpoints_checked']} "
               f"（{summary['checkpoints_passed'] / summary['checkpoints_checked'] * 100:.2f}%）；"
               '只覆盖已保存的摘要，不证明整段无缺口。'
               if summary['checkpoints_checked'] else
               '逐档回放校验通过率：N/A（日志没有保存前 20 档检查点）。'), '',
              (f"检查点覆盖不完整：记录 {summary['checkpoint_gap_records']} 次遗漏事件，"
               f"错过 {summary['checkpoint_gaps']} 个计划周期；"
               f"原因（按错过周期数）：{summary['checkpoint_gap_reasons']}。"
               if summary['checkpoint_gaps'] else
               '检查点覆盖完整性：未知（未记录遗漏不等于整段每秒覆盖已通过验收）。'), '',
              '## 事件到本机接收时间差', '', summary['time_difference_note'],
              f"日志校时样本：{summary['clock_sample_count']}；符合规则：{summary['usable_clock_samples']}；"
              f"处理行情时发现本机时钟跳变：{summary['clock_jumps']}。",
              f"日志中的时钟偏差告警：{types['clock_offset_alert']} 条。",
              (f"校时请求 RTT 中位数：{summary['clock_rtt']['median_ms']:.3f} ms；"
               f"最大值：{summary['clock_rtt']['max_ms']:.3f} ms。"
               if summary['clock_rtt']['count'] else '校时请求 RTT：N/A。'),
              (f"已记录的校时 HTTPS 连接：复用 {clock_connections.get('reused', 0)} 次，"
               f"新建 {clock_connections.get('fresh', 0)} 次。" if clock_connections else ''),
              summary['bookTicker_latency_note'], '',
              '| 类型 | 样本数 | 中位数 ms | P99 ms | 最大值 ms |', '| --- | ---: | ---: | ---: | ---: |']
    for kind, stats in summary['latency'].items():
        def fmt(value):
            return 'N/A' if value is None else f'{value:.3f}'
        lines.append(f"| {kind} | {stats['count']} | {fmt(stats['median_ms'])} | {fmt(stats['p99_ms'])} | {fmt(stats['max_ms'])} |")
    lines += ['', '## 10 秒运行统计', '']
    runtime = summary['runtime_stats']
    if runtime is None:
        lines.append('N/A（日志中没有运行统计记录）。')
    else:
        seconds = runtime['elapsed_seconds']
        lines += [f"完整窗口 {runtime['full_windows']} 个，末尾部分窗口 {runtime['partial_windows']} 个，"
                  f"共覆盖 {seconds:.3f} 秒。按采集程序处理消息的单调时钟分窗。", '',
                  '| 类型 | 窗口内处理条数 | 平均条数/秒 | 可用时间差样本 |',
                  '| --- | ---: | ---: | ---: |']
        for kind, count in runtime['counts'].items():
            lines.append(f"| {kind} | {count} | {count / seconds:.3f} | {runtime['delay_sample_counts'][kind]} |")
        lines += ['', '各窗口的最小值、P50、P99 和最大值保存在运行日志的 `runtime_stats` 记录中。']
    lines += ['', '## 图表', '']
    lines.append('![每分钟消息数](messages_per_minute.png)' if minute_plot else
                 '每分钟消息数曲线：N/A（至少需 60 秒观察跨度和两个已观察分钟，或图形依赖不可用）。')
    lines.append('![事件到接收时间差分布](latency_distribution.png)' if latency_plot else
                 '时间差分布图：N/A（需有效校时窗口及至少 20 条可计算样本，或图形依赖不可用）。')
    lines += ['', '图中仅显示实际出现消息的分钟；缺失分钟不自动推定为零。20 条是画图门槛，不证明样本具有统计代表性。']
    if plot_error:
        lines += ['', plot_error]
    lines += ['', '## 一天数据量估算', '']
    estimate_seconds = recorded_duration if recorded_duration is not None else duration
    if estimate_seconds is not None and estimate_seconds >= 1800:
        estimate = (total_bytes if total_bytes is not None else journal_bytes) / estimate_seconds * 86400
        scope = '日志与分类行情分片' if total_bytes is not None else '已选日志'
        lines.append(f'约 {estimate:.0f} 字节，按{scope}字节数 ÷ {estimate_seconds:.3f} 秒 × 86400 秒线性外推。'
                     '这是假定全天速率不变的估计，不同时段、异常和存储格式会改变实际大小。')
    else:
        lines.append('N/A：观察跨度不足 1800 秒；未来可按文件字节数 ÷ 观察秒数 × 86400 外推，并说明其假设。')
    lines += ['', '## 录制时长与验收范围', '']
    if recorded_duration is not None and recorded_duration >= 1800:
        lines.append('日志记录的同步后计时已达到 30 分钟。')
    else:
        lines.append('至少 30 分钟的录制时长尚待验证，以同步后的单调计时为准。')
    lines += ['分小时分类行情对账、摘要覆盖和异常影响需要结合分片及运行日志核对。', '']
    target = output / 'quality_report.md'
    target.write_text('\n'.join(lines), encoding='utf-8')
    return target


def main():
    parser = argparse.ArgumentParser(description='为已完成 JSONL 日志生成教学质量报告')
    parser.add_argument('path', help='JSONL 文件、journal 分片目录或采集运行目录')
    parser.add_argument('--run-id', help='分片目录有多个 run 时选择其中一个')
    parser.add_argument('--out', help='新建报告目录；必须尚不存在')
    parser.add_argument('--clock-window-json', help='调用方确认的校时窗口 JSON，含 start_ns/end_ns/offset_ns/rtt_ns/max_rtt_ns')
    args = parser.parse_args()
    clock = None
    if args.clock_window_json:
        with open(args.clock_window_json, encoding='utf-8') as file:
            clock = ClockWindow(**json.load(file))
    output = (Path(args.out) if args.out else
              Path.home() / 'Documents' / 'KID_Binance_练习数据' / 'reports' / f'report_{time.time_ns()}')
    report = write_report(args.path, output, clock=clock, run_id=args.run_id)
    print(f'报告：{report}')


if __name__ == '__main__':
    main()
