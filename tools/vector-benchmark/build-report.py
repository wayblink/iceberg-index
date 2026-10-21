#!/usr/bin/env python3
"""Builds a self-contained HTML report from the logs run-benchmark.sh writes.

A run is a log file of one JSON object per line, prefixed by what it is:

    BENCHMETA    what was measured: tables, index definition, settings
    BENCHSCAN    one exact query: latency and the rows it returned
    BENCHRESULT  one indexed query: setting, latency, hits, recall against the scan
    BENCHSUMMARY the closing marker, so a truncated run is recognizable

Nothing external is loaded: the charts are inline SVG and the styling is inline CSS, so the file
opens the same on a laptop with no network as it does in a browser tab.

    ./build-report.py results/*.log -o report.html [--title "..."]
"""
import argparse
import glob
import json
import os
import statistics
import sys
from datetime import datetime

PREFIX = {"BENCHMETA": "meta", "BENCHSCAN": "scan", "BENCHRESULT": "result", "BENCHSUMMARY": "summary"}

# one colour per run, cycling; chosen to stay distinguishable in the usual kinds of colour blindness
PALETTE = ["#2f6fd0", "#d1495b", "#2a9d8f", "#e08b1b", "#6a4c93", "#3f7d20", "#b5179e"]


def parse(path):
    run = {"name": os.path.basename(path).replace(".log", ""), "meta": {}, "index": [],
           "scans": [], "results": [], "done": False, "missing": []}
    with open(path, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            for prefix, kind in PREFIX.items():
                if prefix not in line:
                    continue
                payload = line[line.index(prefix) + len(prefix):].strip()
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if kind == "meta":
                    if "index" in data:
                        run["index"].append(data["index"])
                    elif "missing" in data:
                        run["missing"].append(data["query"])
                    else:
                        run["meta"].update(data)
                elif kind == "scan":
                    run["scans"].append(data)
                elif kind == "result":
                    run["results"].append(data)
                elif kind == "summary":
                    if data.get("kind") == "done":
                        run["done"] = True
                    else:
                        run.setdefault("summaries", []).append(data)
                break
    return run


def quartiles(values):
    if not values:
        return [0, 0, 0, 0, 0]
    ordered = sorted(values)
    return [ordered[0], percentile(ordered, 0.25), percentile(ordered, 0.5),
            percentile(ordered, 0.75), ordered[-1]]


def percentile(ordered, fraction):
    index = round(fraction * (len(ordered) - 1))
    return ordered[min(len(ordered) - 1, max(0, index))]


def by_over(run):
    """{over: {"ms": [...], "recall": [...], "found": [...], "hits": [...]}}"""
    table = {}
    for row in run["results"]:
        entry = table.setdefault(row["over"], {"ms": [], "recall": [], "found": [], "hits": []})
        entry["ms"].append(row["ms"])
        entry["recall"].append(row["recall"])
        entry["found"].append(row["found"])
        entry["hits"].append(row["hits"])
    return table


def scan_latencies(run):
    return [row["ms"] for row in run["scans"] if not row.get("warmup")]


# ---------------------------------------------------------------- svg helpers


def esc(text):
    return (str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def log_ticks(low, high):
    """Ticks for a log axis covering [low, high], thinned so the labels do not collide."""
    import math
    decades = math.log10(high) - math.log10(low)
    factors = (1,) if decades > 2.5 else (1, 2, 5)
    ticks = []
    for exponent in range(int(math.floor(math.log10(low))), int(math.ceil(math.log10(high))) + 1):
        for factor in factors:
            value = factor * (10 ** exponent)
            if low * 0.95 <= value <= high * 1.05:
                ticks.append(value)
    return ticks


def nice_ticks(low, high, count=5):
    if high - low < 1e-9:
        high = low + 1.0
    raw = (high - low) / count
    magnitude = 10 ** int(f"{raw:e}".split("e")[1])
    for step in (1, 2, 2.5, 5, 10):
        candidate = step * magnitude
        if raw <= candidate:
            break
    start = (low // candidate) * candidate
    ticks = []
    value = start
    while value <= high + candidate * 0.5:
        ticks.append(round(value, 6))
        value += candidate
    return ticks


class Chart:
    """A minimal line/point chart with an optional band, enough for these measurements."""

    def __init__(self, title, xlabel, ylabel, width=780, height=360, pad=(70, 24, 48, 120),
                 numeric_x=False, log_x=False, log_y=False):
        self.log_x = log_x
        self.log_y = log_y
        self.numeric_x = numeric_x
        self.title = title
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.width = width
        self.height = height
        self.left, self.top, self.bottom, self.right = pad
        self.series = []
        self.bands = []
        self.hlines = []
        self.xlabels = []
        self.y_min = None
        self.y_max = None

    def add_hline(self, y, label, colour="#888888"):
        self.hlines.append((y, label, colour))

    def add_band(self, xs, lows, highs, colour):
        self.bands.append((xs, lows, highs, colour))

    def add_series(self, label, xs, ys, colour, markers=True, error=None):
        self.series.append({"label": label, "xs": xs, "ys": ys, "colour": colour,
                            "markers": markers, "error": error})

    def _bounds(self):
        lows = [y for s in self.series for y in s["ys"] if y is not None]
        highs = list(lows)
        for _, low, high, _ in self.bands:
            lows += [v for v in low if v is not None]
            highs += [v for v in high if v is not None]
        for y, _, _ in self.hlines:
            lows.append(y)
            highs.append(y)
        if not lows:
            return 0.0, 1.0
        low, high = min(lows), max(highs)
        if self.log_y:
            # a log axis is padded by a factor, not by a fraction of the span: a span of two orders
            # of magnitude would push a fractional pad below zero and the axis would have to show
            # ten decades to hold it
            low, high = low / 1.25, high * 1.25
        else:
            span = high - low
            if span < 1e-9:
                span = max(abs(high), 1.0)
            low = low - span * 0.08
            high = high + span * 0.08
        if self.y_min is not None:
            low = self.y_min
        if self.y_max is not None:
            high = self.y_max
        return low, high

    def render(self):
        w, h = self.width, self.height
        left, top, bottom, right = self.left, self.top, self.bottom, self.right
        plot_w, plot_h = w - left - right, h - top - bottom
        count = max(1, len(self.xlabels) - 1)
        y_low, y_high = self._bounds()

        x_values = [x for serie in self.series for x in serie["xs"]]
        x_low = min(x_values) if x_values else 0.0
        x_high = max(x_values) if x_values else 1.0
        if x_high - x_low < 1e-9:
            x_low, x_high = x_low - 1.0, x_high + 1.0

        def sx(value):
            if self.numeric_x and self.log_x:
                import math
                low, high = math.log10(max(1e-9, x_low)), math.log10(max(1e-9, x_high))
                return left + plot_w * (math.log10(max(1e-9, value)) - low) / (high - low)
            if self.numeric_x:
                return left + plot_w * (value - x_low) / (x_high - x_low)
            return left + (plot_w * value / count if count else plot_w / 2)

        def ty(value):
            import math
            return math.log10(max(1e-9, value)) if self.log_y else value

        y_low, y_high = ty(y_low), ty(y_high)

        def sy(value):
            return top + plot_h * (1 - (ty(value) - y_low) / (y_high - y_low))

        parts = [
            f'<svg viewBox="0 0 {w} {h}" width="100%" role="img" '
            f'aria-label="{esc(self.title)}" xmlns="http://www.w3.org/2000/svg">',
            f'<text x="{w / 2:.0f}" y="20" text-anchor="middle" class="chart-title">'
            f'{esc(self.title)}</text>',
        ]

        if self.log_y:
            tick_labels = [(value, f"{value:g}") for value in log_ticks(10 ** y_low, 10 ** y_high)]
        else:
            tick_labels = [(tick, f"{tick:g}") for tick in nice_ticks(y_low, y_high, 5)]
        for tick, label in tick_labels:
            y = sy(tick)
            parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" '
                         f'class="grid"/>')
            parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" text-anchor="end" '
                         f'class="axis">{esc(label)}</text>')

        if self.numeric_x and self.log_x:
            import math
            for exponent in range(int(math.log10(max(1e-9, x_low))),
                                  int(math.log10(max(1e-9, x_high))) + 1):
                for factor in (1, 2, 5):
                    value = factor * (10 ** exponent)
                    if value < x_low * 0.95 or value > x_high * 1.05:
                        continue
                    x = sx(value)
                    parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" '
                                 f'y2="{top + plot_h}" class="grid-faint"/>')
                    parts.append(f'<text x="{x:.1f}" y="{top + plot_h + 18}" '
                                 f'text-anchor="middle" class="axis">{value:g}</text>')
        elif self.numeric_x:
            for tick in nice_ticks(x_low, x_high, 5):
                x = sx(tick)
                parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top + plot_h}" '
                             f'class="grid-faint"/>')
                parts.append(f'<text x="{x:.1f}" y="{top + plot_h + 18}" text-anchor="middle" '
                             f'class="axis">{tick:g}</text>')
        else:
            for index, label in enumerate(self.xlabels):
                x = sx(index)
                parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top + plot_h}" '
                             f'class="grid-faint"/>')
                parts.append(f'<text x="{x:.1f}" y="{top + plot_h + 18}" text-anchor="middle" '
                             f'class="axis">{esc(label)}</text>')

        parts.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" '
                     f'y2="{top + plot_h}" class="axis-line"/>')
        parts.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" '
                     f'class="axis-line"/>')

        for xs, lows, highs, colour in self.bands:
            upper = " ".join(f"{sx(x if self.numeric_x else i):.1f},{sy(v):.1f}"
                             for i, (x, v) in enumerate(zip(xs, highs)))
            lower = " ".join(f"{sx(x if self.numeric_x else i):.1f},{sy(v):.1f}"
                             for i, (x, v) in
                             reversed(list(enumerate(zip(xs, lows)))))
            parts.append(f'<polygon points="{upper} {lower}" fill="{colour}" opacity="0.10"/>')

        for y, label, colour in self.hlines:
            # the label is in the legend, which is outside the plot, so the line carries none: an
            # annotation inside the plot is one more thing the data has to be read around
            parts.append(f'<line x1="{left}" y1="{sy(y):.1f}" x2="{left + plot_w}" '
                         f'y2="{sy(y):.1f}" stroke="{colour}" stroke-dasharray="6 4" '
                         f'stroke-width="1.5" opacity="0.85"/>')

        for serie in self.series:
            points = " ".join(f"{sx(x if self.numeric_x else i):.1f},{sy(v):.1f}"
                              for i, (x, v) in enumerate(zip(serie["xs"], serie["ys"]))
                              if v is not None)
            parts.append(f'<polyline points="{points}" fill="none" stroke="{serie["colour"]}" '
                         f'stroke-width="2.2"/>')
            if serie["error"]:
                for index, spread in enumerate(serie["error"]):
                    if spread is None:
                        continue
                    low, high = spread
                    x = sx(serie["xs"][index] if self.numeric_x else index)
                    parts.append(f'<line x1="{x:.1f}" y1="{sy(low):.1f}" x2="{x:.1f}" '
                                 f'y2="{sy(high):.1f}" stroke="{serie["colour"]}" '
                                 f'stroke-width="1.5" opacity="0.75"/>')
                    for value in (low, high):
                        parts.append(f'<line x1="{x - 4:.1f}" y1="{sy(value):.1f}" '
                                     f'x2="{x + 4:.1f}" y2="{sy(value):.1f}" '
                                     f'stroke="{serie["colour"]}" stroke-width="1.5" '
                                     f'opacity="0.75"/>')
            if serie["markers"]:
                for index, value in enumerate(serie["ys"]):
                    if value is None:
                        continue
                    x = serie["xs"][index] if self.numeric_x else index
                    parts.append(f'<circle cx="{sx(x):.1f}" cy="{sy(value):.1f}" r="4" '
                                 f'fill="#ffffff" stroke="{serie["colour"]}" stroke-width="2"/>')

        parts.append(f'<text x="{left + plot_w / 2:.0f}" y="{h - 8}" text-anchor="middle" '
                     f'class="axis-label">{esc(self.xlabel)}</text>')
        parts.append(f'<text x="18" y="{top + plot_h / 2:.0f}" text-anchor="middle" '
                     f'transform="rotate(-90 18 {top + plot_h / 2:.0f})" '
                     f'class="axis-label">{esc(self.ylabel)}</text>')

        legend_x = w - 12
        legend_y = top + 12
        for index, serie in enumerate(self.series):
            y = legend_y + index * 18
            parts.append(f'<line x1="{legend_x - 26}" y1="{y}" x2="{legend_x}" y2="{y}" '
                         f'stroke="{serie["colour"]}" stroke-width="2.4"/>')
            parts.append(f'<text x="{legend_x - 32}" y="{y + 4}" text-anchor="end" '
                         f'class="legend">{esc(serie["label"])}</text>')
        for index, (_, label, colour) in enumerate(self.hlines):
            y = legend_y + (len(self.series) + index) * 18
            parts.append(f'<line x1="{legend_x - 26}" y1="{y}" x2="{legend_x}" y2="{y}" '
                         f'stroke="{colour}" stroke-width="1.6" stroke-dasharray="5 3"/>')
            parts.append(f'<text x="{legend_x - 32}" y="{y + 4}" text-anchor="end" '
                         f'class="legend">{esc(label)}</text>')

        parts.append("</svg>")
        return "\n".join(parts)


# ---------------------------------------------------------------- report


def build(runs, title):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    parts = [f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<style>
  :root {{ --fg:#1c2430; --muted:#5b6675; --line:#dfe4ea; --card:#ffffff; --bg:#f6f8fa; }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--fg);
         font:15px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC",
              "Hiragino Sans GB","Microsoft YaHei",sans-serif; }}
  .wrap {{ max-width:1060px; margin:0 auto; padding:36px 22px 72px; }}
  h1 {{ font-size:27px; margin:0 0 6px; letter-spacing:-0.2px; }}
  h2 {{ font-size:19px; margin:38px 0 12px; padding-bottom:7px; border-bottom:1px solid var(--line); }}
  h3 {{ font-size:15px; margin:22px 0 8px; color:var(--muted); font-weight:600; }}
  .sub {{ color:var(--muted); margin:0 0 22px; }}
  .card {{ background:var(--card); border:1px solid var(--line); border-radius:10px;
           padding:18px 20px; margin:14px 0; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:12px; }}
  .kpi {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }}
  .kpi .label {{ color:var(--muted); font-size:12.5px; }}
  .kpi .value {{ font-size:23px; font-weight:650; margin-top:3px; letter-spacing:-0.3px; }}
  .kpi .note {{ color:var(--muted); font-size:12px; }}
  table {{ border-collapse:collapse; width:100%; font-size:13.5px; }}
  th,td {{ border-bottom:1px solid var(--line); padding:7px 9px; text-align:right;
          word-break:break-word; }}
  td.definition {{ max-width:460px; word-break:break-word; }}
  th:first-child, td:first-child {{ text-align:left; }}
  thead th {{ color:var(--muted); font-weight:600; border-bottom:1.5px solid var(--line);
              white-space:nowrap; }}
  tbody tr:hover {{ background:#f9fbfd; }}
  code {{ background:#eef1f4; padding:1px 5px; border-radius:4px; font-size:12.5px; }}
  .chart-title {{ font-size:14px; font-weight:600; fill:var(--fg); }}
  .axis {{ font-size:11px; fill:var(--muted); }}
  .axis-label {{ font-size:12px; fill:var(--muted); }}
  .legend {{ font-size:11.5px; fill:var(--muted); }}
  .annotation {{ font-size:11px; fill:var(--muted); }}
  .grid {{ stroke:#e8ecf1; stroke-width:1; }}
  .grid-faint {{ stroke:#f2f5f8; stroke-width:1; }}
  .axis-line {{ stroke:#c3ccd6; stroke-width:1.2; }}
  .note {{ color:var(--muted); font-size:13px; }}
  ul {{ margin:8px 0 8px 18px; padding:0; }}
  li {{ margin:4px 0; }}
  details {{ margin:10px 0; }}
  summary {{ cursor:pointer; color:#2f6fd0; }}
  .warn {{ border-left:3px solid #e08b1b; padding-left:12px; color:var(--muted); }}
</style></head><body><div class="wrap">
<h1>{esc(title)}</h1>
<p class="sub">生成于 {stamp} · 数据来自 benchmark 日志，未经二次加工 ·
{esc(", ".join(run["name"] for run in runs))}</p>
"""]

    # ---- what was measured
    parts.append("<h2>被测对象</h2><div class=\"card\"><table><thead><tr>"
                 "<th>run</th><th>表</th><th>索引定义</th><th>topK</th>"
                 "<th>查询数</th><th>重复</th><th>over-sample 取值</th>"
                 "</tr></thead><tbody>")
    for run in runs:
        meta = run["meta"]
        index = " ".join(run["index"]) if run["index"] else "—"
        parts.append(
            "<tr>"
            f"<td>{esc(run['name'])}</td>"
            f"<td><code>{esc(meta.get('base', '?'))}</code></td>"
            f"<td class=\"definition\" style=\"text-align:left;font-size:12.5px\">"
            f"{esc(index)}</td>"
            f"<td>{meta.get('topK', '?')}</td>"
            f"<td>{meta.get('measuredQueries', '?')}</td>"
            f"<td>{meta.get('repeats', '?')}</td>"
            f"<td>{esc(meta.get('overSamples', ''))}</td>"
            "</tr>")
    parts.append("</tbody></table>")

    incomplete = [run["name"] for run in runs if not run["done"]]
    if incomplete:
        parts.append(f'<p class="warn">这些 run 没有跑到结束标记，数字可能不完整：'
                     f'{esc(", ".join(incomplete))}</p>')
    parts.append("</div>")

    # ---- KPI cards: the extremes of each run
    parts.append('<h2>结论速览</h2><div class="cards">')
    for run in runs:
        per_over = by_over(run)
        if not per_over:
            continue
        baseline = scan_latencies(run)
        scan_median = statistics.median(baseline) if baseline else 0
        best = max(per_over.items(), key=lambda item: statistics.mean(item[1]["recall"]))
        fastest = min(per_over.items(), key=lambda item: statistics.median(item[1]["ms"]))
        parts.append(
            f'<div class="kpi"><div class="label">{esc(run["name"])} · 最高 recall</div>'
            f'<div class="value">{statistics.mean(best[1]["recall"]):.4f}</div>'
            f'<div class="note">over-sample {best[0]} · 中位延迟 '
            f'{statistics.median(best[1]["ms"]):.0f} ms</div></div>')
        parts.append(
            f'<div class="kpi"><div class="label">{esc(run["name"])} · 最低中位延迟</div>'
            f'<div class="value">{statistics.median(fastest[1]["ms"]):.0f} ms</div>'
            f'<div class="note">over-sample {fastest[0]} · recall '
            f'{statistics.mean(fastest[1]["recall"]):.4f}</div></div>')
        parts.append(
            f'<div class="kpi"><div class="label">{esc(run["name"])} · 全表扫描</div>'
            f'<div class="value">{scan_median:.0f} ms</div>'
            f'<div class="note">中位数，{len(baseline)} 次</div></div>')
    parts.append("</div>")

    # ---- chart 1: recall against the over-sample
    chart = Chart("recall@10 随 over-sample 的变化", "over-sample（1 = 关闭）", "recall@10",
                  pad=(70, 24, 48, 130))
    every = sorted({over for run in runs for over in by_over(run)})
    xlabels = [str(over) for over in every]
    for index, run in enumerate(runs):
        per_over = by_over(run)
        if not per_over:
            continue
        colour = PALETTE[index % len(PALETTE)]
        means, spread = [], []
        for over in every:
            if over not in per_over:
                means.append(None)
                spread.append(None)
                continue
            values = per_over[over]["recall"]
            means.append(statistics.mean(values))
            spread.append((min(values), max(values)))
        chart.add_series(run["name"], every, means, colour, error=spread)
    chart.xlabels = xlabels or ["1"]
    chart.y_min, chart.y_max = 0.0, 1.02
    parts.append(f'<h2>曲线</h2><div class="card">{chart.render()}'
                 f'<p class="note">折线是每个取值下所有（查询 × 轮次）的平均 recall，'
                 f'竖线是其中的最小值与最大值：查询之间的差异比轮次之间的差异大得多。</p></div>')

    # ---- chart 2: latency against the over-sample, with the scan as a reference line
    chart = Chart("查询延迟随 over-sample 的变化", "over-sample（1 = 关闭）",
                  "每次查询耗时 (ms，对数刻度)", pad=(84, 24, 48, 150), log_y=True)
    for index, run in enumerate(runs):
        per_over = by_over(run)
        if not per_over:
            continue
        colour = PALETTE[index % len(PALETTE)]
        medians, lows, highs = [], [], []
        for over in every:
            if over not in per_over:
                medians.append(None)
                lows.append(0)
                highs.append(0)
                continue
            entry = per_over[over]
            medians.append(statistics.median(entry["ms"]))
            lows.append(percentile(sorted(entry["ms"]), 0.25))
            highs.append(percentile(sorted(entry["ms"]), 0.75))
        band = [i for i, value in enumerate(medians) if value is not None]
        chart.add_band([every[i] for i in band], [lows[i] for i in band],
                       [highs[i] for i in band], colour)
        chart.add_series(f"{run['name']} 中位数", every, medians, colour)
    chart.xlabels = xlabels or ["1"]
    for index, run in enumerate(runs):
        baseline = scan_latencies(run)
        if baseline:
            chart.add_hline(statistics.median(baseline),
                            f"{run['name']} 全表扫描 {statistics.median(baseline):.0f} ms",
                            PALETTE[index % len(PALETTE)])
    parts.append(f'<div class="card">{chart.render()}'
                 f'<p class="note">实线是每个取值的中位延迟，浅色带是 p25–p75，虚线是该表全表扫描的中位数。</p>'
                 f'</div>')

    # ---- chart 3: the trade-off
    chart = Chart("recall 与延迟的取舍", "每次查询耗时（中位数, ms）", "recall@10",
                  pad=(70, 24, 48, 130), numeric_x=True, log_x=True)
    for index, run in enumerate(runs):
        per_over = by_over(run)
        if not per_over:
            continue
        colour = PALETTE[index % len(PALETTE)]
        overs = sorted(per_over)
        xs = [statistics.median(per_over[over]["ms"]) for over in overs]
        ys = [statistics.mean(per_over[over]["recall"]) for over in overs]
        chart.add_series(run["name"], xs, ys, colour)
    chart.y_min, chart.y_max = 0.0, 1.02
    parts.append(f'<div class="card">{chart.render()}'
                 f'<p class="note">横轴是每个 over-sample 取值的中位延迟（对数刻度），'
                 f'纵轴是它的平均 recall；越靠左上越好，同一个 run 的点按 over-sample 从小到大连线。</p></div>')

    # ---- chart 4: per-query spread, so the noise is visible
    chart = Chart("每次查询的耗时分布（点 = 一次查询）", "over-sample（1 = 关闭）",
                  "每次查询耗时 (ms)", pad=(70, 24, 48, 130))
    for index, run in enumerate(runs):
        per_over = by_over(run)
        if not per_over:
            continue
        colour = PALETTE[index % len(PALETTE)]
        for position, over in enumerate(every):
            if over not in per_over:
                continue
            for value in per_over[over]["ms"]:
                # a strip plot: every measurement keeps its own position, so a long tail shows
                jitter = (hash((run["name"], over, value, position)) % 100) / 100.0 - 0.5
                chart.series.append({"label": "", "xs": [], "ys": [], "colour": colour,
                                     "markers": False, "error": None,
                                     "strip": (position + jitter * 0.6, value)})
    chart.xlabels = xlabels or ["1"]
    chart.strip_legend = [(run["name"], PALETTE[index % len(PALETTE)])
                          for index, run in enumerate(runs) if run["results"]]
    parts.append(f'<div class="card">{render_strip(chart)}</div>')

    # ---- summary table
    parts.append("<h2>每个取值的汇总</h2><div class=\"card\"><table><thead><tr>"
                 "<th>run</th><th>over-sample</th><th>recall 均值</th><th>recall 最差</th>"
                 "<th>延迟 min</th><th>p25</th><th>median</th><th>p75</th><th>max</th>"
                 "<th>对扫描加速</th><th>测量数</th></tr></thead><tbody>")
    for run in runs:
        per_over = by_over(run)
        baseline = scan_latencies(run)
        scan_median = statistics.median(baseline) if baseline else None
        for over in sorted(per_over):
            entry = per_over[over]
            low, p25, median, p75, high = quartiles(entry["ms"])
            speedup = f"{scan_median / median:.2f}×" if scan_median and median else "—"
            parts.append(
                "<tr>"
                f"<td>{esc(run['name'])}</td>"
                f"<td>{over}</td>"
                f"<td>{statistics.mean(entry['recall']):.4f}</td>"
                f"<td>{min(entry['recall']):.4f}</td>"
                f"<td>{low:.0f}</td><td>{p25:.0f}</td><td><b>{median:.0f}</b></td>"
                f"<td>{p75:.0f}</td><td>{high:.0f}</td>"
                f"<td>{speedup}</td>"
                f"<td>{len(entry['ms'])}</td>"
                "</tr>")
    parts.append("</tbody></table></div>")

    # ---- method and caveats
    parts.append("""<h2>方法与口径</h2><div class="card">
<ul>
  <li><b>精确答案</b>：<code>ORDER BY vec_distance(col, q) + 0.0d LIMIT k</code>——加一个
      <code>+ 0.0d</code> 让改写规则认不出来，于是走全表扫描，同时充当 recall 的 ground truth。
      每个查询只算一次，所有设置共用。</li>
  <li><b>索引答案</b>：<code>ORDER BY vec_distance(col, q) LIMIT k</code>，即用户写的那条 SQL，经
      改写规则走索引。</li>
  <li><b>over-sample</b>：<code>spark.iceberg.index.vector.over-sample</code>，1 表示关闭（只取
      k 个候选）；取值越大，交给精确重排的候选越多。</li>
  <li><b>去偏</b>：设置按查询交错测量（同一个查询把所有取值测完再换下一个），避免时间/缓存漂移
      系统性地偏袒某一个取值；每个取值重复多轮，图上的竖线是轮次与查询之间 min–max。</li>
  <li><b>预热</b>：前若干个查询同时走扫描与索引但不计入统计；第一次索引查询会读文件、建缓存。</li>
  <li><b>注意</b>：集群是共享的，同一取值的延迟仍有秒级抖动；横向比较看同一轮里取值的相对关系，
      不要拿不同时间跑的数字绝对对比。</li>
</ul></div>""")

    # ---- raw data
    parts.append("<h2>原始测量</h2>")
    for run in runs:
        rows = run["results"]
        parts.append(f"<details><summary>{esc(run['name'])} · {len(rows)} 条索引测量 · "
                     f"{len(run['scans'])} 条扫描测量</summary><div class=\"card\">"
                     "<table><thead><tr><th>over</th><th>round</th><th>query</th>"
                     "<th>ms</th><th>hits</th><th>found</th><th>recall</th></tr></thead><tbody>")
        for row in rows:
            parts.append(
                "<tr>"
                f"<td>{row['over']}</td><td>{row['repeat']}</td><td>{row['query']}</td>"
                f"<td>{row['ms']}</td><td>{row['hits']}</td><td>{row['found']}</td>"
                f"<td>{row['recall']:.3f}</td>"
                "</tr>")
        parts.append("</tbody></table></div></details>")

    parts.append("</div></body></html>")
    return "\n".join(parts)


def render_strip(chart):
    """A strip plot: one dot per measurement, so a long tail is visible rather than averaged away."""
    w, h = 780, 360
    left, top, bottom, right = 70, 24, 48, 150
    plot_w, plot_h = w - left - right, h - top - bottom
    values = [entry["strip"][1] for entry in chart.series if "strip" in entry]
    if not values:
        return "<p class='note'>没有数据。</p>"
    # a log axis: the tail of a shared cluster reaches a hundred times the median, and on a linear
    # axis that tail would be the only thing visible
    import math
    low = math.log10(max(1.0, min(values))) - 0.05
    high = math.log10(max(values)) + 0.05
    count = max(1, len(chart.xlabels) - 1)

    def sx(index):
        return left + plot_w * index / count if count else left + plot_w / 2

    def sy(value):
        return top + plot_h * (1 - (math.log10(max(1.0, value)) - low) / (high - low))

    parts = [f'<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="每次查询的耗时分布" '
             f'xmlns="http://www.w3.org/2000/svg">',
             f'<text x="{w / 2:.0f}" y="20" text-anchor="middle" class="chart-title">'
             f'{"每次查询的耗时分布（点 = 一次查询）"}</text>']
    for value in log_ticks(10 ** low, 10 ** high):
        y = sy(value)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" '
                     f'class="grid"/>')
        parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" text-anchor="end" '
                     f'class="axis">{value:g}</text>')
    for index, label in enumerate(chart.xlabels):
        x = sx(index)
        parts.append(f'<text x="{x:.1f}" y="{top + plot_h + 18}" text-anchor="middle" '
                     f'class="axis">{esc(label)}</text>')
    parts.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" '
                 f'y2="{top + plot_h}" class="axis-line"/>')
    parts.append(f'<text x="{left + plot_w / 2:.0f}" y="{h - 8}" text-anchor="middle" '
                 f'class="axis-label">over-sample（1 = 关闭）</text>')
    parts.append(f'<text x="18" y="{top + plot_h / 2:.0f}" text-anchor="middle" '
                 f'transform="rotate(-90 18 {top + plot_h / 2:.0f})" '
                 f'class="axis-label">每次查询耗时 (ms)</text>')
    for entry in chart.series:
        if "strip" not in entry:
            continue
        position, value = entry["strip"]
        parts.append(f'<circle cx="{sx(position):.1f}" cy="{sy(value):.1f}" r="2.8" '
                     f'fill="{entry["colour"]}" opacity="0.6"/>')
    for index, label in enumerate(chart.strip_legend):
        y = 44 + index * 16
        parts.append(f'<circle cx="{w - 108}" cy="{y}" r="3.2" fill="{label[1]}" opacity="0.8"/>')
        parts.append(f'<text x="{w - 100}" y="{y + 4}" class="legend">{esc(label[0])}</text>')
    parts.append(f'<text x="{left + plot_w / 2:.0f}" y="{h - 24}" text-anchor="middle" '
                 f'class="annotation">纵轴为对数刻度</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Build an HTML report from benchmark logs")
    parser.add_argument("logs", nargs="+", help="result logs, one per run")
    parser.add_argument("-o", "--out", default="vector-benchmark-report.html")
    parser.add_argument("--title", default="向量索引 benchmark 报告")
    options = parser.parse_args()

    paths = []
    for pattern in options.logs:
        matched = glob.glob(pattern)
        paths += matched if matched else [pattern]
    runs = [parse(path) for path in paths]
    if not any(run["results"] for run in runs):
        sys.exit("no BENCHRESULT lines in the given logs")

    with open(options.out, "w", encoding="utf-8") as handle:
        handle.write(build(runs, options.title))
    print(f"wrote {options.out}")


if __name__ == "__main__":
    main()
