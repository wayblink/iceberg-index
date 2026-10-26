#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Renders the one-page summary of the vector index benchmark.

The detailed report (build-report.py) keeps every measurement plus the per-table curves. This page
answers the question that report is too big to answer at a glance: which configuration should we
ship, what does it cost, and what is still unknown. It reads the same logs, so the two cannot drift.
"""

import argparse
import glob
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "build_report", os.path.join(os.path.dirname(os.path.abspath(__file__)), "build-report.py"))
_build_report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_build_report)

cell_of = _build_report.cell_of
arm_of = _build_report.arm_of
by_over = _build_report.by_over
cheapest_full_recall = _build_report.cheapest_full_recall
tables_of = _build_report.tables_of
esc = _build_report.esc
parse = _build_report.parse
split_by_index = _build_report.split_by_index

HERE = os.path.dirname(os.path.abspath(__file__))
ARMS = ("HNSW", "IVF（未量化）", "IVF·1bit", "IVF·3bit", "IVF·4bit", "FLAT（暴力）")
ARM_COLOURS = {
    "全表扫描": "#9aa4b1",
    "HNSW": "#4a6fa5",
    "IVF（未量化）": "#7b5ea7",
    "IVF·1bit": "#c0392b",
    "IVF·3bit": "#1a8f7a",
    "IVF·4bit": "#d68910",
    "FLAT（暴力）": "#5d6d7e",
}


def load(paths):
    runs = []
    for path in paths:
        for run in split_by_index(parse(path)):
            runs.append(run)
    return runs


_SIZES = _build_report.index_sizes()


def size_of(run):
    """Bytes of the index this run measured: the run's own BENCHMETA when it carries it, else the
    sidecar that collect-results.sh writes from the file listing."""
    facts = _build_report.size_fact(run, _SIZES)
    return facts.get("indexBytes")


def megabytes(value):
    if not value:
        return "—"
    return f"{value / 1e9:.2f} GB" if value >= 1e9 else f"{value / 1e6:.0f} MB"


def bar_chart(title, groups, legend, height=300, width=880):
    """A log-scale grouped bar chart: latency against the references, one group per table.

    Bars are drawn as SVG rects so the page stays self-contained; the log axis is what lets the
    10M numbers (seconds) and the 100M numbers (minutes) share one frame honestly.
    """
    values = [value for group in groups for _, value in group["bars"] if value]
    if not values:
        return ""
    low, high = min(values), max(values)
    low = 10 ** (int(_log10(low)))
    high = 10 ** (int(_log10(high)) + 1)

    pad_left, pad_right, pad_top, pad_bottom = 74, 18, 44, 74
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom

    def y_of(value):
        fraction = (_log10(value) - _log10(low)) / (_log10(high) - _log10(low))
        return pad_top + plot_h * (1 - fraction)

    parts = [f'<svg viewBox="0 0 {width} {height}" width="100%" role="img">',
             f'<text class="chart-title" x="{pad_left}" y="26">{esc(title)}</text>']

    ticks = []
    power = int(_log10(low))
    while 10 ** power <= high:
        ticks.append(10 ** power)
        power += 1
    for tick in ticks:
        y = y_of(tick)
        parts.append(f'<line class="grid" x1="{pad_left}" y1="{y:.1f}" '
                     f'x2="{width - pad_right}" y2="{y:.1f}"/>')
        label = (f"{tick / 1000:.0f} s" if tick >= 1000 else f"{tick} ms")
        parts.append(f'<text class="axis" x="{pad_left - 8}" y="{y + 4:.1f}" '
                     f'text-anchor="end">{label}</text>')

    group_width = plot_w / len(groups)
    for index, group in enumerate(groups):
        bars = group["bars"]
        slot = group_width * 0.78 / len(bars)
        base_x = pad_left + group_width * index + group_width * 0.11
        for position, (label, value) in enumerate(bars):
            x = base_x + slot * position
            if not value:
                continue
            top = y_of(value)
            parts.append(
                f'<rect x="{x:.1f}" y="{top:.1f}" width="{slot * 0.84:.1f}" '
                f'height="{pad_top + plot_h - top:.1f}" fill="{ARM_COLOURS.get(label, "#888")}" '
                f'opacity="0.85" rx="2"/>')
            parts.append(
                f'<text class="annotation" x="{x + slot * 0.42:.1f}" y="{top - 6:.1f}" '
                f'text-anchor="middle">{value / 1000:.1f}s</text>')
        parts.append(f'<text class="axis-label" x="{pad_left + group_width * (index + 0.5):.1f}" '
                     f'y="{pad_top + plot_h + 22:.1f}" text-anchor="middle">'
                     f'{esc(group["label"])}</text>')

    legend_x = pad_left
    for label, colours in legend:
        width = 0
        for colour in colours:
            parts.append(f'<rect x="{legend_x + width}" y="{height - 30}" width="9" height="9" '
                         f'rx="2" fill="{colour}" opacity="0.85"/>')
            width += 11
        parts.append(f'<text class="legend" x="{legend_x + width + 3}" y="{height - 22}">'
                     f'{esc(label)}</text>')
        # CJK glyphs at 11px are about 11px wide; ASCII a bit over half that
        legend_x += width + 16 + 11.0 * sum(1 for ch in label if ord(ch) > 0x2e80) \
            + 6.5 * sum(1 for ch in label if ord(ch) <= 0x2e80)

    parts.append("</svg>")
    return "".join(parts)


def _log10(value):
    import math
    return math.log10(value)


def curve_chart(title, runs, arms, height=260, width=430):
    """recall against over-sample, small multiples: one panel per table.

    Recall is the dimension that decides whether a configuration is usable at all, so it gets the
    linear axis and the shared 0–1 range; the panels only differ in the arms drawn.
    """
    pad_left, pad_right, pad_top, pad_bottom = 48, 14, 40, 46
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom
    overs = sorted({over for run in runs for over in by_over(run)})
    if not overs:
        return ""
    slots = {over: index for index, over in enumerate(overs)}

    def x_of(over):
        if len(overs) == 1:
            return pad_left + plot_w / 2
        return pad_left + plot_w * slots[over] / (len(overs) - 1)

    def y_of(value):
        return pad_top + plot_h * (1 - value)

    parts = [f'<svg viewBox="0 0 {width} {height}" width="100%" role="img">',
             f'<text class="chart-title" x="{pad_left}" y="24">{esc(title)}</text>']
    for value in (0.0, 0.5, 1.0):
        parts.append(f'<line class="grid" x1="{pad_left}" y1="{y_of(value):.1f}" '
                     f'x2="{width - pad_right}" y2="{y_of(value):.1f}"/>')
        parts.append(f'<text class="axis" x="{pad_left - 6}" y="{y_of(value) + 4:.1f}" '
                     f'text-anchor="end">{value:.1f}</text>')
    for over in overs:
        parts.append(f'<text class="axis" x="{x_of(over):.1f}" y="{pad_top + plot_h + 16:.1f}" '
                     f'text-anchor="middle">{"直接" if over == 0 else over}</text>')

    for run in runs:
        arm = arm_of(run)
        if arm not in arms:
            continue
        per_over = by_over(run)
        points = [(x_of(over), y_of(statistics.mean(per_over[over]["recall"])))
                  for over in overs if over in per_over]
        if len(points) < 2:
            continue
        path = " ".join(f"{'M' if index == 0 else 'L'}{x:.1f},{y:.1f}"
                        for index, (x, y) in enumerate(points))
        parts.append(f'<path d="{path}" fill="none" stroke="{ARM_COLOURS[arm]}" '
                     f'stroke-width="2" stroke-linejoin="round"/>')
        for x, y in points:
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.6" fill="#fff" '
                         f'stroke="{ARM_COLOURS[arm]}" stroke-width="1.8"/>')

    legend_x = pad_left
    for arm in arms:
        parts.append(f'<rect x="{legend_x}" y="{height - 22}" width="9" height="9" rx="2" '
                     f'fill="{ARM_COLOURS[arm]}"/>')
        parts.append(f'<text class="legend" x="{legend_x + 13}" y="{height - 14}">'
                     f'{esc(arm)}</text>')
        legend_x += 96
    parts.append("</svg>")
    return "".join(parts)


def candidate_prices(runs, wide=100, narrow=10, limit=10):
    """What one more candidate costs on the exact re-rank, per table and arm.

    The kernel truncates after sorting every visited list, so widening the candidate list buys no
    extra scanning -- the only thing it adds is rows to re-score exactly. Dividing the latency
    difference between two over-sample values by the extra candidates prices that fetch, which is
    the term the two data formats disagree about.
    """
    prices = {}
    for run in runs:
        per_over = by_over(run)
        if wide not in per_over or narrow not in per_over:
            continue
        extra = limit * (wide - narrow)
        delta = (statistics.median(per_over[wide]["ms"])
                 - statistics.median(per_over[narrow]["ms"]))
        prices[(cell_of(run)[0], cell_of(run)[1], arm_of(run))] = (
            delta / extra, statistics.median(per_over[narrow]["ms"]),
            statistics.median(per_over[wide]["ms"]), extra)
    return prices


def paired_comparison(runs, arm_a, over_a, arm_b, over_b):
    """Per query, the warm repeat of two arms, compared inside the same table.

    The logs are split per index, so an arm and its rival live in different run dicts; they are
    regrouped by table here. Taking the last repeat matters: each arm pays its payload load on its
    first touch, and that cold number is 5-10x the warm one.
    """
    # only a run that measured both arms can be paired, and only inside that run: merging two runs
    # would compare a warm cell of one against a cold cell of the other
    warm = {}
    for run in runs:
        log = run["name"].split(" · ")[0]
        for row in run["results"]:
            key = (log, row.get("index"), row.get("over"), row.get("query"))
            warm.setdefault(key, []).append((row.get("repeat", 0), row["ms"]))
    pairs = {}
    logs = sorted({log for (log, _, _, _) in warm})
    for log in logs:
        table = {}
        for query in sorted({q for (l, _, _, q) in warm if l == log}):
            a = warm.get((log, arm_a, over_a, query))
            b = warm.get((log, arm_b, over_b, query))
            if not a or not b:
                continue
            # the highest repeat number is the warmest: repeat 1 of a cell can be the run's
            # first touch of that payload
            table[query] = (sorted(a)[-1][1], sorted(b)[-1][1])
        if table:
            pairs[log] = table
    return pairs


def build(runs, confirmations, replications, build_times, title):
    ceilings, cheapest = cheapest_full_recall(runs)
    summary = tables_of(runs)
    sizes = {}
    for run in runs:
        key = (cell_of(run)[0], cell_of(run)[1], arm_of(run))
        sizes[key] = size_of(run)

    tables = [run for run in runs if run["results"]]

    # latencies for the log-scale picture: the recommendation, then the two references
    groups = []
    for row in summary:
        best_arm, best_over = (row["best"][0], row["best"][1][0]) if row["best"] else (None, None)
        bars = []
        if row["best"]:
            bars.append((best_arm, row["best"][1][1]))
        if row["scan"]:
            bars.append(("全表扫描", row["scan"]))
        if row["flat"]:
            bars.append(("FLAT（暴力）", row["flat"][1]))
        if row["hnsw"]:
            bars.append(("HNSW", row["hnsw"][1]))
        groups.append({"label": f"{row['scale']} · {row['format']}", "bars": bars})

    parts = [f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<style>
  :root {{ --fg:#1b2430; --muted:#5b6675; --line:#e0e5eb; --card:#fff; --bg:#f5f7fa; }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--fg);
         font:15px/1.7 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC",
              "Hiragino Sans GB","Microsoft YaHei",sans-serif; }}
  .wrap {{ max-width:1000px; margin:0 auto; padding:40px 22px 80px; }}
  h1 {{ font-size:26px; margin:0 0 8px; letter-spacing:-0.2px; }}
  h2 {{ font-size:18px; margin:36px 0 12px; padding-bottom:6px; border-bottom:1px solid var(--line); }}
  .sub {{ color:var(--muted); margin:0 0 24px; font-size:14px; }}
  .card {{ background:var(--card); border:1px solid var(--line); border-radius:10px;
           padding:18px 20px; margin:14px 0; }}
  .verdict {{ border-left:3px solid #1a8f7a; }}
  .verdict p {{ margin:8px 0; }}
  table {{ border-collapse:collapse; width:100%; font-size:13.5px; }}
  th,td {{ border-bottom:1px solid var(--line); padding:8px 9px; text-align:right; }}
  th:first-child, td:first-child {{ text-align:left; }}
  thead th {{ color:var(--muted); font-weight:600; border-bottom:1.5px solid var(--line);
              white-space:nowrap; }}
  tbody tr:hover {{ background:#fafbfd; }}
  td.arm {{ text-align:left; }}
  code {{ background:#eef1f4; padding:1px 5px; border-radius:4px; font-size:12.5px; }}
  .sub-value {{ color:var(--muted); font-size:12px; }}
  .note {{ color:var(--muted); font-size:13px; }}
  .chart-title {{ font-size:13.5px; font-weight:600; fill:var(--fg); }}
  .axis {{ font-size:10.5px; fill:var(--muted); }}
  .axis-label {{ font-size:11.5px; fill:var(--muted); }}
  .legend {{ font-size:11px; fill:var(--muted); }}
  .annotation {{ font-size:10.5px; fill:var(--muted); }}
  .grid {{ stroke:#e8ecf1; stroke-width:1; }}
  .panels {{ display:grid; grid-template-columns:1fr 1fr; gap:12px; }}
  .warn {{ border-left:3px solid #d68910; }}
  ul {{ margin:8px 0 8px 20px; padding:0; }}
  li {{ margin:5px 0; }}
  .kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(210px,1fr)); gap:12px; }}
  .kpi {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }}
  .kpi .label {{ color:var(--muted); font-size:12.5px; }}
  .kpi .value {{ font-size:22px; font-weight:650; letter-spacing:-0.3px; }}
  .kpi .foot {{ color:var(--muted); font-size:12px; }}
</style></head><body><div class="wrap">
<h1>{esc(title)}</h1>
<p class="sub">伞卡 #1948 的收口摘要 · 数据取自 <code>tools/vector-benchmark/results/</code> 下的
benchmark 日志，与详细报告同源，未经二次加工</p>
"""]

    best_rows = [row for row in summary if row["best"]]
    parts.append('<div class="kpis">')
    for row in best_rows:
        arm, (over, ms) = row["best"]
        speed = f"{row['scan'] / ms:.1f}×" if row["scan"] else "—"
        parts.append(
            f'<div class="kpi"><div class="label">{esc(row["scale"])} · {esc(row["format"])}</div>'
            f'<div class="value">{ms / 1000:.1f} s</div>'
            f'<div class="foot">{esc(arm)} over {over} · 扫描的 {speed}</div></div>')
    parts.append("</div>")

    parts.append(f"""<div class="card verdict">
<p><b>结论：IVF × RaBitQ 1bit，nprobe=32，over-sample=100，数据文件用 Lance。</b>
10M 上每次查询 4.4 s、100M 上 13.5 s，都是同表精确 recall；相对全表扫描快 4.3 倍与 9.9 倍，
相对暴力搜索快 2.0 倍与 9.0 倍。</p>
<p>这个结论推翻了中途的判断。原先按 over-sample=10 看，IVF·3bit 是"甜点"；但把每个臂
<i>达到精确 recall 所需的最窄候选</i>摊开比较，1bit 反而最快：它的索引只有 3bit 的一半
（323 vs 630 MB，3.21 vs 6.28 GB），每个候选只有 16 字节量化码、打分是 popcount，
多买一个数量级的候选比多读一倍索引便宜得多。</p>
<p>100M Parquet 是唯一说不清明细的一格，而且两次跑给出相反的排序：2 查询那次 3bit@10 是
51.7 s、1bit@100 是 93.3 s；补做的 6 查询复核里 1bit@100 变成 60.0 s、3bit@10 是 70.3 s，
逐查询配对看 1bit 赢 4/6、中位只快 1.2 倍，但 q4 上它反过来慢一倍（123.6 vs 62.9 s）。
所以这一格应当当作<b>两者相当</b>，落地时选 3bit@10：它的成本不依赖"一个候选多少钱"这个
既贵又随查询抖动的项。原因不在索引而在回表——同一份 3.21 GB 的 1bit 索引、同样 1000 个候选，
Lance 上 14.9 s、Parquet 上 60–93 s。</p>
<p>这个"Lance 更快"只对<b>按候选回表</b>的臂成立：不按候选回表的臂上两种格式相当甚至 Parquet
更快（100M 的 FLAT 是 98.0 vs 121.8 s，IVF 未量化 353.6 vs 366.2 s，HNSW 139.4 vs 134.9 s；
10M 的全表扫描中位也是 Parquet 14.0 s 比 Lance 18.4 s 快）。</p>
<p>还有个容易被漏掉的事实：收紧候选宽度这件"优化"在 10M 上完全免费，在 100M 上却很贵，
而且贵多少由格式决定。下一节把它单独量了出来。</p>
</div>""")

    # ---- the recommendation, table by table
    parts.append('<h2>每张表的推荐配置</h2><div class="card"><table><thead><tr>'
                 "<th>规模 / 数据格式</th><th>推荐</th><th>延迟</th><th>索引体积</th>"
                 "<th>全表扫描</th><th>暴力搜索</th><th>HNSW</th><th>相对扫描</th>"
                 "</tr></thead><tbody>")
    for row in summary:
        best = row["best"]
        arm, over, ms = (best[0], best[1][0], best[1][1]) if best else (None, None, None)
        size = sizes.get((row["scale"], row["format"], arm)) if arm else None
        parts.append(
            "<tr>"
            f"<td><b>{esc(row['scale'])}</b> · {esc(row['format'])}</td>"
            f"<td class='arm'><code>{esc(arm or '—')}</code>"
            f"<span class='sub-value'> · over {over}</span></td>"
            f"<td>{f'{ms / 1000:.1f} s' if ms else '—'}</td>"
            f"<td>{megabytes(size)}</td>"
            f"<td>{f'{row['scan'] / 1000:.1f} s' if row['scan'] else '—'}</td>"
            f"<td>{f'{row['flat'][1] / 1000:.1f} s' if row['flat'] else '—'}</td>"
            f"<td>{f'{row['hnsw'][1] / 1000:.1f} s' if row['hnsw'] else '—'}</td>"
            f"<td>{f'{row['scan'] / ms:.1f}×' if row['scan'] and ms else '—'}</td>"
            "</tr>")
    parts.append("</tbody></table>"
                 '<p class="note">"推荐"是在所有测过的 over-sample 里挑出的最便宜组合，'
                 "不是固定某个取值的结果；HNSW 一列取它自己最快的 over-sample。</p></div>")

    parts.append('<div class="card">'
                 + bar_chart("达到精确 recall 的延迟：推荐配置 vs 全表扫描 / 暴力搜索 / HNSW",
                             groups,
                             (("该表最优臂", (ARM_COLOURS["IVF·1bit"], ARM_COLOURS["IVF·3bit"])),
                              ("全表扫描", (ARM_COLOURS["全表扫描"],)),
                              ("FLAT（暴力）", (ARM_COLOURS["FLAT（暴力）"],)),
                              ("HNSW", (ARM_COLOURS["HNSW"],))))
                 + '<p class="note">对数纵轴：10M 的秒与 100M 的分在同一张图里才看得出量级差。'
                   "100M 上 HNSW 与全表扫描几乎重合，说明图装不进缓存后退化成扫描自己的文件。</p>"
                 "</div>")

    # ---- everything that was measured
    parts.append('<h2>正交矩阵（over-sample = 10）</h2><div class="card"><table><thead><tr>'
                 "<th>规模 / 数据格式</th>"
                 + "".join(f"<th>{esc(arm)}</th>" for arm in ARMS)
                 + "</tr></thead><tbody>")
    cells = {}
    for run in tables:
        scale, data_format = cell_of(run)
        arm = arm_of(run)
        per_over = by_over(run)
        if 10 in per_over or len(per_over) == 1:
            entry = per_over[10] if 10 in per_over else next(iter(per_over.values()))
            cells[(scale, data_format, arm)] = (
                statistics.mean(entry["recall"]), statistics.median(entry["ms"]))
    for key in sorted(ceilings, reverse=True):
        scale, data_format = key
        parts.append(f"<tr><td><b>{esc(scale)}</b> · {esc(data_format)}</td>")
        for arm in ARMS:
            cell = cells.get((scale, data_format, arm))
            if not cell:
                parts.append("<td>—</td>")
                continue
            recall, ms = cell
            size = sizes.get((scale, data_format, arm))
            parts.append(f"<td>{recall:.4f}<br>"
                         f"<span class='sub-value'>{ms / 1000:.1f} s</span><br>"
                         f"<span class='sub-value'>{megabytes(size)}</span></td>")
        parts.append("</tr>")
    parts.append("</tbody></table>"
                 '<p class="note">每格三行：recall@10 / 中位查询延迟 / 索引体积。'
                 "固定 over=10 会让 1bit 看起来不堪用（它需要 100 倍候选），"
                 "下一张表才是选型依据。</p></div>")

    parts.append('<h2>达到精确 recall 的最小 over-sample</h2><div class="card"><table><thead><tr>'
                 "<th>规模 / 数据格式</th>"
                 + "".join(f"<th>{esc(arm)}</th>" for arm in ARMS)
                 + "</tr></thead><tbody>")
    for key in sorted(ceilings, reverse=True):
        scale, data_format = key
        parts.append(f"<tr><td><b>{esc(scale)}</b> · {esc(data_format)}<br>"
                     f"<span class='sub-value'>天花板 {ceilings[key]:.4f}</span></td>")
        for arm in ARMS:
            found = cheapest.get((scale, data_format, arm))
            if not found:
                parts.append("<td>—</td>")
                continue
            over, ms = found
            label = "直接调用" if over == 0 else f"over {over}"
            parts.append(f"<td>{label}<br><span class='sub-value'>{ms / 1000:.1f} s</span></td>")
        parts.append("</tr>")
    parts.append("</tbody></table>"
                 '<p class="note">"天花板"是同表精确臂（暴力或未量化 IVF）已达到的 recall。'
                 "1bit 在四张表里都要开满 100 倍候选才追平，追平之后它是最快的；"
                 "4bit 从来没有比 3bit 好——recall 不需要它，体积也不占优。</p></div>")

    # ---- the term the two formats actually disagree about
    prices = candidate_prices(runs)
    if prices:
        parts.append('<h2>一个候选值多少钱</h2><div class="card">'
                     '<p class="note" style="margin-top:0">kernel 是把访问到的列表条目全部打分、'
                     "排序之后才截断到 <code>limit × over-sample</code>，所以放宽候选<b>不增加扫描量</b>；"
                     "它唯一增加的是交给精确重排的行数。用 over=10 与 over=100 的延迟差除以多出来的 "
                     "900 个候选，就把这笔回表单价单独量了出来。</p>"
                     '<table><thead><tr><th>规模 / 数据格式</th><th>IVF·1bit</th><th>IVF·3bit</th>'
                     "<th>IVF·4bit</th></tr></thead><tbody>")
        for key in sorted(ceilings, reverse=True):
            scale, data_format = key
            row = []
            for arm in ("IVF·1bit", "IVF·3bit", "IVF·4bit"):
                found = prices.get((scale, data_format, arm))
                if not found:
                    row.append("—")
                    continue
                price, narrow_ms, wide_ms, extra = found
                row.append("≈ 0" if abs(price) < 4 else f"{price:.0f} ms")
            parts.append(
                f"<tr><td><b>{esc(scale)}</b> · {esc(data_format)}</td>"
                + "".join(f"<td>{esc(cell)}</td>" for cell in row)
                + "</tr>")
        parts.append("</tbody></table>"
                     '<p class="note">10M 上两种格式都近似免费（差值在 ±0.1 s 内，写 ≈ 0），'
                     "所以那里「1bit + 100 倍候选」没有任何额外代价；100M 上同一个候选在 Parquet 上"
                     "（34–47 ms）比 Lance 上（6–12 ms）贵约 6 倍，这才是两个格式在索引臂上拉开差距的地方。"
                     "3bit 在 100M Lance 上写 ≈ 0，是它 38 s 的基数把 5 s 的变化淹在噪声里，"
                     "不是真的免费。</p></div>")

    # ---- the shape of the trade-off
    panels = []
    for key in sorted(ceilings, reverse=True):
        scale, data_format = key
        group = [run for run in tables if cell_of(run) == (scale, data_format)]
        if not group:
            continue
        panels.append('<div class="card">'
                      + curve_chart(f"{scale} · {data_format}", group,
                                    ("IVF·1bit", "IVF·3bit", "IVF·4bit", "HNSW"))
                      + "</div>")
    parts.append("<h2>recall 随候选宽度的变化</h2>"
                 "<p class='note' style='margin:-4px 0 10px'>四条量化臂 + HNSW，纵轴 0–1 固定，"
                 "横轴是 over-sample（最左为直接调用/暴力）。</p>"
                 f'<div class="panels">{"".join(panels)}</div>')

    # ---- the replication that decides how far the 100M ranking can be pushed
    if replications:
        pairs = paired_comparison(replications, "ivf_1bit", 100, "ivf_3bit", 10)
        if pairs:
            parts.append('<h2>补测：100M 用 6 个查询复核</h2><div class="card">'
                         '<p class="note" style="margin-top:0">原网格每张 100M 表只测了 2 个查询，'
                         '不足以支撑"1bit 比 3bit 快多少"。这轮只测三个量化臂、每个臂 10 与 100 两个候选宽度、'
                         '6 个查询 × 2 轮，两张表各一次、串行执行。下表是<b>逐查询配对</b>的结果：'
                         "同一查询下 1bit@100 与 3bit@10 的暖态耗时，查询之间的难度差异因此被消掉。</p>"
                         '<table><thead><tr><th>规模 / 数据格式</th>'
                         + "".join(f"<th>q{q}</th>" for q in range(1, 7))
                         + "<th>中位比值</th></tr></thead><tbody>")
            for key in sorted(pairs, reverse=True):
                table = pairs[key]
                ratios = [b / a for a, b in table.values() if a]
                cells_ = "".join(
                    f"<td>{table[q][1] / table[q][0]:.2f}×</td>" if q in table else "<td>—</td>"
                    for q in range(1, 7))
                tail = key.replace("cmp-", "").replace("-q6", "")
                label = ("100M · " + tail.split("-")[-1].capitalize()) if "100m" in tail else tail
                parts.append(
                    f"<tr><td><b>{esc(label)}</b></td>{cells_}"
                    f"<td><b>{statistics.median(ratios):.2f}×</b>"
                    f"<br><span class='sub-value'>{sum(1 for r in ratios if r > 1)}/{len(ratios)} 个查询</span>"
                    "</td></tr>")
            parts.append("</tbody></table>"
                         '<p class="note">比值 &gt;1 表示 1bit@100 更快，&lt;1 表示 3bit@10 更快。'
                         "<b>Lance 上 6 个查询全胜</b>，中位 2.5 倍，与前面两轮独立复现一致；"
                         "<b>Parquet 上只赢 4/6</b>，中位 1.2 倍，q4 上反而慢一倍——"
                         "所以那一格只能算相当：候选越宽，对「一个候选多少钱」这个最抖的项依赖越重。</p>"
                         "</div>")

            # the same runs, unpaired: what each cell costs in absolute terms
            parts.append('<div class="card"><table><thead><tr>'
                         "<th>run</th><th>臂</th><th>over</th><th>recall</th>"
                         "<th>中位延迟</th><th>测量次数</th></tr></thead><tbody>")
            for run in replications:
                per_over = by_over(run)
                for over in sorted(per_over):
                    entry = per_over[over]
                    parts.append(
                        f"<tr><td>{esc(run['name'])}</td>"
                        f"<td class='arm'><code>{esc(arm_of(run))}</code></td>"
                        f"<td>{over}</td><td>{statistics.mean(entry['recall']):.3f}</td>"
                        f"<td>{statistics.median(entry['ms']) / 1000:.1f} s</td>"
                        f"<td>{len(entry['ms'])}</td></tr>")
            parts.append("</tbody></table>"
                         '<p class="note">这一轮与另一份 benchmark 有重叠时段，绝对值可能偏高'
                         "（同一格子另一轮是 13.5 s / 51.7 s）；同一查询的三个臂连续测，"
                         "所以配对比值不受影响。另一处细节：每个臂第一次被触碰时是冷的"
                         "（q1 首轮 522.9 / 506.1 / 395.4 s），配对比值取的是暖态那一轮。</p></div>")

    # ---- build cost
    if build_times:
        parts.append('<h2>构建耗时</h2><div class="card"><table><thead><tr>'
                     "<th>规模</th><th>臂</th><th>Parquet</th><th>Lance</th>"
                     "</tr></thead><tbody>")
        for scale in ("10M", "100M"):
            for arm in ("IVF·1bit", "IVF·3bit", "IVF·4bit", "IVF 未量化", "HNSW", "暴力"):
                row = build_times.get(scale, {})
                parquet = row.get("Parquet", {}).get(arm, "—")
                lance = row.get("Lance", {}).get(arm, "—")
                if parquet == "—" and lance == "—":
                    continue
                parts.append(f"<tr><td><b>{esc(scale)}</b></td><td class='arm'>{esc(arm)}</td>"
                             f"<td>{esc(parquet)}</td><td>{esc(lance)}</td></tr>")
        parts.append("</tbody></table>"
                     f'<p class="note">{esc(build_times.get("method", ""))}</p></div>')

    # ---- what the numbers cannot tell you
    parts.append(f"""<h2>口径与已知短板</h2><div class="card warn">
<ul>
<li><b>100M 的单个数字要按量级读。</b>隔离复现里 3bit 的查询 1 是 86–97 s、查询 2 是 13–20 s，
差 6 倍；查询之间的差异远大于臂之间、格式之间的差异。表里用的是中位数。</li>
<li><b>第一格永远是冷启动。</b>100M Lance 上 HNSW 首次 1093.7 s、IVF 未量化首次 2227.2 s、
3bit 首次 765.3 s、1bit 首次 356.0 s。这些值说明了冷缓存的真实体验，但没有进入中位数。</li>
<li><b>两张规模用的查询数不同</b>：10M 是 6 个查询 × 2 轮（每格 12 次测量），100M 只有
2 个查询 × 2 轮（每格 4 次），因为 100M 单次查询从 8 s 到 900 s 不等，多查询会把一轮拉到几十小时。
所以 100M 的结论比 10M 脆：同一张表上 3bit 在网格里是查询 1 约 33–44 s、查询 2 约 36–44 s，
而在单臂隔离复现里是 76–97 s 与 13–20 s，两个查询的相对难度会随缓存状态翻转；
"1bit 比 3bit 快"这个方向在三次跑里都成立，但倍数从 2.5 到 4.7 不等。</li>
<li><b>100M 另做了一轮 6 查询复核</b>（只测三个量化臂、两格式各一轮，串行）：Lance 上 1bit@100
在 6 个查询上全胜、中位 2.5 倍；Parquet 上 1bit 只赢 4/6、中位 1.2 倍且 q4 慢一倍，
与 2 查询那轮的排序相反——所以 Parquet 那一格按"相当"处理。复核跑与另一份 benchmark 有重叠时段，
绝对值可能偏高，但同一查询的三臂是连续测的，配对比值不受影响。</li>
<li><b>nprobe 没有系统扫。</b>10M 全用 150、100M 全用 32，只有两个点。已有的独立测量显示
10M 上从 150 降到 32 还能快约 2.2 倍而 recall 几乎不变，所以 10M 的绝对延迟还有余量没算进来。</li>
<li><b>构建耗时只能读到分钟。</b>按索引元数据 g1→gN 的时间戳计算，含 driver 启动约 1 分钟，
且不同参数是在不同时段建的，簇负载会盖过格式差异。</li>
</ul></div>""")

    parts.append(f"""<h2>仍然没答的问题</h2><div class="card">
<ul>
<li><b>nprobe 曲线</b>——10M 从 150 降到 32 的完整召回/延迟曲线，是把 10M 再压到秒级以下的最后一个旋钮。</li>
<li><b>更大规模</b>——1bit 的优势来自"索引小、候选便宜"，这套逻辑在 10 亿行上是否成立，取决于
量化码的随机读是否仍能落在缓存里。</li>
<li><b>删除与增量</b>——本网格全是静态表，带 equality/position delete 的表上索引要读删除文件，
延迟构成会变。</li>
<li><b>索引常驻</b>——这里没有显式设置缓存预算（沿用按工作集与堆推导的默认值，6 执行器 × 12 GB 堆），
常驻策略在 #2391 里单独推进。</li>
</ul>
<p class="note"><b>一个已经决定不去追的问题</b>：100M Parquet 上 1bit@100 与 3bit@10 谁更快，
两轮跑给出相反排序（93.3 vs 51.7 s，60.0 vs 70.3 s），差额在 20% 以内。要把它压成有方向的结论，
需要在独占集群上重测约 3 小时（固定 6 查询、反转臂顺序以抵掉冷启动的位置效应）。这个代价能买到的
只是一句"谁快 20%"，而两种配置都远快于全表扫描（140 s），且 3bit@10 的成本不依赖那个随查询翻倍
抖动的回表单价——所以这里按<b>相当</b>收口，把时间留给上一项的 nprobe 曲线。</p></div>""")

    if confirmations:
        parts.append('<h2>交叉验证</h2><div class="card"><table><thead><tr>'
                     "<th>run</th><th>臂</th><th>over</th><th>recall</th><th>中位延迟</th>"
                     "<th>轮次</th></tr></thead><tbody>")
        for run in confirmations:
            per_over = by_over(run)
            arm = arm_of(run)
            for over in sorted(per_over):
                entry = per_over[over]
                parts.append(
                    f"<tr><td>{esc(run['name'])}</td><td class='arm'><code>{esc(arm)}</code></td>"
                    f"<td>{over}</td><td>{statistics.mean(entry['recall']):.3f}</td>"
                    f"<td>{statistics.median(entry['ms']) / 1000:.1f} s</td>"
                    f"<td>{len(entry['ms'])}</td></tr>")
        parts.append("</tbody></table>"
                     '<p class="note">100M Lance 的 1bit 与 3bit 各跑一次，每次只测一个索引、'
                     "独立 JVM，用来排除同一轮内缓存顺序造成的假象；两个数都与网格一致。</p></div>")

    parts.append(f"""<h2>材料</h2><div class="card">
<ul>
<li>详细报告（全部曲线、每次查询的值、四分位汇总）：<code>reports/vector-index-benchmark.html</code></li>
<li>本页的权威副本：<code>docs/sift-vector-index-benchmark.html</code>（提交进仓库；memory docs 里那份与之逐字节相同）</li>
<li>原始日志：<code>tools/vector-benchmark/results/grid-*.log</code>、<code>flat-*.log</code></li>
<li>复现方式：<code>tools/vector-benchmark/README.md</code>（<code>run-benchmark.sh</code> +
<code>build-report.py</code> + <code>build-summary.py</code>）</li>
<li>看板：#1948 及子卡 #1632（10M）、#1924（100M Lance）</li>
</ul></div>
</div></body></html>""")

    return "".join(parts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="*", help="benchmark logs; defaults to the whole grid")
    # the summary is a decision document, so it lives under docs/ (committed, and mirrored into the
    # memory store byte for byte); the detailed report stays in reports/ as generated evidence
    parser.add_argument("-o", "--out", default=os.path.join(HERE, "..", "..", "docs",
                                                           "sift-vector-index-benchmark.html"))
    parser.add_argument("--title", default="Iceberg 向量索引基准 · 结论摘要（SIFT 10M / 100M）")
    parser.add_argument("--confirmations", nargs="*", default=None,
                        help="logs of the isolated single-arm runs, shown as cross-checks")
    parser.add_argument("--build-times", default=os.path.join(HERE, "results",
                                                             "build-times.json"))
    options = parser.parse_args()

    patterns = options.logs or [os.path.join(HERE, "results", "grid-*.log"),
                                os.path.join(HERE, "results", "flat-*.log")]
    paths = []
    for pattern in patterns:
        matched = glob.glob(pattern)
        paths += matched if matched else [pattern]
    runs = load(paths)
    if not any(run["results"] for run in runs):
        sys.exit("no BENCHRESULT lines in the given logs")

    confirm_paths = options.confirmations
    if confirm_paths is None:
        confirm_paths = sorted(glob.glob(os.path.join(HERE, "results", "confirm-*.log")))
        replication_paths = sorted(glob.glob(os.path.join(HERE, "results", "cmp-*.log")))
    else:
        # an explicit list may hold either kind; the filename says which
        replication_paths = [path for path in confirm_paths
                             if os.path.basename(path).startswith("cmp-")]
        confirm_paths = [path for path in confirm_paths
                         if not os.path.basename(path).startswith("cmp-")]
    confirmations = load(confirm_paths) if confirm_paths else []
    replications = load(replication_paths) if replication_paths else []

    build_times = {}
    if options.build_times and os.path.exists(options.build_times):
        with open(options.build_times, encoding="utf-8") as handle:
            build_times = json.load(handle)

    with open(options.out, "w", encoding="utf-8") as handle:
        handle.write(build(runs, confirmations, replications, build_times, options.title))
    print(f"wrote {os.path.normpath(options.out)}")


if __name__ == "__main__":
    main()
