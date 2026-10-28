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

#!/usr/bin/env python3
"""Build the merged reading page for the Lance-Spark research documents.

The Markdown files stay authoritative; ``index.html`` beside them is a reading
view that keeps every paragraph. Run this after editing any of the four:

    python3 docs/research/build_index_html.py
"""

import html
import re
import sys
from pathlib import Path

RESEARCH = Path(__file__).resolve().parent

DOCS = [
    ("2026-10-28-lance-spark-ann-comparison.md", "#2810 / #2814",
     "两张体系对比卡：先看这一篇，其余三篇是它在两个方向上的放大镜"),
    ("2026-10-28-lance-index-kernel-reuse.md", "#2817 / #2818",
     "能不能借用 Lance 的索引算法，同时把表管理留在 Iceberg"),
    ("2026-10-28-lance-spark-sql-syntax-comparison.md", "#2818",
     "同一件事，两边各怎么写，以及哪些写法看着像其实不等价"),
    ("2026-10-28-lance-spark-sql-alignment-feasibility.md", "#2820",
     "如果要往 Lance-Spark 的写法靠，边界、工作量与收益各是什么"),
]

ANCHOR = {name: "doc%d" % (index + 1) for index, (name, _, _) in enumerate(DOCS)}

FENCE = re.compile(r"^\s*```")
HEADING = re.compile(r"^(#{1,6})\s+(.*)$")
LIST_ITEM = re.compile(r"^(\s*)([-*+]|\d+[.)])\s+(.*)$")
TABLE_SEP = re.compile(r"^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$")
HR = re.compile(r"^\s*(-{3,}|\*{3,}|_{3,})\s*$")
DEF = re.compile(r'^\[([^\]]+)\]:\s*(\S+?)(?:\s+"(.*)")?\s*$')
NUM = re.compile(r"\[(\d+)\]")


def sibling(url):
    """Point sibling Markdown links at the merged page's own sections."""
    for name, anchor in ANCHOR.items():
        if url.endswith(name):
            return "#" + anchor
    return url


class Doc:
    """Renders one Markdown document, keeping its link references and lists."""

    def __init__(self, name, position):
        self.name = name
        self.prefix = "doc%d" % position
        self.defs = {}
        self.labels = []
        self.evidence = set()   # numbers cited as [n] and resolvable in this doc
        self.bullets = 0        # ordered-list items emitted so far

    # -- inline ---------------------------------------------------------

    def inline(self, text):
        codes = []

        def keep(match):
            codes.append(match.group(1))
            return "\x00%d\x00" % (len(codes) - 1)

        text = re.sub(r"`([^`]+)`", keep, text)
        text = html.escape(text, quote=False)

        def note_of(label):
            note = self.defs.get(label, (None, None))[1]
            return ' title="%s"' % html.escape(note, quote=True) if note else ""

        def numeric(match):
            label = match.group(1)
            url = self.defs.get(label, (None, None))[0]
            if url:
                return '<a class="ref" href="%s"%s>[%s]</a>' % (
                    html.escape(sibling(url), quote=True), note_of(label), label)
            if label in self.evidence:
                return '<a class="ref" href="#%s-ev%s">[%s]</a>' % (self.prefix, label, label)
            return match.group(0)

        def full(match):
            label = match.group(2) or match.group(1)
            url = self.defs.get(label, (None, None))[0]
            if not url:
                return match.group(0)
            return '<a href="%s"%s>%s</a>' % (html.escape(sibling(url), quote=True),
                                              note_of(label), match.group(1))

        # numeric markers first: "[1][2]" is two citations, not a reference labelled "1"
        text = NUM.sub(numeric, text)
        text = re.sub(r"\[([^\]]+)\]\[([^\]]*)\]", full, text)
        text = re.sub(r"\[([^\]]+)\]\(([^)\s]+)\)",
                      lambda m: '<a href="%s">%s</a>' % (html.escape(sibling(m.group(2)),
                                                                     quote=True), m.group(1)),
                      text)

        def shortcut(match):
            label = match.group(1)
            if label.isdigit():
                return match.group(0)
            url = self.defs.get(label, (None, None))[0]
            if not url:
                return match.group(0)
            return '<a class="ref" href="%s"%s>[%s]</a>' % (
                html.escape(sibling(url), quote=True), note_of(label), label)

        text = re.sub(r"\[([^\]\[]+)\]", shortcut, text)
        text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
        text = re.sub(r"__([^_]+)__", r"<strong>\1</strong>", text)
        text = re.sub(r"(?<![\w*])\*([^*\n]+)\*(?![\w*])", r"<em>\1</em>", text)
        text = re.sub(r"~~([^~]+)~~", r"<del>\1</del>", text)

        def restore(match):
            return "<code>%s</code>" % html.escape(codes[int(match.group(1))], quote=False)

        return re.sub(r"\x00(\d+)\x00", restore, text)

    # -- blocks ---------------------------------------------------------

    @staticmethod
    def cells(line):
        line = line.strip()
        if line.startswith("|"):
            line = line[1:]
        if line.endswith("|"):
            line = line[:-1]
        return [cell.strip() for cell in line.split("|")]

    def table(self, lines):
        header = self.cells(lines[0])
        aligns = []
        for cell in self.cells(lines[1]):
            left, right = cell.startswith(":"), cell.endswith(":")
            aligns.append("center" if left and right else "right" if right else "left")
        out = ['<div class="table-wrap"><table>', "<thead><tr>"]
        for index, cell in enumerate(header):
            align = aligns[index] if index < len(aligns) else "left"
            out.append('<th style="text-align:%s">%s</th>' % (align, self.inline(cell)))
        out.append("</tr></thead><tbody>")
        for line in lines[2:]:
            if not line.strip():
                continue
            out.append("<tr>")
            for index, cell in enumerate(self.cells(line)):
                align = aligns[index] if index < len(aligns) else "left"
                out.append('<td style="text-align:%s">%s</td>' % (align, self.inline(cell)))
            out.append("</tr>")
        out.append("</tbody></table></div>")
        return "".join(out)

    def listing(self, items):
        def build(index, indent):
            parts = []
            group = None
            while index < len(items):
                level, ordered, text = items[index]
                if level < indent:
                    break
                if level > indent:
                    nested, index = build(index, level)
                    parts.append(nested)
                    continue
                if group != ordered:
                    if group is not None:
                        parts.append("</li>%s>" % ("ol" if group else "ul"))
                    parts.append("<%s>" % ("ol" if ordered else "ul"))
                    group = ordered
                else:
                    parts.append("</li>")
                if ordered:
                    self.bullets += 1
                    ident = ' id="%s-ev%d"' % (self.prefix, self.bullets)
                else:
                    ident = ""
                parts.append("<li%s>%s" % (ident, self.inline(text)))
                index += 1
            if group is not None:
                parts.append("</li>%s>" % ("ol" if group else "ul"))
            return "".join(parts), index

        if not items:
            return ""
        return build(0, items[0][0])[0]

    def refs_block(self):
        rows = []
        for label in self.labels:
            url, note = self.defs[label]
            rows.append('<li><a href="%s">%s</a>%s</li>'
                        % (html.escape(sibling(url), quote=True), html.escape(label),
                           '<span class="refnote">%s</span>' % html.escape(note) if note else ""))
        return ('<details class="refs"><summary>链接定义 · %d 条（正文以 [说明][标签] 或 [n] 引用）'
                '</summary><ul class="reflist">%s</ul></details>'
                % (len(self.labels), "".join(rows)))

    # -- document -------------------------------------------------------

    def parse(self, text):
        lines = text.split("\n")
        while lines and (not lines[0].strip() or lines[0].lstrip().startswith("<!--")):
            if lines[0].lstrip().startswith("<!--"):
                while lines and "-->" not in lines[0]:
                    lines.pop(0)
                if lines:
                    lines.pop(0)
            else:
                lines.pop(0)

        skip = set()
        for number, line in enumerate(lines):
            match = DEF.match(line)
            if match:
                self.defs[match.group(1)] = (match.group(2), match.group(3))
                self.labels.append(match.group(1))
                skip.add(number)

        # A numeric citation only resolves inside the document that has a numbered
        # evidence list, and only for the numbers that list actually carries.
        blocks = self.ordered_blocks(lines, skip)
        if len(blocks) == 1:
            self.evidence = set(str(number) for number in range(1, blocks[0] + 1))

        nodes = []
        title = None
        refs_done = False
        index = 0
        while index < len(lines):
            line = lines[index]
            if index in skip:
                if not refs_done:
                    nodes.append(("html", self.refs_block()))
                    refs_done = True
                index += 1
                continue
            if not line.strip():
                index += 1
                continue

            if FENCE.match(line):
                lang = line.strip().lstrip("`").strip()
                index += 1
                body = []
                while index < len(lines) and not FENCE.match(lines[index]):
                    body.append(lines[index])
                    index += 1
                index += 1
                klass = ' class="language-%s"' % html.escape(lang) if lang else ""
                nodes.append(("html", "<pre><code%s>%s</code></pre>"
                              % (klass, html.escape("\n".join(body), quote=False))))
                continue

            match = HEADING.match(line)
            if match:
                level = len(match.group(1))
                heading = match.group(2).strip()
                if level == 1 and title is None:
                    title = heading
                else:
                    nodes.append(("heading", level, heading))
                index += 1
                continue

            if line.lstrip().startswith("|") and index + 1 < len(lines) \
                    and TABLE_SEP.match(lines[index + 1]):
                block = [line, lines[index + 1]]
                index += 2
                while index < len(lines) and lines[index].lstrip().startswith("|"):
                    block.append(lines[index])
                    index += 1
                nodes.append(("html", self.table(block)))
                continue

            match = LIST_ITEM.match(line)
            if match:
                items = []
                while index < len(lines):
                    current = LIST_ITEM.match(lines[index])
                    if current:
                        indent = len(current.group(1).expandtabs(2))
                        ordered = current.group(2)[0].isdigit()
                        items.append((indent, ordered, current.group(3).strip()))
                        index += 1
                    elif lines[index].strip() and lines[index].startswith("  ") \
                            and index not in skip:
                        indent, ordered, previous = items[-1]
                        items[-1] = (indent, ordered, previous + " " + lines[index].strip())
                        index += 1
                    else:
                        break
                nodes.append(("html", self.listing(items)))
                continue

            if HR.match(line):
                nodes.append(("html", "<hr/>"))
                index += 1
                continue

            paragraph = []
            while index < len(lines) and lines[index].strip() and index not in skip:
                current = lines[index]
                if FENCE.match(current) or HEADING.match(current) or LIST_ITEM.match(current) \
                        or HR.match(current):
                    break
                if current.lstrip().startswith("|") and index + 1 < len(lines) \
                        and TABLE_SEP.match(lines[index + 1]):
                    break
                paragraph.append(current.strip())
                index += 1
            if not paragraph:
                paragraph = [lines[index].strip()]
                index += 1
            nodes.append(("html", "<p>%s</p>" % self.inline(" ".join(paragraph))))

        return title, nodes

    @staticmethod
    def ordered_blocks(lines, skip):
        """Sizes of the document's top-level numbered lists."""
        sizes = []
        inside = False
        fence = False
        for number, line in enumerate(lines):
            if FENCE.match(line):
                fence = not fence
                continue
            if fence or number in skip:
                inside = False
                continue
            match = re.match(r"^(\d+)[.)]\s+\S", line)
            if match:
                if not inside:
                    sizes.append(0)
                    inside = True
                sizes[-1] += 1
            elif not line.strip() or line.startswith("  "):
                continue
            else:
                inside = False
        return sizes


def build_tree(nodes):
    """Nest blocks under their headings: an h2 owns blocks until the next one."""
    root = {"level": 0, "title": None, "blocks": [], "children": []}
    stack = [root]
    for node in nodes:
        if node[0] == "heading":
            _, level, text = node
            while stack[-1]["level"] >= level and len(stack) > 1:
                stack.pop()
            section = {"level": level, "title": text, "blocks": [], "children": []}
            stack[-1]["children"].append(section)
            stack.append(section)
        else:
            stack[-1]["blocks"].append(node[1])
    return root


def emit(section, doc, counter):
    anchor = "%s-s%d" % (doc.prefix, counter[0])
    counter[0] += 1
    body = "".join(section["blocks"])
    children = "".join(emit(child, doc, counter) for child in section["children"])
    title = doc.inline(section["title"]) if section["title"] is not None else ""
    depth = section["level"]
    if depth == 2:
        return ('<details class="section" id="%s"><summary><span class="depth">§</span>%s</summary>'
                '<div class="body">%s%s</div></details>' % (anchor, title, body, children))
    if depth == 3:
        return ('<details class="subsection" id="%s"><summary>%s</summary>'
                '<div class="body">%s%s</div></details>' % (anchor, title, body, children))
    return '<div class="plain" id="%s">%s%s</div>' % (anchor, body, children)


def toc(section, doc, counter, out, depth=0):
    anchor = "%s-s%d" % (doc.prefix, counter[0])
    counter[0] += 1
    if section["title"] is None:
        for child in section["children"]:
            toc(child, doc, counter, out, depth)
        return
    out.append('<li class="lvl%d"><a href="#%s">%s</a>'
               % (depth, anchor, doc.inline(section["title"])))
    child_out = []
    for child in section["children"]:
        toc(child, doc, counter, child_out, depth + 1)
    if child_out:
        out.append('<ol>%s</ol>' % "".join(child_out))
    out.append("</li>")


def count(section, tally):
    tally[0] += 1
    for child in section["children"]:
        count(child, tally)


def main():
    parts = []
    sidebar = []
    for position, (name, cards, blurb) in enumerate(DOCS, start=1):
        doc = Doc(name, position)
        title, nodes = doc.parse((RESEARCH / name).read_text(encoding="utf-8"))
        tree = build_tree(nodes)
        tally = [0]
        count(tree, tally)
        body = emit(tree, doc, [0])
        parts.append(
            '<details class="doc" id="%s"%s><summary><span class="docno">%d</span>'
            '<span class="doctitle">%s</span><span class="docmeta">%s · %d 节</span></summary>'
            '<div class="docbody"><p class="blurb">%s</p>%s'
            '<p class="source">原文：<code>docs/research/%s</code></p></div></details>'
            % (doc.prefix, " open" if position == 1 else "", position, doc.inline(title), cards,
               tally[0] - 1, html.escape(blurb), body, name))
        entries = []
        toc(tree, doc, [0], entries)
        sidebar.append('<li class="doc-item"><a href="#%s">%d. %s</a><ol>%s</ol></li>'
                       % (doc.prefix, position, doc.inline(title), "".join(entries)))

    page = TEMPLATE.replace("{DOCS}", "".join(parts)).replace("{TOC}", "".join(sidebar))
    out = RESEARCH / "index.html"
    out.write_text(page, encoding="utf-8")
    print("wrote", out, len(page), "bytes")


TEMPLATE = r"""<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Lance-Spark 与 Iceberg 索引：调研合集</title>
<style>
  :root {
    --ink: #1c2024; --muted: #5d666f; --line: #e2e6ea; --bg: #fbfbfa;
    --panel: #ffffff; --accent: #1f6feb; --code-bg: #f4f5f7; --mark: #fff6d6;
  }
  * { box-sizing: border-box; }
  html { scroll-behavior: smooth; }
  body {
    margin: 0; background: var(--bg); color: var(--ink);
    font: 15px/1.78 -apple-system, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
  }
  header.top { padding: 28px 32px 18px; border-bottom: 1px solid var(--line); background: var(--panel); }
  header.top h1 { margin: 0 0 6px; font-size: 22px; letter-spacing: .01em; }
  header.top p { margin: 0; color: var(--muted); font-size: 13px; max-width: 900px; }
  .layout { display: grid; grid-template-columns: 300px minmax(0, 1fr); align-items: start; }
  nav.toc {
    position: sticky; top: 0; max-height: 100vh; overflow: auto;
    padding: 18px 14px 40px; border-right: 1px solid var(--line); font-size: 13px;
  }
  nav.toc h2 { font-size: 12px; text-transform: uppercase; letter-spacing: .08em;
    color: var(--muted); margin: 4px 0 10px; }
  nav.toc ol { list-style: none; margin: 0; padding-left: 12px; }
  nav.toc > ol { padding-left: 0; }
  nav.toc li { margin: 2px 0; }
  nav.toc a { color: var(--ink); text-decoration: none; display: block; padding: 2px 6px; border-radius: 4px; }
  nav.toc a:hover { background: #eef3fb; color: var(--accent); }
  nav.toc li.lvl1 > a { font-weight: 700; margin-top: 10px; }
  nav.toc li.lvl2 > a, nav.toc li.lvl3 > a { color: var(--muted); }
  main { padding: 22px 34px 90px; max-width: 1080px; }
  .controls { margin-bottom: 14px; display: flex; gap: 10px; flex-wrap: wrap; }
  .controls button {
    font: inherit; font-size: 13px; padding: 5px 12px; border-radius: 6px; cursor: pointer;
    border: 1px solid var(--line); background: var(--panel); color: var(--ink);
  }
  .controls button:hover { border-color: var(--accent); color: var(--accent); }
  details.doc { border: 1px solid var(--line); border-radius: 10px; background: var(--panel); margin: 0 0 18px; }
  details.doc > summary {
    list-style: none; cursor: pointer; padding: 16px 18px; display: flex;
    gap: 12px; align-items: baseline; flex-wrap: wrap;
  }
  details.doc > summary::-webkit-details-marker { display: none; }
  details.doc > summary::before { content: "▸"; color: var(--muted); font-size: 12px; }
  details.doc[open] > summary::before { content: "▾"; }
  details.doc > summary:hover { background: #fafbfc; }
  .docno { font-weight: 700; color: var(--accent); font-variant-numeric: tabular-nums; }
  .doctitle { font-weight: 700; font-size: 17px; }
  .docmeta { color: var(--muted); font-size: 12px; margin-left: auto; }
  .docbody { padding: 4px 22px 18px; border-top: 1px solid var(--line); }
  .blurb { color: var(--muted); font-size: 13px; margin: 12px 0 4px; }
  details.section { border-top: 1px dashed var(--line); margin-top: 6px; }
  details.section > summary, details.subsection > summary {
    cursor: pointer; list-style: none; padding: 9px 2px; font-weight: 650; font-size: 16px;
  }
  details.section > summary::-webkit-details-marker,
  details.subsection > summary::-webkit-details-marker { display: none; }
  details.section > summary::before,
  details.subsection > summary::before { content: "▸ "; color: var(--muted); }
  details.section[open] > summary::before,
  details.subsection[open] > summary::before { content: "▾ "; }
  details.section > summary:hover, details.subsection > summary:hover { color: var(--accent); }
  .depth { color: var(--muted); font-weight: 400; margin-right: 8px; }
  details.subsection { margin: 2px 0 2px 14px; padding-left: 12px; border-left: 2px solid #eef1f4; }
  details.subsection > summary { font-size: 14px; font-weight: 600; }
  .body { padding: 0 4px 6px; }
  p { margin: 10px 0; }
  ol, ul { padding-left: 22px; }
  li { margin: 4px 0; }
  strong { font-weight: 700; }
  a { color: var(--accent); }
  a.ref { text-decoration: none; font-size: 12px; vertical-align: super; }
  code {
    font: 12.5px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace;
    background: var(--code-bg); padding: 1px 5px; border-radius: 4px;
  }
  pre {
    background: #1f2430; color: #e6e9ef; padding: 14px 16px; border-radius: 8px;
    overflow: auto; font: 12.5px/1.6 ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  pre code { background: none; color: inherit; padding: 0; }
  .table-wrap { overflow-x: auto; margin: 14px 0; }
  table { border-collapse: collapse; width: 100%; font-size: 13.5px; }
  th, td { border: 1px solid var(--line); padding: 8px 10px; vertical-align: top; }
  th { background: #f7f9fb; font-weight: 650; }
  tbody tr:nth-child(even) { background: #fcfdff; }
  hr { border: none; border-top: 1px solid var(--line); margin: 22px 0; }
  .source { color: var(--muted); font-size: 12px; margin-top: 18px; }
  details.refs { margin: 14px 0 6px; font-size: 12.5px; }
  details.refs > summary { cursor: pointer; color: var(--muted); }
  .reflist { margin: 8px 0 0; padding-left: 18px; }
  .reflist li { margin: 3px 0; }
  .refnote { color: var(--muted); margin-left: 8px; }
  :target > summary { background: var(--mark); }
  @media (max-width: 900px) {
    .layout { grid-template-columns: 1fr; }
    nav.toc { position: static; max-height: none; border-right: none; border-bottom: 1px solid var(--line); }
    main { padding: 18px 16px 60px; }
  }
  @media print {
    nav.toc, .controls { display: none; }
    details.doc, details.section, details.subsection { border: none; }
    main { max-width: none; padding: 0; }
  }
</style>
</head>
<body>
<header class="top">
  <h1>Lance-Spark 与 Iceberg 索引：调研合集</h1>
  <p>四份调研文档的合并阅读版，来自看板伞卡 #2819（子卡 #2810 / #2814 / #2817 / #2818 / #2820）。
     内容按原文逐段保留，未做改写或摘要；Markdown 原文仍是权威件，路径见每篇末尾。结论均为研究建议，
     不代表已批准实现，也不代表已通过发布验收。</p>
</header>
<div class="layout">
  <nav class="toc">
    <h2>目录</h2>
    <ol>{TOC}</ol>
  </nav>
  <main>
    <div class="controls">
      <button type="button" onclick="setAll(true)">展开全部</button>
      <button type="button" onclick="setAll(false)">收起全部</button>
      <button type="button" onclick="window.print()">打印 / 存 PDF</button>
    </div>
    {DOCS}
  </main>
</div>
<script>
  function setAll(open) {
    document.querySelectorAll('details').forEach(function (node) { node.open = open; });
    var first = document.querySelector('details.doc');
    if (first) { first.open = true; }
  }
  function reveal(id) {
    var target = document.getElementById(id);
    if (!target) { return; }
    while (target) {
      if (target.tagName === 'DETAILS') { target.open = true; }
      target = target.parentElement;
    }
  }
  document.querySelectorAll('nav.toc a').forEach(function (link) {
    link.addEventListener('click', function () { reveal(link.getAttribute('href').slice(1)); });
  });
  // a deep link into a collapsed section opens the sections that contain it
  if (window.location.hash.length > 1) { reveal(decodeURIComponent(window.location.hash.slice(1))); }
  window.addEventListener('beforeprint', function () { setAll(true); });
</script>
</body>
</html>
"""

if __name__ == "__main__":
    sys.exit(main())
