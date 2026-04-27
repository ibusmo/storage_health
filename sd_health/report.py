from __future__ import annotations

import base64
import html
import json
from pathlib import Path
from typing import Any

from sd_health.log import read_jsonl


def load_records_for_report(
    db_path: Path | None,
    jsonl_path: Path | None,
) -> list[dict[str, Any]]:
    """Prefer SQLite; import JSONL into DB when DB is empty; fall back to JSONL."""
    from sd_health.db import list_runs, migrate_from_jsonl
    from sd_health.storage_identity import enrich_identity

    if db_path and jsonl_path:
        migrate_from_jsonl(db_path, jsonl_path)
    if db_path:
        recs = list_runs(db_path)
        if recs:
            return recs
    if jsonl_path and jsonl_path.is_file():
        out: list[dict[str, Any]] = []
        for rec in read_jsonl(jsonl_path):
            if not isinstance(rec, dict):
                continue
            row = dict(rec)
            ident = row.get("identity")
            if isinstance(ident, dict):
                row["identity"] = enrich_identity(ident)
            out.append(row)
        return out
    return []


def build_html_from_records(records: list[dict[str, Any]], source_label: str) -> str:
    raw = json.dumps(records, ensure_ascii=False).encode("utf-8")
    b64 = base64.b64encode(raw).decode("ascii")
    esc = html.escape(source_label)
    n = len(records)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Storage Health — report</title>
  <style>
    :root {{
      --bg: #0f1117; --surface: #1a1d27; --border: #2a2d3a;
      --accent: #4f8ef7; --text: #e2e6f0; --muted: #7a8099;
      --success: #3ddc84; --danger: #f75f5f; --warning: #f7c948;
      --radius: 8px;
    }}
    body {{ margin: 0; padding: 1.25rem 1.5rem 2rem; background: var(--bg); color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; }}
    h1 {{ font-size: 22px; font-weight: 700; color: #fff; margin: 0 0 0.35rem; }}
    .meta {{ color: var(--muted); font-size: 12px; margin-bottom: 1rem; }}
    .summary {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 1rem; }}
    .pill {{ background: var(--surface); border: 1px solid var(--border); border-radius: 20px;
      padding: 4px 12px; font-size: 12px; color: var(--muted); }}
    .controls {{ display: flex; flex-wrap: wrap; gap: 0.75rem; align-items: center; margin-bottom: 1rem; }}
    input[type="search"] {{ background: var(--surface); border: 1px solid var(--border); color: var(--text);
      padding: 7px 12px; border-radius: var(--radius); min-width: 14rem; }}
    .table-wrap {{ border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th {{ background: var(--surface); padding: 11px 12px; text-align: left; font-size: 11px;
      text-transform: uppercase; letter-spacing: 0.6px; color: var(--muted); border-bottom: 1px solid var(--border); }}
    td {{ padding: 9px 12px; border-bottom: 1px solid var(--border); font-size: 13px; vertical-align: top; }}
    tr:last-child td {{ border-bottom: none; }}
    tbody tr:hover {{ background: rgba(79,142,247,0.06); }}
    .pass {{ color: var(--success); font-weight: 600; }}
    .fail {{ color: var(--danger); font-weight: 600; }}
    .err {{ color: var(--warning); font-weight: 600; }}
    code {{ font-family: 'SF Mono', 'Fira Code', monospace; font-size: 11px; }}
  </style>
</head>
<body>
  <h1>Storage Health — offline snapshot</h1>
  <p class="meta">Same data as the live dashboard, embedded for sharing without a server. Source: <code>{esc}</code> — {n} run(s)</p>
  <div class="summary" id="summary"></div>
  <div class="controls">
    <label>Filter <input type="search" id="q" placeholder="search…" /></label>
    <span id="shown" style="color:var(--muted);font-size:12px"></span>
  </div>
  <div class="table-wrap">
    <table id="tbl">
      <thead>
        <tr>
          <th>Finished</th>
          <th>Mode</th>
          <th>Result</th>
          <th>Mount</th>
          <th>Device</th>
          <th>Capacity</th>
          <th>Label</th>
          <th>Notes</th>
          <th>Summary</th>
        </tr>
      </thead>
      <tbody id="body"></tbody>
    </table>
  </div>
  <script>
    const DATA = JSON.parse(new TextDecoder().decode(Uint8Array.from(atob('{b64}'), c => c.charCodeAt(0))));
    function fmtBytes(n) {{
      if (n == null || n === '') return '';
      const x = Number(n);
      if (!Number.isFinite(x)) return String(n);
      const u = ['B','KB','MB','GB','TB'];
      let i = 0, v = x;
      while (v >= 1024 && i < u.length - 1) {{ v /= 1024; i++; }}
      return v.toFixed(i ? 1 : 0) + ' ' + u[i];
    }}
    function escAttr(s) {{
      return String(s ?? '')
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/</g, '&lt;');
    }}
    function cap(r) {{
      const id = r.identity || {{}};
      const tip = (id.capacity_tooltip || '').trim();
      const b = id.capacity_bytes ?? id.extra?.disk_usage?.total;
      const raw = fmtBytes(b);
      const show = (id.capacity_label || '').trim() || raw;
      if (!tip) return show;
      return '<span title="' + escAttr(tip) + '">' + show + '</span>';
    }}
    function label(r) {{
      const id = r.identity || {{}};
      return id.volume_name || id.label || '';
    }}
    function resultClass(res) {{
      if (res === 'pass') return 'pass';
      if (res === 'fail') return 'fail';
      return 'err';
    }}
    function formatTestMode(mode) {{
      const m = String(mode || '').trim().toLowerCase();
      if (m === 'repair') return 'First Aid repair';
      if (m === 'verify') return 'verify (read-only)';
      return String(mode || '');
    }}
    function render(rows) {{
      const tb = document.getElementById('body');
      tb.innerHTML = '';
      for (const r of rows) {{
        const tr = document.createElement('tr');
        tr.innerHTML = [
          '<td>', (r.finished_at || '').replace('T',' ').slice(0,19), '</td>',
          '<td>', formatTestMode(r.test_mode), '</td>',
          '<td class=\"' + resultClass(r.result) + '\">', r.result || '', '</td>',
          '<td>', (r.mount_point || ''), '</td>',
          '<td><code>', (r.device_path || ''), '</code></td>',
          '<td>', cap(r), '</td>',
          '<td>', label(r), '</td>',
          '<td>', (r.operator_notes || ''), '</td>',
          '<td>', (r.summary || '').slice(0, 400), '</td>'
        ].join('');
        tb.appendChild(tr);
      }}
      document.getElementById('shown').textContent = rows.length + ' row(s) shown';
    }}
    function summarize(all) {{
      const s = document.getElementById('summary');
      let pass = 0, fail = 0, err = 0;
      for (const r of all) {{
        if (r.result === 'pass') pass++;
        else if (r.result === 'fail') fail++;
        else err++;
      }}
      s.innerHTML = [
        '<span class=\"pill\">pass: ' + pass + '</span>',
        '<span class=\"pill\">fail: ' + fail + '</span>',
        '<span class=\"pill\">error: ' + err + '</span>'
      ].join('');
    }}
    function applyFilter() {{
      const q = (document.getElementById('q').value || '').toLowerCase();
      if (!q) {{ render(DATA.slice().reverse()); return; }}
      const filtered = DATA.filter(r => JSON.stringify(r).toLowerCase().includes(q));
      render(filtered.slice().reverse());
    }}
    document.getElementById('q').addEventListener('input', applyFilter);
    summarize(DATA);
    applyFilter();
  </script>
</body>
</html>
"""


def build_html(jsonl_path: Path) -> str:
    """Legacy: build from JSONL file only."""
    records = load_records_for_report(None, jsonl_path)
    return build_html_from_records(records, str(jsonl_path))


def write_report(
    out_path: Path,
    *,
    jsonl_path: Path | None = None,
    db_path: Path | None = None,
) -> None:
    records = load_records_for_report(db_path, jsonl_path)
    label = str(db_path or jsonl_path or "sd_health")
    html_doc = build_html_from_records(records, label)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_doc, encoding="utf-8")
