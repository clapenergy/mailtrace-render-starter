# app/dashboard_export.py — v17 dashboard (tweaked table layout)
# -----------------------------------------------------------------
# - KPIs across the top
# - Confidence Breakdown + Notes Breakdown (same as v17)
# - Sample table:
#       Mail Address | Mail City/State/Zip | CRM Address | CRM City/State/Zip | Confidence | Notes
#
# Bucket column is hidden; Confidence moved next to Notes.

from html import escape
import pandas as pd

EXPECTED_COLS = [
    "confidence", "bucket", "match_notes",
    "mail_date", "crm_job_date",
    "city", "state", "zip", "address1", "address2",
    "crm_address1", "crm_address2", "crm_city", "crm_state", "crm_zip",
    "MailID", "CustomerID",
]

BUCKET_ORDER = [">=94", "88–94", "<88"]


def finalize_summary_for_export_v17(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(getattr(df, "columns", [])) == 0:
        out = pd.DataFrame(columns=EXPECTED_COLS)
        out["confidence"] = pd.Series(dtype="int64")
        out["confidence_pct"] = ""
        return out

    out = df.copy()

    for col in EXPECTED_COLS:
        if col not in out.columns:
            out[col] = ""

    try:
        out["confidence"] = pd.to_numeric(out["confidence"], errors="coerce").fillna(0).astype(int)
    except Exception:
        out["confidence"] = 0
    out["confidence_pct"] = out["confidence"].astype(str) + "%"

    if "bucket" not in out.columns:
        out["bucket"] = ""
    else:
        out["bucket"] = out["bucket"].astype(str)

    if "match_notes" in out.columns:
        out["match_notes"] = out["match_notes"].fillna("").astype(str)
        out["match_notes"] = out["match_notes"].replace({"NaN": "none", "nan": "none"})

    for col in ["address1", "city", "state", "zip", "crm_address1", "crm_city", "crm_state", "crm_zip"]:
        out[col] = out[col].fillna("").astype(str)

    return out


def _kpi(label: str, value) -> str:
    return f"""
    <div class="card kpi">
      <div class="k">{escape(label)}</div>
      <div class="v">{escape(str(value))}</div>
    </div>"""


def _pct(n, d) -> str:
    if not d or d == 0:
        return "0%"
    return f"{round(100.0 * (float(n) / float(d)))}%"


def _bucket_counts(df: pd.DataFrame):
    counts = {b: 0 for b in BUCKET_ORDER}
    if df is None or df.empty or "bucket" not in df.columns:
        return counts
    vc = df["bucket"].value_counts(dropna=False)
    for b in BUCKET_ORDER:
        counts[b] = int(vc.get(b, 0))
    return counts


def _notes_breakdown(df: pd.DataFrame):
    if df is None or df.empty or "match_notes" not in df.columns:
        return {}
    s = df["match_notes"].fillna("").astype(str)
    unit_diff   = int(s.str.contains("(unit)", regex=False).sum())
    stype_diff  = int(s.str.contains("(street type)", regex=False).sum())
    unit_vs_none = int(s.str.contains(" vs none (unit)", regex=False).sum()) + \
                   int(s.str.contains("none vs ", regex=False).sum())
    return {
        "Street type differences": stype_diff,
        "Unit present vs none": unit_vs_none,
        "Unit conflicts (both present, different)": max(0, unit_diff - unit_vs_none),
    }


def render_full_dashboard_v17(summary: pd.DataFrame, mail_total: int) -> str:
    summary = summary.copy() if summary is not None else pd.DataFrame(columns=EXPECTED_COLS)
    matches = len(summary)

    bc = _bucket_counts(summary)
    high = bc[">=94"]; mid = bc["88–94"]; low = bc["<88"]
    match_rate = _pct(matches, mail_total)

    nb = _notes_breakdown(summary)

    kpis_html = (
        _kpi("Total Mail", mail_total) +
        _kpi("Matches", matches) +
        _kpi("Match Rate", match_rate) +
        _kpi("High (≥94)", high) +
        _kpi("Mid (88–94)", mid) +
        _kpi("Low (<88)", low)
    )

    def _row(lbl, cnt):
        return f"<tr><td>{escape(lbl)}</td><td>{cnt}</td><td>{escape(_pct(cnt, matches))}</td></tr>"

    bucket_table = f"""
      <div class="card" style="margin-top:16px;">
        <h3 style="margin-top:0;">Confidence Breakdown</h3>
        <div class="note">Share of matched rows by v17 buckets</div>
        <div style="overflow:auto;">
          <table>
            <thead><tr><th>Bucket</th><th>Count</th><th>% of Matches</th></tr></thead>
            <tbody>
              {_row("High (≥94)", high)}
              {_row("Mid (88–94)", mid)}
              {_row("Low (<88)", low)}
            </tbody>
          </table>
        </div>
      </div>
    """

    notes_rows = ""
    if nb:
        for k, v in nb.items():
            notes_rows += f"<tr><td>{escape(k)}</td><td>{int(v)}</td><td>{escape(_pct(v, matches))}</td></tr>"
    notes_table = f"""
      <div class="card" style="margin-top:16px;">
        <h3 style="margin-top:0;">Why scores dropped (notes)</h3>
        <div class="note">Counts come from the <code>match_notes</code> text.</div>
        <div style="overflow:auto;">
          <table>
            <thead><tr><th>Reason</th><th>Count</th><th>% of Matches</th></tr></thead>
            <tbody>
              {notes_rows or '<tr><td colspan="3">No notes.</td></tr>'}
            </tbody>
          </table>
        </div>
      </div>
    """

    rows_html = []
    for _, r in summary.head(200).iterrows():
        rows_html.append(f"""
        <tr>
          <td>{escape(str(r.get('address1','')))}</td>
          <td>{escape(str(r.get('city','')))}, {escape(str(r.get('state','')))} {escape(str(r.get('zip','')))}</td>
          <td>{escape(str(r.get('crm_address1','')))}</td>
          <td>{escape(str(r.get('crm_city','')))}, {escape(str(r.get('crm_state','')))} {escape(str(r.get('crm_zip','')))}</td>
          <td>{escape(str(r.get('confidence','')))}%</td>
          <td>{escape(str(r.get('match_notes','')))}</td>
        </tr>""")

    table_html = f"""
      <div class="card" style="margin-top:16px;">
        <h3 style="margin-top:0;">Sample of Matches</h3>
        <div class="note">Showing up to 200 rows. Use <b>Download All</b> to export the full CSV.</div>
        <div style="overflow:auto;">
          <table>
            <thead>
              <tr>
                <th>Mail Address</th>
                <th>Mail City/State/Zip</th>
                <th>CRM Address</th>
                <th>CRM City/State/Zip</th>
                <th>Confidence</th>
                <th>Notes</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows_html) if rows_html else '<tr><td colspan="6">No matches yet.</td></tr>'}
            </tbody>
          </table>
        </div>
      </div>
    """

    html = f"""
    <div class="container">
      <div class="grid">
        {kpis_html}
      </div>
      {bucket_table}
      {notes_table}
      {table_html}
    </div>
    """
    return html
