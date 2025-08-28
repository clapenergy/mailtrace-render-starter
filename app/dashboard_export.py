# app/dashboard_export.py — summary + dashboard HTML for v17

import pandas as pd
from html import escape

def finalize_summary_for_export_v17(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure the columns the dashboard needs exist and are clean.
    v17 buckets: >=94, 88–94, <88
    """
    # When no matches, return empty with expected columns
    base_cols = [
        "confidence","bucket","match_notes",
        "mail_date","crm_job_date",
        "city","state","zip","address1","address2",
        "crm_address1","crm_address2","crm_city","crm_state","crm_zip",
        "MailID","CustomerID"
    ]
    if df is None or df.empty:
        out = pd.DataFrame(columns=base_cols)
        out["confidence_pct"] = ""
        return out

    out = df.copy()

    # Ensure expected columns exist (prevent KeyErrors)
    for col in base_cols:
        if col not in out.columns:
            out[col] = ""

    # Confidence integer + percent string for display
    try:
        out["confidence"] = pd.to_numeric(out["confidence"], errors="coerce").fillna(0).astype(int)
    except Exception:
        out["confidence"] = 0
    out["confidence_pct"] = out["confidence"].astype(str) + "%"

    # Normalize notes display (never change original data upstream)
    if "match_notes" in out.columns:
        out["match_notes"] = out["match_notes"].fillna("").astype(str)
        out["match_notes"] = out["match_notes"].replace({"NaN": "none", "nan": "none"})

    # Normalize bucket values just in case
    valid = {">=94", "88–94", "<88"}
    if "bucket" in out.columns:
        out["bucket"] = out["bucket"].where(out["bucket"].isin(valid), other="")
    else:
        out["bucket"] = ""

    return out


def _kpi_card(label, value):
    return f"""
    <div class="card kpi">
      <div class="k">{escape(str(label))}</div>
      <div class="v">{escape(str(value))}</div>
    </div>"""


def _safe_city_groups(df: pd.DataFrame) -> pd.DataFrame:
    """Top cities by matches. Falls back to mail city/state if crm_* missing."""
    if df.empty:
        return pd.DataFrame(columns=["city", "state", "matches"])

    use_crm = {"crm_city", "crm_state"}.issubset(df.columns)
    ccol = "crm_city" if use_crm else "city"
    scol = "crm_state" if use_crm else "state"

    # Build groups defensively
    tmp = df[[ccol, scol]].copy()
    tmp[ccol] = tmp[ccol].fillna("").astype(str)
    tmp[scol] = tmp[scol].fillna("").astype(str)

    grp = (
        tmp.groupby([ccol, scol], dropna=False)
           .size()
           .reset_index(name="matches")
           .sort_values("matches", ascending=False)
           .head(12)
    )
    grp = grp.rename(columns={ccol: "city", scol: "state"})
    return grp


def render_full_dashboard_v17(summary_short: pd.DataFrame, mail_total: int) -> str:
    """
    Returns an HTML snippet (string) for the dashboard body.
    Expects summary_short already finalized by finalize_summary_for_export_v17.
    """
    summary_short = summary_short.copy()

    # KPIs
    matches = len(summary_short)
    high = int((summary_short["bucket"] == ">=94").sum()) if "bucket" in summary_short else 0
    mid  = int((summary_short["bucket"] == "88–94").sum()) if "bucket" in summary_short else 0
    low  = int((summary_short["bucket"] == "<88").sum()) if "bucket" in summary_short else 0

    # Top cities (CRM city/state preferred; fall back to mail)
    city_counts = _safe_city_groups(summary_short)

    # Table (cap to 200 rows for speed)
    rows_html = []
    for _, r in summary_short.head(200).iterrows():
        rows_html.append(f"""
        <tr>
          <td>{escape(str(r.get('confidence', '')))}%</td>
          <td>{escape(str(r.get('bucket','')))}</td>
          <td>{escape(str(r.get('address1','')))}</td>
          <td>{escape(str(r.get('city','')))}, {escape(str(r.get('state','')))} {escape(str(r.get('zip','')))}</td>
          <td>{escape(str(r.get('crm_address1','')))}</td>
          <td>{escape(str(r.get('crm_city','')))}, {escape(str(r.get('crm_state','')))} {escape(str(r.get('crm_zip','')))}</td>
          <td>{escape(str(r.get('match_notes','')))}</td>
        </tr>""")

    # City “chips”
    cities_html = ""
    if not city_counts.empty:
        chips = []
        for _, rr in city_counts.iterrows():
            chips.append(
                f"""<span class="badge">{escape(str(rr['city']))}, {escape(str(rr['state']))}: {int(rr['matches'])}</span>"""
            )
        cities_html = "<div class='chips'>" + " ".join(chips) + "</div>"

    # Assemble HTML
    html = f"""
    <div class="container">
      <div class="grid">
        {_kpi_card("Total mail records", mail_total)}
        {_kpi_card("Matches", matches)}
        {_kpi_card("High confidence (≥94)", high)}
        {_kpi_card("Mid confidence (88–94)", mid)}
        {_kpi_card("Low confidence (<88)", low)}
      </div>

      <h3>Top Cities</h3>
      {cities_html or "<div class='note'>No city data available.</div>"}

      <h3 style="margin-top:24px;">Sample of Matches</h3>
      <div class="card">
        <div class="note">Showing up to 200 rows for speed. Use <b>Download All</b> to get the full CSV.</div>
        <div style="overflow:auto;">
          <table>
            <thead>
              <tr>
                <th>Conf.</th>
                <th>Bucket</th>
                <th>Mail Address</th>
                <th>Mail City/State/Zip</th>
                <th>CRM Address</th>
                <th>CRM City/State/Zip</th>
                <th>Notes</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows_html) if rows_html else '<tr><td colspan="7">No matches yet.</td></tr>'}
            </tbody>
          </table>
        </div>
      </div>
    </div>
    """
    return html
