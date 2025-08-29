# app/dashboard_export.py — KPIs + Top Cities/Zips + Month Chart + Date/Amount table
# Adds KPIs:
# - Mailers per Match (total_mail / total_matches)
# - Avg. Mailers Before First Response (count mailers to same normalized address with mail_date < crm_job_date, averaged)

from html import escape
import io
import base64
import re
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Pull normalize_address1 from matching core
try:
    from app.matching_logic_v17 import normalize_address1
except Exception:
    from .matching_logic_v17 import normalize_address1

EXPECTED_COLS = [
    "confidence", "bucket", "match_notes",
    "mail_date", "crm_job_date",
    "city", "state", "zip", "address1", "address2",
    "crm_address1", "crm_address2", "crm_city", "crm_state", "crm_zip",
    "crm_amount",
    "MailID", "CustomerID",
]

_CURRENCY_KEEP = re.compile(r"[^0-9\.\-]")  # keep digits/.-

def _parse_amount_series(s: pd.Series) -> pd.Series:
    if s is None or len(s) == 0:
        return pd.Series([], dtype=float)
    x = s.astype(str).map(lambda v: _CURRENCY_KEEP.sub("", v or ""))
    return pd.to_numeric(x, errors="coerce").fillna(0.0)

def _fmt_currency(x) -> str:
    try:
        val = float(str(x).replace(",", ""))
    except Exception:
        return ""
    return f"${val:,.2f}"

def finalize_summary_for_export_v17(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(getattr(df, "columns", [])) == 0:
        out = pd.DataFrame(columns=EXPECTED_COLS)
        out["confidence"] = pd.Series(dtype="int64")
        out["confidence_pct"] = ""
        out["crm_amount"] = ""
        return out

    out = df.copy()
    for col in EXPECTED_COLS:
        if col not in out.columns:
            out[col] = ""

    # confidence
    try:
        out["confidence"] = pd.to_numeric(out["confidence"], errors="coerce").fillna(0).astype(int)
    except Exception:
        out["confidence"] = 0
    out["confidence_pct"] = out["confidence"].astype(str) + "%"

    # notes cleanup
    out["match_notes"] = out.get("match_notes", "").fillna("").astype(str).replace({"NaN": "none", "nan": "none"})

    # safe strings
    for col in ["address1", "city", "state", "zip", "crm_address1", "crm_city", "crm_state", "crm_zip"]:
        out[col] = out[col].fillna("").astype(str)

    out["crm_amount"] = out.get("crm_amount", "").fillna("").astype(str)
    for col in ["mail_date", "crm_job_date"]:
        out[col] = out.get(col, "").astype(str).fillna("")

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

def _top_cities(df: pd.DataFrame, k: int = 10) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["city", "state", "matches"])
    use_crm = {"crm_city", "crm_state"}.issubset(df.columns)
    ccol = "crm_city" if use_crm else "city"
    scol = "crm_state" if use_crm else "state"
    tmp = df[[ccol, scol]].copy().fillna("").astype(str)
    grp = (
        tmp.groupby([ccol, scol], dropna=False)
           .size()
           .reset_index(name="matches")
           .sort_values("matches", ascending=False)
           .head(k)
           .rename(columns={ccol: "city", scol: "state"})
    )
    return grp

def _top_zips(df: pd.DataFrame, k: int = 10) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["zip", "matches"])
    zcol = "crm_zip" if "crm_zip" in df.columns else "zip"
    z = df[[zcol]].copy().fillna("").astype(str)
    z[zcol] = z[zcol].str[:5]
    grp = (
        z.groupby(zcol, dropna=False)
         .size()
         .reset_index(name="matches")
         .sort_values("matches", ascending=False)
         .head(k)
         .rename(columns={zcol: "zip"})
    )
    return grp

def _matches_by_month_chart_uri(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return ""
    date_col = "crm_job_date" if "crm_job_date" in df.columns else "mail_date"
    series = pd.to_datetime(df[date_col], errors="coerce", utc=False)
    months = series.dropna().dt.to_period("M").astype(str)
    if months.empty:
        return ""
    counts = (
        months.value_counts()
              .rename_axis("month")
              .reset_index(name="matches")
              .sort_values("month")
    )
    fig, ax = plt.subplots(figsize=(8, 3))
    ax.bar(counts["month"], counts["matches"])
    ax.set_xlabel("Month")
    ax.set_ylabel("Matches")
    ax.set_title("Matched Jobs by Month")
    ax.tick_params(axis='x', rotation=45)
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("ascii")
    return f"data:image/png;base64,{b64}"

# ---- KPI calcs that need full mail file ----

def _avg_mailers_before_first_response(matches_df: pd.DataFrame, mail_all_df: pd.DataFrame) -> float | None:
    """
    For each matched row, count how many mailers to the same normalized address stem
    have mail_date strictly before that row's crm_job_date. Then average across matches.
    """
    if matches_df is None or matches_df.empty or mail_all_df is None or mail_all_df.empty:
        return None

    mail_copy = mail_all_df.copy()

    # find a mail address column in uploaded mail file
    for candidate in ["Address1", "Address", "MailAddress1", "address1"]:
        if candidate in mail_copy.columns:
            mail_addr_col = candidate
            break
    else:
        return None

    # find a mail date column
    for dcol in ["MailDate", "Mailed", "Date", "Mail Date", "mail_date"]:
        if dcol in mail_copy.columns:
            mail_date_col = dcol
            break
    else:
        return None

    mail_copy["_stem"] = mail_copy[mail_addr_col].astype(str).map(lambda a: normalize_address1(a)["stem"])
    mail_copy["_mail_dt"] = pd.to_datetime(mail_copy[mail_date_col], errors="coerce", utc=False)

    mx = matches_df.copy()
    mx["_stem"] = mx["address1"].astype(str).map(lambda a: normalize_address1(a)["stem"])
    mx["_crm_dt"] = pd.to_datetime(mx["crm_job_date"], errors="coerce", utc=False)

    mx = mx[(mx["_stem"].astype(str).str.len() > 0) & (mx["_crm_dt"].notna())]
    if mx.empty or mail_copy.empty:
        return None

    mail_groups = mail_copy.dropna(subset=["_stem"]).groupby("_stem", sort=False)

    counts = []
    for _, r in mx.iterrows():
        stem = r["_stem"]
        crm_dt = r["_crm_dt"]
        if stem in mail_groups.indices:
            grp = mail_groups.get_group(stem)
            cnt = int((grp["_mail_dt"] < crm_dt).sum())
            counts.append(cnt)

    if not counts:
        return None

    return round(sum(counts) / len(counts), 2)

def _mailers_per_match(total_mail: int, total_matches: int) -> float | str:
    if not total_matches:
        return "—"
    return round(float(total_mail) / float(total_matches), 2)

# ---- main renderer ----

def render_full_dashboard_v17(
    summary: pd.DataFrame,
    mail_total: int,
    *,
    mail_all_df: pd.DataFrame | None = None
) -> str:
    """
    KPIs → Top Cities → Top ZIPs → Matches by Month → Sample table
    Sample table: CRM Date first; sorted by CRM date desc (fallback to mail date); Amount left; Confidence before Notes.
    """
    summary = summary.copy() if summary is not None else pd.DataFrame(columns=EXPECTED_COLS)
    matches = len(summary)
    match_rate = _pct(matches, mail_total)

    # revenue
    amt_series = _parse_amount_series(summary.get("crm_amount", pd.Series([], dtype=str)))
    total_revenue = float(amt_series.sum()) if len(amt_series) else 0.0
    total_rev_str = _fmt_currency(total_revenue)

    # new KPIs
    kpi_mailers_per_match = _mailers_per_match(mail_total, matches)
    kpi_avg_before = _avg_mailers_before_first_response(summary, mail_all_df) if mail_all_df is not None else None
    avg_before_str = f"{kpi_avg_before:.2f}" if isinstance(kpi_avg_before, float) else "—"

    kpis_html = (
        _kpi("Total Mail", mail_total) +
        _kpi("Matches", matches) +
        _kpi("Match Rate", match_rate) +
        _kpi("Total Revenue Generated", total_rev_str) +
        _kpi("Mailers per Match", kpi_mailers_per_match) +
        _kpi("Avg. Mailers Before First Response", avg_before_str)
    )

    # top cities
    top_cities = _top_cities(summary, 10)
    cities_rows = ""
    if not top_cities.empty:
        for _, r in top_cities.iterrows():
            cities_rows += f"<tr><td>{escape(str(r['city']))}, {escape(str(r['state']))}</td><td>{int(r['matches'])}</td></tr>"
    cities_card = f"""
      <div class="card" style="margin-top:16px;">
        <h3 style="margin-top:0;">Top Cities (matches)</h3>
        <div style="overflow:auto;">
          <table>
            <thead><tr><th>City</th><th>Matches</th></tr></thead>
            <tbody>{cities_rows or '<tr><td colspan="2">No city data.</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    """

    # top zips
    top_zips = _top_zips(summary, 10)
    zips_rows = ""
    if not top_zips.empty:
        for _, r in top_zips.iterrows():
            zips_rows += f"<tr><td>{escape(str(r['zip']))}</td><td>{int(r['matches'])}</td></tr>"
    zips_card = f"""
      <div class="card" style="margin-top:16px;">
        <h3 style="margin-top:0;">Top ZIP Codes (matches)</h3>
        <div style="overflow:auto;">
          <table>
            <thead><tr><th>ZIP</th><th>Matches</th></tr></thead>
            <tbody>{zips_rows or '<tr><td colspan="2">No ZIP data.</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    """

    # month chart
    chart_uri = _matches_by_month_chart_uri(summary)
    month_card = f"""
      <div class="card" style="margin-top:16px;">
        <h3 style="margin-top:0;">Matched Jobs by Month</h3>
        {"<img alt='Matches by month' src='" + chart_uri + "' style='max-width:100%; height:auto;'/>" if chart_uri else "<div class='note'>No date data available.</div>"}
      </div>
    """

    # sample table sorted by CRM date desc (fallback to mail date)
    sort_series = pd.to_datetime(
        summary["crm_job_date"].where(summary["crm_job_date"].astype(str).str.strip() != "", summary["mail_date"]),
        errors="coerce",
        utc=False
    )
    work = summary.copy()
    work["_sort_date"] = sort_series
    work["_display_date"] = work["crm_job_date"].where(
        work["crm_job_date"].astype(str).str.strip() != "", work["mail_date"]
    ).astype(str)
    work = work.sort_values("_sort_date", ascending=False, na_position="last")

    rows_html = []
    for _, r in work.head(200).iterrows():
        amt_disp = _fmt_currency(r.get("crm_amount", "")) if str(r.get("crm_amount", "")).strip() != "" else ""
        crm_date = str(r.get("_display_date", "")).strip()
        rows_html.append(f"""
        <tr>
          <td>{escape(crm_date)}</td>
          <td>{escape(amt_disp)}</td>
          <td>{escape(str(r.get('address1','')))}</td>
          <td>{escape(str(r.get('city','')))}, {escape(str(r.get('state','')))} {escape(str(r.get('zip','')))}</td>
          <td>{escape(str(r.get('crm_address1','')))}</td>
          <td>{escape(str(r.get('crm_city','')))}, {escape(str(r.get('crm_state','')))} {escape(str(r.get('crm_zip','')))}</td>
          <td>{escape(str(r.get('confidence','')))}%</td>
          <td>{escape(str(r.get('match_notes','')))}</td>
        </tr>""")

    table_card = f"""
      <div class="card" style="margin-top:16px;">
        <h3 style="margin-top:0;">Sample of Matches</h3>
        <div class="note">Sorted by most recent CRM date (falls back to mail date).</div>
        <div style="overflow:auto;">
          <table>
            <thead>
              <tr>
                <th>CRM Date</th>
                <th>Amount</th>
                <th>Mail Address</th>
                <th>Mail City/State/Zip</th>
                <th>CRM Address</th>
                <th>CRM City/State/Zip</th>
                <th>Confidence</th>
                <th>Notes</th>
              </tr>
            </thead>
            <tbody>{''.join(rows_html) if rows_html else '<tr><td colspan="8">No matches yet.</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    """

    html = f"""
    <div class="container">
      <div class="grid">{kpis_html}</div>
      <div class="grid">{cities_card}{zips_card}</div>
      {month_card}
      {table_card}
    </div>
    """
    return html
