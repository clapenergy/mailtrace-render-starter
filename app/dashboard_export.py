# app/dashboard_export.py
# MailTrace Dashboard Renderer — v17-preview R7
# - Adds "Mail Dates" as the FAR-LEFT column in the summary table
# - Horizontal "Matched Jobs by Month" chart
# - Top Cities / Top ZIPs (top 5, scrollable)
# - KPI cards: Total Mail, Matches, Total Revenue, Avg Mailers Before Engagement, Mailers per Acquisition
# - Confidence color-coding
#
# Exposed functions:
#   - finalize_summary_for_export_v17(summary_df) -> DataFrame (cleaned for CSV + render)
#   - render_full_dashboard_v17(summary_df, mail_total_count: int) -> HTML string

from __future__ import annotations
import math
import pandas as pd
from datetime import datetime
from io import StringIO

BRAND = "#0c2d4e"
ACCENT = "#759d40"

# -----------------------------
# Helpers
# -----------------------------
def _as_date_any(x) -> pd.Timestamp | None:
    if x is None or (isinstance(x, float) and math.isnan(x)):  # NaN
        return None
    try:
        return pd.to_datetime(x, errors="coerce")
    except Exception:
        return None

def _fmt_currency(x) -> str:
    if x is None or x == "" or (isinstance(x, float) and math.isnan(x)):
        return ""
    try:
        # Strip currency symbols/commas if present
        s = str(x).replace("$", "").replace(",", "").strip()
        val = float(s)
        return "${:,.2f}".format(val)
    except Exception:
        return str(x)

def _parse_currency_to_float(x) -> float:
    if x is None or x == "" or (isinstance(x, float) and math.isnan(x)):
        return 0.0
    try:
        s = str(x).replace("$", "").replace(",", "").strip()
        return float(s)
    except Exception:
        return 0.0

def _coalesce(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v
        if v not in (None, "", float("nan")):
            return v
    return ""

def _join_addr(city, state, zipc) -> str:
    city = str(city or "").strip()
    state = str(state or "").strip()
    zipc = str(zipc or "").strip()
    if city and state and zipc:
        return f"{city}, {state} {zipc}"
    if city and state:
        return f"{city}, {state}"
    if city:
        return city
    return ""

def _join_street_with_unit(street, unit) -> str:
    street = str(street or "").strip()
    unit = str(unit or "").strip()
    if unit:
        # Use comma separator between street and unit as requested
        return f"{street}, {unit}"
    return street

def _month_key(ts: pd.Timestamp | None) -> str:
    if ts is None or pd.isna(ts):
        return ""
    return ts.strftime("%Y-%m")

def _safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

# -----------------------------
# Public: finalize for CSV + render
# -----------------------------
def finalize_summary_for_export_v17(summary: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes columns expected by the renderer & CSV export.
    Expected potential columns from your pipeline/matcher:
      - crm_job_date (string date) OR crm_date
      - amount / crm_amount / value (currency)
      - matched_mail_full_address OR (address1/address2 + city/state/zip)
      - mail_dates_in_window (comma-delimited dd-mm-yy, etc.)  <-- used for Mail Dates column
      - match_notes
      - confidence_percent (0..100) OR confidence
      - crm_* address parts, mail_* address parts
    Returns a NEW DataFrame with normalized columns for the dashboard table.
    """
    df = summary.copy()

    # Normalize confidence column → integer 0..100
    if "confidence_percent" in df.columns:
        conf = df["confidence_percent"]
    elif "confidence" in df.columns:
        conf = df["confidence"]
    else:
        conf = 0
    df["__confidence"] = pd.to_numeric(conf, errors="coerce").fillna(0).astype(int).clip(0, 100)

    # Normalize CRM date
    date_col = None
    for cand in ["crm_job_date", "crm_date", "CRM Date", "job_date"]:
        if cand in df.columns:
            date_col = cand
            break
    # Keep original string for display ordering/fallback parse
    df["__crm_date_raw"] = df[date_col] if date_col else ""

    # Parse to timestamp for sorting and monthly chart
    def _to_ts(x):
        if pd.isna(x) or str(x).strip() == "":
            return pd.NaT
        try:
            return pd.to_datetime(x, errors="coerce")
        except Exception:
            return pd.NaT
    df["__crm_ts"] = df["__crm_date_raw"].apply(_to_ts)

    # Amount normalization
    amt_col = None
    for cand in ["amount", "crm_amount", "job_value", "value", "revenue"]:
        if cand in df.columns:
            amt_col = cand
            break
    if amt_col:
        df["__amount_display"] = df[amt_col].map(_fmt_currency)
        df["__amount_float"] = df[amt_col].map(_parse_currency_to_float)
    else:
        df["__amount_display"] = ""
        df["__amount_float"] = 0.0

    # Build compact display fields for Mail & CRM address lines
    # MAIL side
    mail_street = _coalesce(df.get("matched_mail_full_address", ""))
    if not isinstance(mail_street, pd.Series):
        mail_street = pd.Series([""] * len(df))
    # If matched_mail_full_address not present, try composing from parts
    if mail_street.eq("").all():
        mail_street = _join_street_series(
            df.get("address1",""), df.get("address2","")
        )
    mail_cityline = _join_cityline_series(
        df.get("city",""), df.get("state",""), df.get("zip","")
    )

    # CRM side (street and cityline)
    crm_street_series = _join_street_series(
        df.get("crm_address1","") if "crm_address1" in df.columns else df.get("address1",""),
        df.get("crm_address2","") if "crm_address2" in df.columns else df.get("address2",""),
    )
    crm_cityline_series = _join_cityline_series(
        df.get("crm_city","") if "crm_city" in df.columns else df.get("city",""),
        df.get("crm_state","") if "crm_state" in df.columns else df.get("state",""),
        df.get("crm_zip","") if "crm_zip" in df.columns else df.get("zip",""),
    )

    # Mail dates list (string) – this powers the Mail Dates column
    if "mail_dates_in_window" not in df.columns:
        # Try a fallback if your pipeline named it differently
        for alt in ["mail_dates", "mail_history", "mailing_dates"]:
            if alt in df.columns:
                df["mail_dates_in_window"] = df[alt]
                break
    if "mail_dates_in_window" not in df.columns:
        df["mail_dates_in_window"] = ""  # last resort

    # Notes
    notes_col = "match_notes" if "match_notes" in df.columns else None
    df["__notes"] = df[notes_col] if notes_col else ""

    # Build the *render* DataFrame in the exact column order needed by the UI table.
    out = pd.DataFrame({
        "Mail Dates": df["mail_dates_in_window"],                              # FAR-LEFT
        "CRM Date": df["__crm_date_raw"],
        "Amount": df["__amount_display"],
        "Mail Address": mail_street,
        "Mail City/State/Zip": mail_cityline,
        "CRM Address": crm_street_series,
        "CRM City/State/Zip": crm_cityline_series,
        "Confidence": df["__confidence"].astype(int),
        "Notes": df["__notes"].fillna(""),
    })

    # Keep auxiliary series for KPIs/graph in the same object (so renderer can access)
    out.__dict__["__aux_crm_ts"] = df["__crm_ts"]
    out.__dict__["__aux_amount_float"] = df["__amount_float"]

    # Also keep a tiny projection for Top Cities/ZIPs on the CRM side
    out.__dict__["__aux_crm_city"] = (df.get("crm_city") if "crm_city" in df.columns else df.get("city"))
    out.__dict__["__aux_crm_state"] = (df.get("crm_state") if "crm_state" in df.columns else df.get("state"))
    out.__dict__["__aux_crm_zip"] = (df.get("crm_zip") if "crm_zip" in df.columns else df.get("zip"))

    # For "Avg # mailers before engagement", try to compute from a mail_count column if present
    # (Your matcher often outputs mail_count_in_window.)
    if "mail_count_in_window" in df.columns:
        out.__dict__["__aux_mail_count"] = pd.to_numeric(df["mail_count_in_window"], errors="coerce").fillna(0).astype(int)
    else:
        out.__dict__["__aux_mail_count"] = pd.Series([0]*len(out))

    return out


def _join_street_series(a1, a2) -> pd.Series:
    a1s = pd.Series(a1, dtype="object") if not isinstance(a1, pd.Series) else a1.astype("object")
    a2s = pd.Series(a2, dtype="object") if not isinstance(a2, pd.Series) else a2.astype("object")
    rows = []
    for s1, s2 in zip(a1s, a2s):
        rows.append(_join_street_with_unit(s1, s2))
    return pd.Series(rows, dtype="object")

def _join_cityline_series(city, state, zipc) -> pd.Series:
    cs = pd.Series(city, dtype="object") if not isinstance(city, pd.Series) else city.astype("object")
    ss = pd.Series(state, dtype="object") if not isinstance(state, pd.Series) else state.astype("object")
    zs = pd.Series(zipc, dtype="object") if not isinstance(zipc, pd.Series) else zipc.astype("object")
    rows = []
    for c, s, z in zip(cs, ss, zs):
        rows.append(_join_addr(c, s, z))
    return pd.Series(rows, dtype="object")


# -----------------------------
# Public: HTML renderer
# -----------------------------
def render_full_dashboard_v17(summary_df: pd.DataFrame, mail_total_count: int) -> str:
    """
    Build the full HTML for the dashboard using the normalized summary_df produced
    by finalize_summary_for_export_v17().
    """
    df = summary_df.copy()

    # KPI metrics
    total_mail = mail_total_count
    total_matches = len(df)
    total_revenue = float(df.__dict__.get("__aux_amount_float", pd.Series([0.0]*len(df))).sum())

    # Avg # of mailers before engagement
    mail_counts = df.__dict__.get("__aux_mail_count", pd.Series([0]*len(df)))
    avg_mailers_before = float(mail_counts.mean()) if len(mail_counts) else 0.0

    # Mailers per acquisition = total_mail / total_matches (avoid div by zero)
    mailers_per_acq = (total_mail / total_matches) if total_matches else 0.0

    # Top cities / zips from CRM side, top 5
    crm_city = df.__dict__.get("__aux_crm_city", pd.Series([], dtype="object"))
    crm_state = df.__dict__.get("__aux_crm_state", pd.Series([], dtype="object"))
    crm_zip = df.__dict__.get("__aux_crm_zip", pd.Series([], dtype="object"))

    top_cities = (
        pd.DataFrame({"city": crm_city, "state": crm_state})
        .fillna("")
        .assign(cityline=lambda x: x["city"].astype(str).str.strip() + ", " + x["state"].astype(str).str.strip())
        .groupby("cityline", dropna=False)
        .size()
        .sort_values(ascending=False)
        .head(5)
    )

    top_zips = (
        pd.Series(crm_zip, dtype="object")
        .astype(str).str.strip()
        .replace({"nan": "", "None": ""})
        .groupby(lambda idx: df.index[idx])  # keep alignment
        .apply(lambda x: x)
        .reset_index(drop=True)
        .value_counts()
        .sort_values(ascending=False)
        .head(5)
    )

    # Monthly counts (by CRM date)
    ts = df.__dict__.get("__aux_crm_ts", pd.Series([], dtype="datetime64[ns]"))
    month_counts = (
        pd.Series(ts)
        .dropna()
        .dt.to_period("M")
        .astype(str)
        .value_counts()
        .sort_index()
    )

    # Build HTML table rows (Mail Dates on the far left)
    def conf_class(v: int) -> str:
        # simple color buckets
        if v >= 94:
            return "conf-high"
        if v >= 88:
            return "conf-mid"
        return "conf-low"

    table_rows = []
    # Sort rows by most recent CRM date (falls back to Mail Dates first parsed date)
    # For simplicity, we already have sorted by CRM date outside; but ensure display:
    order_ts = ts.fillna(pd.Timestamp(0))
    order = order_ts.sort_values(ascending=False).index
    df_sorted = df.loc[order]

    for _, r in df_sorted.iterrows():
        mail_dates = str(r.get("Mail Dates") or "")
        crm_date = str(r.get("CRM Date") or "")
        amt = str(r.get("Amount") or "")
        mail_addr = str(r.get("Mail Address") or "")
        mail_cityline = str(r.get("Mail City/State/Zip") or "")
        crm_addr = str(r.get("CRM Address") or "")
        crm_cityline = str(r.get("CRM City/State/Zip") or "")
        conf = _safe_int(r.get("Confidence"), 0)
        notes = str(r.get("Notes") or "")
        table_rows.append(f"""
          <tr>
            <td class="mono">{_escape(mail_dates)}</td>
            <td class="mono">{_escape(crm_date)}</td>
            <td class="mono">{_escape(amt)}</td>
            <td>{_escape(mail_addr)}</td>
            <td>{_escape(mail_cityline)}</td>
            <td>{_escape(crm_addr)}</td>
            <td>{_escape(crm_cityline)}</td>
            <td class="conf {conf_class(conf)}">{conf}%</td>
            <td>{_escape(notes)}</td>
          </tr>
        """)

    # Render top cities/zips list items
    def li_list(series_counts) -> str:
        items = []
        for label, cnt in series_counts.items():
            items.append(f'<div class="li"><span class="name">{_escape(str(label))}</span><span class="cnt">{int(cnt)}</span></div>')
        return "\n".join(items) or "<div class='empty'>No data</div>"

    cities_html = li_list(top_cities)
    zips_html = li_list(top_zips)

    # Horizontal bar chart HTML for month_counts
    chart_html = _horizontal_bar_chart(month_counts)

    # CSS + HTML skeleton
    css = f"""
    <style>
      :root {{
        --brand: {BRAND};
        --accent: {ACCENT};
        --on-brand: #ffffff;
        --bg: #ffffff;
        --text: #0f172a;
        --muted: #64748b;
        --card: #ffffff;
        --border: #e5e7eb;
      }}
      * {{ box-sizing: border-box; }}
      body {{ margin:0; padding:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; background: var(--bg); color: var(--text); }}
      .container {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}

      .grid-kpi {{ display:grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 16px 0 8px; }}
      .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 16px; box-shadow: 0 2px 10px rgba(0,0,0,0.04); }}
      .kpi .k {{ color: var(--muted); font-size: 13px; }}
      .kpi .v {{ font-size: 30px; font-weight: 900; }}
      .kpi {{ border-top: 4px solid var(--brand); }}

      .grid-top {{ display:grid; grid-template-columns: 1fr 1fr; gap: 16px; margin: 8px 0 16px; }}
      .scroll {{ max-height: 200px; overflow: auto; border:1px solid var(--border); border-radius: 12px; padding: 8px; }}
      .li {{ display:flex; justify-content: space-between; padding: 8px 6px; border-bottom: 1px dashed #eef2f7; }}
      .li:last-child {{ border-bottom: 0; }}
      .name {{ font-weight: 700; }}
      .cnt {{ color: var(--muted); }}

      .chart {{ margin: 8px 0 24px; padding: 16px; }}
      .chart .bar {{ display:flex; align-items:center; gap: 8px; margin: 6px 0; }}
      .chart .bar .label {{ width: 110px; text-align: right; font-size: 12px; color: var(--muted); }}
      .chart .bar .fill {{ height: 14px; background: color-mix(in oklab, var(--brand) 22%, white); border:1px solid color-mix(in oklab, var(--brand) 50%, white); border-radius: 8px; }}
      .chart .bar .val {{ font-size: 12px; color: var(--muted); }}

      table {{ width: 100%; border-collapse: collapse; background: #fff; border-radius: 12px; overflow: hidden; }}
      thead th {{ background: #f8fafc; text-align:left; padding: 12px 14px; border-bottom: 1px solid #f1f5f9; font-size: 13px; }}
      tbody td {{ padding: 12px 14px; border-bottom: 1px solid #f3f4f6; font-size: 14px; }}
      tbody tr:hover {{ background:#fafafa; }}
      .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }}
      .conf {{ font-weight: 800; }}
      .conf-high {{ color: #065f46; }}  /* green-ish */
      .conf-mid  {{ color: #92400e; }}  /* amber-ish */
      .conf-low  {{ color: #991b1b; }}  /* red-ish */

      @media (max-width: 900px) {{
        .grid-kpi {{ grid-template-columns: 1fr 1fr; }}
        .grid-top {{ grid-template-columns: 1fr; }}
      }}
    </style>
    """

    kpis_html = f"""
      <div class="grid-kpi">
        <div class="card kpi">
          <div class="k">Total mail records</div>
          <div class="v">{total_mail:,}</div>
        </div>
        <div class="card kpi">
          <div class="k">Matches</div>
          <div class="v">{total_matches:,}</div>
        </div>
        <div class="card kpi">
          <div class="k">Total revenue generated</div>
          <div class="v">${total_revenue:,.2f}</div>
        </div>
        <div class="card kpi">
          <div class="k">Avg mailers before engagement</div>
          <div class="v">{avg_mailers_before:.2f}</div>
        </div>
      </div>
      <div class="card kpi" style="margin-top:-4px;">
        <div class="k">Mailers per acquisition</div>
        <div class="v">{mailers_per_acq:.2f}</div>
      </div>
    """

    top_section = f"""
      <div class="grid-top">
        <div class="card">
          <div class="k" style="margin-bottom:8px;">Top Cities (matches)</div>
          <div class="scroll">{cities_html}</div>
        </div>
        <div class="card">
          <div class="k" style="margin-bottom:8px;">Top ZIP Codes (matches)</div>
          <div class="scroll">{zips_html}</div>
        </div>
      </div>
    """

    chart_section = f"""
      <div class="card chart">
        <div class="k" style="margin-bottom:8px;">Matched Jobs by Month</div>
        {chart_html}
      </div>
    """

    table_html = f"""
      <div class="card">
        <div class="k" style="margin-bottom:8px;">Sample of Matches</div>
        <div class="k" style="color: var(--muted); margin-bottom:12px;">Sorted by most recent CRM date (falls back to mail date).</div>
        <div style="overflow-x:auto;">
          <table>
            <thead>
              <tr>
                <th class="mono">Mail Dates</th>
                <th class="mono">CRM Date</th>
                <th class="mono">Amount</th>
                <th>Mail Address</th>
                <th>Mail City/State/Zip</th>
                <th>CRM Address</th>
                <th>CRM City/State/Zip</th>
                <th>Confidence</th>
                <th>Notes</th>
              </tr>
            </thead>
            <tbody>
              {''.join(table_rows)}
            </tbody>
          </table>
        </div>
      </div>
    """

    html = f"""
    {css}
    <div class="container">
      {kpis_html}
      {top_section}
      {chart_section}
      {table_html}
    </div>
    """
    return html


def _horizontal_bar_chart(month_counts: pd.Series) -> str:
    """Return HTML for a simple horizontal bar chart."""
    if month_counts is None or len(month_counts) == 0:
        return "<div class='empty'>No monthly data</div>"
    max_v = max(int(v) for v in month_counts.values) or 1
    rows = []
    # Keep chronological order by index (YYYY-MM)
    for label, val in month_counts.items():
        pct = int((int(val) / max_v) * 100)
        rows.append(f"""
          <div class="bar">
            <div class="label">{_escape(label)}</div>
            <div class="fill" style="width:{pct}%"></div>
            <div class="val">{int(val)}</div>
          </div>
        """)
    return "\n".join(rows)


def _escape(s: str) -> str:
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
