# app/dashboard_export.py
# MailTrace Dashboard Renderer — v17-preview R8a (robust columns, KPI fix)
# REVERTED TO ORIGINAL + ONLY Mail Dates fix

from __future__ import annotations
import math, re
import pandas as pd

BRAND = "#0c2d4e"
ACCENT = "#759d40"

# ---------- small utils ----------
def _lower_map(cols):
    m = {}
    for c in cols:
        lc = str(c).strip().lower()
        if lc not in m:
            m[lc] = c
    return m

def _get_col_ci(df: pd.DataFrame, *candidates) -> str | None:
    m = _lower_map(df.columns)
    for cand in candidates:
        lc = str(cand).strip().lower()
        if lc in m:
            return m[lc]
    return None

def _as_ts_flexible(s):
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return pd.NaT
    txt = str(s).strip()
    if not txt:
        return pd.NaT
    ts = pd.to_datetime(txt, errors="coerce", infer_datetime_format=True)
    if pd.isna(ts):
        ts = pd.to_datetime(txt, errors="coerce", dayfirst=True)
    if pd.isna(ts):
        ts2 = txt.replace("/", "-")
        ts = pd.to_datetime(ts2, errors="coerce", dayfirst=True)
    return ts

def _fmt_currency(x) -> str:
    if x is None or x == "" or (isinstance(x, float) and math.isnan(x)):
        return ""
    try:
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
        return f"{street}, {unit}"
    return street

def _safe_int(x, default=0):
    try:
        if isinstance(x, str) and x.endswith("%"):
            x = x[:-1]
        return int(float(x))
    except Exception:
        return default

def _escape(s: str) -> str:
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

# ---------- series builders ----------
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

# ---------- public: finalize ----------
def finalize_summary_for_export_v17(summary: pd.DataFrame) -> pd.DataFrame:
    df_raw = summary.copy()

    # Confidence
    conf_col = _get_col_ci(df_raw, "confidence_percent", "confidence", "match_confidence", "score")
    if conf_col:
        conf_vals = df_raw[conf_col].map(lambda x: _safe_int(x, 0)).fillna(0).astype(int).clip(0, 100)
    else:
        conf_vals = pd.Series([0]*len(df_raw))

    # CRM Date
    crm_date_col = _get_col_ci(df_raw, "crm_job_date", "crm_date", "job_date", "crm date", "crm-date", "date")
    crm_date_raw = df_raw[crm_date_col] if crm_date_col else pd.Series([""]*len(df_raw))
    crm_ts = crm_date_raw.map(_as_ts_flexible)

    # Amount / revenue
    amt_col = None
    for cand in ["amount", "crm_amount", "job_value", "value", "revenue", "Amount", "Job Value"]:
        c = _get_col_ci(df_raw, cand)
        if c:
            amt_col = c
            break
    if amt_col:
        amt_disp = df_raw[amt_col].map(_fmt_currency)
        amt_float = df_raw[amt_col].map(_parse_currency_to_float)
    else:
        amt_disp = pd.Series([""]*len(df_raw))
        amt_float = pd.Series([0.0]*len(df_raw))

    # ONLY FIX: Mail Dates (check exact column name)
    mail_dates_col = _get_col_ci(df_raw, "mail_dates_in_window", "mail_dates", "mailing_dates", "mail history", "mail_history")
    if mail_dates_col:
        print(f"DEBUG: Found mail dates column '{mail_dates_col}'")
        print(f"DEBUG: Sample values: {df_raw[mail_dates_col].head(3).tolist()}")
        mail_dates_series = df_raw[mail_dates_col]
    else:
        print(f"DEBUG: No mail dates column found in {list(df_raw.columns)}")
        mail_dates_series = pd.Series([""]*len(df_raw))

    # Mail side
    full_mail_col = _get_col_ci(df_raw, "matched_mail_full_address")
    if full_mail_col:
        mail_street_series = df_raw[full_mail_col].fillna("")
        mail_city_series = df_raw.get(_get_col_ci(df_raw, "city"), "")
        mail_state_series = df_raw.get(_get_col_ci(df_raw, "state"), "")
        mail_zip_series = df_raw.get(_get_col_ci(df_raw, "zip","postal_code","zipcode","zip_code"), "")
        mail_cityline = _join_cityline_series(mail_city_series, mail_state_series, mail_zip_series)
    else:
        a1 = df_raw.get(_get_col_ci(df_raw, "address1","mail_address1","mail street","street","addr1","address"), "")
        a2 = df_raw.get(_get_col_ci(df_raw, "address2","mail_address2","unit","addr2","line2"), "")
        mail_street_series = _join_street_series(a1, a2)
        mail_city_series = df_raw.get(_get_col_ci(df_raw, "city","mail_city"), "")
        mail_state_series = df_raw.get(_get_col_ci(df_raw, "state","mail_state","st"), "")
        mail_zip_series = df_raw.get(_get_col_ci(df_raw, "zip","postal_code","zipcode","zip_code","mail_zip"), "")
        mail_cityline = _join_cityline_series(mail_city_series, mail_state_series, mail_zip_series)

    # CRM side
    crm_a1 = df_raw.get(_get_col_ci(df_raw, "crm_address1","address1","crm street","crm_addr1"), "")
    crm_a2 = df_raw.get(_get_col_ci(df_raw, "crm_address2","address2","crm unit","crm_addr2","unit"), "")
    crm_street_series = _join_street_series(crm_a1, crm_a2)

    crm_city = df_raw.get(_get_col_ci(df_raw, "crm_city","city"), "")
    crm_state = df_raw.get(_get_col_ci(df_raw, "crm_state","state","st"), "")
    crm_zip = df_raw.get(_get_col_ci(df_raw, "crm_zip","zip","postal_code","zipcode","zip_code"), "")
    crm_cityline_series = _join_cityline_series(crm_city, crm_state, crm_zip)

    # Notes
    notes_col = _get_col_ci(df_raw, "match_notes","notes")
    notes_series = df_raw[notes_col] if notes_col else pd.Series([""]*len(df_raw))

    # Mail count (for KPI avg mailers before engagement)
    mc_col = _get_col_ci(df_raw, "mail_count_in_window", "mail_count", "mailers_before")
    mail_count_series = (
        pd.to_numeric(df_raw[mc_col], errors="coerce").fillna(0).astype(int)
        if mc_col else pd.Series([0]*len(df_raw))
    )

    # Build normalized table for rendering
    out = pd.DataFrame({
        "Mail Dates": mail_dates_series,
        "CRM Date": crm_date_raw.fillna(""),
        "Amount": amt_disp.fillna(""),
        "Mail Address": mail_street_series.fillna(""),
        "Mail City/State/Zip": mail_cityline.fillna(""),
        "CRM Address": crm_street_series.fillna(""),
        "CRM City/State/Zip": crm_cityline_series.fillna(""),
        "Confidence": conf_vals.astype(int),
        "Notes": notes_series.fillna(""),
    })

    # Aux for KPIs / charts
    out.__dict__["__aux_crm_ts"] = crm_ts
    out.__dict__["__aux_amount_float"] = amt_float
    out.__dict__["__aux_crm_city"] = crm_city
    out.__dict__["__aux_crm_state"] = crm_state
    out.__dict__["__aux_crm_zip"] = crm_zip
    out.__dict__["__aux_mail_count"] = mail_count_series

    return out

# ---------- public: renderer ----------
def render_full_dashboard_v17(summary_df: pd.DataFrame, mail_total_count: int) -> str:
    df = summary_df.copy()

    # KPIs
    total_mail = int(mail_total_count or 0)
    total_matches = int(len(df))
    total_revenue = float(df.__dict__.get("__aux_amount_float", pd.Series([0.0]*len(df))).sum())
    mail_counts = df.__dict__.get("__aux_mail_count", pd.Series([0]*len(df)))
    avg_mailers_before = float(mail_counts.mean()) if len(mail_counts) else 0.0
    mailers_per_acq = (total_mail / total_matches) if total_matches else 0.0

    # Top cities / zips (CRM side) — top 5
    crm_city = df.__dict__.get("__aux_crm_city", pd.Series([], dtype="object")).fillna("")
    crm_state = df.__dict__.get("__aux_crm_state", pd.Series([], dtype="object")).fillna("")
    crm_zip = df.__dict__.get("__aux_crm_zip", pd.Series([], dtype="object")).fillna("")

    cityline = (crm_city.astype(str).str.strip() + ", " + crm_state.astype(str).str.strip()).str.strip(", ")
    top_cities = cityline[cityline != ""].value_counts().head(5)

    top_zips = crm_zip.astype(str).str.strip()
    top_zips = top_zips[(top_zips != "") & (top_zips != "nan") & (top_zips != "None")].value_counts().head(5)

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

    # Sort table by most recent CRM date
    order_ts = pd.Series(ts).fillna(pd.Timestamp(0))
    df_sorted = df.loc[order_ts.sort_values(ascending=False).index]

    def conf_class(v: int) -> str:
        if v >= 94: return "conf-high"
        if v >= 88: return "conf-mid"
        return "conf-low"

    rows_html = []
    for _, r in df_sorted.iterrows():
        rows_html.append(f"""
          <tr>
            <td class="mono">{_escape(r.get("Mail Dates"))}</td>
            <td class="mono">{_escape(r.get("CRM Date"))}</td>
            <td class="mono">{_escape(r.get("Amount"))}</td>
            <td>{_escape(r.get("Mail Address"))}</td>
            <td>{_escape(r.get("Mail City/State/Zip"))}</td>
            <td>{_escape(r.get("CRM Address"))}</td>
            <td>{_escape(r.get("CRM City/State/Zip"))}</td>
            <td class="conf {conf_class(_safe_int(r.get("Confidence"), 0))}">{_safe_int(r.get("Confidence"), 0)}%</td>
            <td>{_escape(r.get("Notes"))}</td>
          </tr>
        """)

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

    # KPI block (FIXED f-string issue)
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

    # Top lists
    def li_list(series_counts) -> str:
        if series_counts is None or len(series_counts) == 0:
            return "<div class='empty'>No data</div>"
        items = []
        for label, cnt in series_counts.items():
            items.append(f'<div class="li"><span class="name">{_escape(str(label))}</span><span class="cnt">{int(cnt)}</span></div>')
        return "\n".join(items)

    cities_html = li_list(top_cities)
    zips_html = li_list(top_zips)

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

    # Horizontal chart
    chart_html = _horizontal_bar_chart(month_counts)
    chart_section = f"""
      <div class="card chart">
        <div class="k" style="margin-bottom:8px;">Matched Jobs by Month</div>
        {chart_html}
      </div>
    """

    # Summary table
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
              {''.join(rows_html)}
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
    if month_counts is None or len(month_counts) == 0:
        return "<div class='empty'>No monthly data</div>"
    max_v = max(int(v) for v in month_counts.values) or 1
    rows = []
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
