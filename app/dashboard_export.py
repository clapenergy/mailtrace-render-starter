# app/dashboard_export.py
# Full drop-in: provides finalize_summary_for_export_v17() and render_full_dashboard_v17()
# - Defensive against varying column names from matcher
# - Builds KPIs, Top Cities/ZIPs, and a horizontal timeline bar chart (dates on X-axis)
# - Renders the "Sample of Matches" table with Mail Dates (left-most), Amount, etc.
# - Confidence color chips

from __future__ import annotations
import io
import base64
from datetime import datetime, date
from typing import List, Tuple, Optional
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt

# ---------- helpers ----------

_DATE_PARSE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y",
    "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%y", "%d-%m-%y"
]

def _parse_any_date(s: str) -> Optional[date]:
    if not isinstance(s, str) or not s.strip():
        return None
    z = s.strip()
    # allow “11/2024”, “2024-11”, “01-2024” (month granularity)
    # try full formats first
    for fmt in _DATE_PARSE_FORMATS:
        try:
            return datetime.strptime(z.replace("/", "-"), fmt).date()
        except Exception:
            pass
    # month-resolution: MM-YYYY or YYYY-MM
    try:
        if "-" in z:
            parts = z.split("-")
            if len(parts) == 2:
                # try MM-YYYY
                m, y = parts
                if len(m) <= 2 and len(y) == 4 and m.isdigit() and y.isdigit():
                    return date(int(y), int(m), 1)
                # try YYYY-MM
                y, m = parts
                if len(y) == 4 and len(m) <= 2 and y.isdigit() and m.isdigit():
                    return date(int(y), int(m), 1)
    except Exception:
        pass
    return None

def _safe_str(x) -> str:
    return "" if (x is None or (isinstance(x, float) and pd.isna(x))) else str(x)

def _first_present(d: pd.Series, options: List[str], default: str = "") -> str:
    for o in options:
        if o in d.index:
            v = d[o]
            if isinstance(v, str):
                return v
    return default

def _find_amount_column(df: pd.DataFrame) -> str:
    candidates = ["amount", "job_value", "value", "job amount", "revenue"]
    cols = [c for c in df.columns]
    low = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in low:
            return low[cand]
    # also try something that looks like money
    for c in cols:
        lc = c.lower()
        if "amount" in lc or "value" in lc or "revenue" in lc or "job" in lc:
            return c
    return ""

def _fmt_money(x) -> str:
    try:
        if isinstance(x, str):
            s = x.replace("$", "").replace(",", "").strip()
            if s == "": 
                return ""
            val = float(s)
        else:
            val = float(x)
        return f"${val:,.2f}"
    except Exception:
        return _safe_str(x)

def _join_mail_city_state_zip(row: pd.Series) -> str:
    # Try both mail and crm naming just in case
    city = _safe_str(row.get("city", row.get("mail_city", "")))
    state = _safe_str(row.get("state", row.get("mail_state", "")))
    z = _safe_str(row.get("zip", row.get("mail_zip", "")))
    if not (city or state or z):
        return ""
    if city and state:
        city_state = f"{city}, {state}"
    elif city:
        city_state = city
    else:
        city_state = state
    return f"{city_state} {z}".strip()

def _join_crm_city_state_zip(row: pd.Series) -> str:
    city = _safe_str(row.get("crm_city", ""))
    state = _safe_str(row.get("crm_state", ""))
    z = _safe_str(row.get("crm_zip", ""))
    if city and state:
        city_state = f"{city}, {state}"
    elif city:
        city_state = city
    else:
        city_state = state
    return f"{city_state} {z}".strip()

def _make_mail_full_address(row: pd.Series) -> str:
    a1 = _safe_str(row.get("matched_mail_address1", row.get("matched_mail_full_address", row.get("address1", ""))))
    a2 = _safe_str(row.get("matched_mail_address2", row.get("address2", "")))
    if a2:
        a = f"{a1}, {a2}"
    else:
        a = a1
    city_state_zip = _join_mail_city_state_zip(row)
    if city_state_zip:
        return f"{a}, {city_state_zip}".replace(" ,", ",")
    return a

def _confidence_color_class(score: int) -> str:
    if score >= 94:
        return "chip hi"
    if score >= 88:
        return "chip mid"
    return "chip lo"

def _month_key(d: Optional[date]) -> Optional[str]:
    if d is None:
        return None
    return d.strftime("%Y-%m")

def _month_label(d: Optional[date]) -> Optional[str]:
    if d is None:
        return None
    # Example: Jan 2024
    return d.strftime("%b %Y")

# ---------- public API ----------

def finalize_summary_for_export_v17(summary: pd.DataFrame) -> pd.DataFrame:
    """
    Take the matcher output and:
      - normalize column names
      - add parsed dates for grouping/sorting
      - produce display columns used by the dashboard
    """
    if summary is None or summary.empty:
        return pd.DataFrame(columns=[
            "mail_dates", "crm_job_date", "amount",
            "mail_address_display", "mail_city_state_zip",
            "crm_address_display", "crm_city_state_zip",
            "confidence_percent", "match_notes",
            "crm_city", "crm_state", "crm_zip"
        ])

    df = summary.copy()

    # Canonical columns we expect from your matcher
    # (be defensive: users may rename; earlier runs used these names)
    rename_map = {}
    if "matched_mail_full_address" not in df.columns and "matched_mail_address1" in df.columns:
        # assemble later, but keep note
        pass

    # Amount column (or job value)
    amt_col = _find_amount_column(df)
    if amt_col:
        df["__amount_raw"] = df[amt_col]
    else:
        df["__amount_raw"] = ""

    # Mail dates list column
    # Known names: "mail_dates_in_window" (your matcher), else try "mail_dates", "mail history"
    mail_dates_col = None
    for c in df.columns:
        if c.lower() in ("mail_dates_in_window", "mail_dates", "mail history"):
            mail_dates_col = c
            break
    if mail_dates_col is None:
        # If not present, just set empty
        df["mail_dates_in_window"] = ""
        mail_dates_col = "mail_dates_in_window"

    # CRM date column
    crm_date_col = None
    for c in df.columns:
        if c.lower() in ("crm_job_date", "job_date", "date", "created_at"):
            crm_date_col = c
            break
    if crm_date_col is None:
        df["crm_job_date"] = ""
        crm_date_col = "crm_job_date"

    # Build display columns
    # Mail address (with optional unit)
    if "mail_address_display" not in df.columns:
        # try matched_mail_full_address from matcher; else build with parts
        if "matched_mail_full_address" in df.columns:
            df["mail_address_display"] = df["matched_mail_full_address"].fillna("")
        else:
            a1 = df.get("address1", "").fillna("")
            a2 = df.get("address2", "").fillna("")
            df["mail_address_display"] = (a1.astype(str) + (", " + a2.astype(str)).where(a2.astype(str) != "", "")).str.replace(" ,", ",", regex=False)

    # Mail city/state/zip
    df["mail_city_state_zip"] = df.apply(_join_mail_city_state_zip, axis=1)

    # CRM address (street only + unit if exists)
    crm_a1 = df.get("crm_address1_original", df.get("crm_address1", df.get("address1", ""))).fillna("")
    crm_a2 = df.get("crm_address2_original", df.get("crm_address2", df.get("address2", ""))).fillna("")
    df["crm_address_display"] = (crm_a1.astype(str) + (", " + crm_a2.astype(str)).where(crm_a2.astype(str) != "", "")).str.replace(" ,", ",", regex=False)

    # CRM city/state/zip
    df["crm_city_state_zip"] = df.apply(_join_crm_city_state_zip, axis=1)

    # Confidence
    conf_col = None
    for c in df.columns:
        if c.lower() in ("confidence_percent", "confidence", "score", "confidence_score"):
            conf_col = c
            break
    if conf_col is None:
        df["confidence_percent"] = 0
        conf_col = "confidence_percent"

    # Notes
    notes_col = None
    for c in df.columns:
        if c.lower() in ("match_notes", "notes", "explanation"):
            notes_col = c
            break
    if notes_col is None:
        df["match_notes"] = ""
        notes_col = "match_notes"

    # Amount formatted
    df["amount_display"] = df["__amount_raw"].map(_fmt_money)

    # Normalize mail dates list (string) and also compute first/last mail date if needed
    def _normalize_mail_dates_cell(cell) -> str:
        if cell is None or (isinstance(cell, float) and pd.isna(cell)):
            return ""
        s = str(cell).strip()
        if not s:
            return ""
        # Normalize separators
        s = s.replace("; ", ", ").replace(" | ", ", ").replace("|", ", ")
        # Collapse double spaces
        return ", ".join([p.strip() for p in s.split(",") if p.strip()])

    df["mail_dates_display"] = df[mail_dates_col].map(_normalize_mail_dates_cell)

    # Parse CRM date to a real date (for sorting & monthly chart)
    df["__crm_date_obj"] = df[crm_date_col].map(_parse_any_date)
    df["__crm_month_key"] = df["__crm_date_obj"].map(_month_key)
    df["__crm_month_label"] = df["__crm_date_obj"].map(_month_label)

    # Pull-through city/state/zip for aggregations
    for col in ("crm_city", "crm_state", "crm_zip"):
        if col not in df.columns:
            df[col] = ""

    # Final projected columns for the summary table
    out = pd.DataFrame({
        "mail_dates": df["mail_dates_display"],
        "crm_job_date": df[crm_date_col],
        "amount": df["amount_display"],
        "mail_address_display": df["mail_address_display"],
        "mail_city_state_zip": df["mail_city_state_zip"],
        "crm_address_display": df["crm_address_display"],
        "crm_city_state_zip": df["crm_city_state_zip"],
        "confidence_percent": df[conf_col].astype(int, errors="ignore"),
        "match_notes": df[notes_col].fillna(""),
        # extras used for aggregates and colors
        "crm_city": df["crm_city"].fillna(""),
        "crm_state": df["crm_state"].fillna(""),
        "crm_zip": df["crm_zip"].fillna(""),
        "__crm_month_key": df["__crm_month_key"],
        "__crm_month_label": df["__crm_month_label"],
    })

    # Sort by most recent CRM date (fall back to raw string sort if missing)
    out["__crm_date_obj"] = df["__crm_date_obj"]
    out = out.sort_values(by="__crm_date_obj", ascending=False, na_position="last").reset_index(drop=True)
    return out


def render_full_dashboard_v17(summary_v17: pd.DataFrame, mail_count_total: int) -> str:
    """
    Build the full HTML for the dashboard.
    - KPI row
    - Top Cities / Top ZIPs (top 5, scrollable)
    - Matched Jobs by Month (horizontal layout; dates along X axis)
    - Sample table (first ~200 rows for speed)
    """
    # ----- aggregates -----
    matches = len(summary_v17)
    # revenue
    # re-parse money strings to float
    def _money_to_float(s: str) -> float:
        try:
            if not isinstance(s, str):
                return float(s)
            z = s.replace("$", "").replace(",", "").strip()
            return float(z) if z else 0.0
        except Exception:
            return 0.0
    revenue_total = float(sum(summary_v17.get("amount", "").map(_money_to_float))) if "amount" in summary_v17.columns else 0.0

    # avg mailers before engagement: count dates in mail_dates column
    def _count_dates_cell(s: str) -> int:
        if not isinstance(s, str) or not s.strip():
            return 0
        return len([p.strip() for p in s.split(",") if p.strip()])
    total_mailers_before = int(summary_v17.get("mail_dates", "").map(_count_dates_cell).sum() if "mail_dates" in summary_v17.columns else 0)
    avg_mailers_before = (total_mailers_before / matches) if matches else 0.0

    mailers_per_acq = (mail_count_total / matches) if matches else 0.0

    # top cities & zips (use CRM side)
    top_cities = (
        summary_v17.groupby(["crm_city", "crm_state"], dropna=False)
        .size()
        .reset_index(name="n")
        .sort_values("n", ascending=False)
    )
    top_zips = (
        summary_v17.groupby(["crm_zip"], dropna=False)
        .size()
        .reset_index(name="n")
        .sort_values("n", ascending=False)
    )

    # monthly chart data
    # Use ordered by month key; drop NA
    monthly = (
        summary_v17[summary_v17["__crm_month_key"].notna()]
        .groupby(["__crm_month_key", "__crm_month_label"], dropna=False)
        .size()
        .reset_index(name="n")
        .sort_values("__crm_month_key")  # chronological
    )

    # ----- chart PNG (dates along X-axis) -----
    chart_b64 = ""
    if not monthly.empty:
        fig = plt.figure(figsize=(12, 3.6))
        ax = fig.add_subplot(111)
        ax.bar(monthly["__crm_month_label"], monthly["n"])
        ax.set_ylabel("Matches")
        ax.set_xlabel("Month")
        ax.set_title("Matched Jobs by Month")
        plt.xticks(rotation=35, ha="right")
        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        chart_b64 = base64.b64encode(buf.read()).decode("ascii")

    # limit lists in UI to top 5 (scrollable box)
    def _render_city_items(df: pd.DataFrame) -> str:
        if df.empty:
            return '<div class="muted">No data</div>'
        items = []
        for _, r in df.head(5).iterrows():
            city = _safe_str(r.get("crm_city"))
            state = _safe_str(r.get("crm_state"))
            n = int(r.get("n", 0))
            label = f"{city}, {state}".strip(", ")
            items.append(f'<div class="li"><span class="name">{label}</span><span class="count">{n}</span></div>')
        return "\n".join(items)

    def _render_zip_items(df: pd.DataFrame) -> str:
        if df.empty:
            return '<div class="muted">No data</div>'
        items = []
        for _, r in df.head(5).iterrows():
            z = _safe_str(r.get("crm_zip"))
            n = int(r.get("n", 0))
            label = z if z else "(blank)"
            items.append(f'<div class="li"><span class="name">{label}</span><span class="count">{n}</span></div>')
        return "\n".join(items)

    # ----- table rows (show up to 200 for speed) -----
    def _conf_chip(score: int) -> str:
        try:
            s = int(score)
        except Exception:
            s = 0
        cls = _confidence_color_class(s)
        return f'<span class="{cls}">{s}%</span>'

    rows_html = []
    view = summary_v17.head(200) if len(summary_v17) > 200 else summary_v17
    for _, r in view.iterrows():
        mail_dates = _safe_str(r.get("mail_dates", ""))
        crm_date = _safe_str(r.get("crm_job_date", ""))
        amount = _safe_str(r.get("amount", ""))
        mail_addr = _safe_str(r.get("mail_address_display", ""))
        mail_csz = _safe_str(r.get("mail_city_state_zip", ""))
        crm_addr = _safe_str(r.get("crm_address_display", ""))
        crm_csz = _safe_str(r.get("crm_city_state_zip", ""))
        conf = r.get("confidence_percent", 0)
        notes = _safe_str(r.get("match_notes", ""))

        rows_html.append(f"""
        <tr>
            <td class="mono">{mail_dates or ""}</td>
            <td>{crm_date}</td>
            <td class="mono">{amount}</td>
            <td>{mail_addr}</td>
            <td class="muted">{mail_csz}</td>
            <td>{crm_addr}</td>
            <td class="muted">{crm_csz}</td>
            <td>{_conf_chip(conf)}</td>
            <td>{notes}</td>
        </tr>
        """)

    rows_section = "\n".join(rows_html) if rows_html else """
        <tr><td colspan="9" class="muted" style="text-align:center;padding:16px;">No matches to display.</td></tr>
    """

    # ----- HTML / CSS -----
    cities_section = _render_city_items(top_cities) if not top_cities.empty else '<div class="muted">No data</div>'
    zips_section = _render_zip_items(top_zips) if not top_zips.empty else '<div class="muted">No data</div>'
    chart_section = f'<img alt="Matched by Month" src="data:image/png;base64,{chart_b64}" />' if chart_b64 else '<div class="muted">No monthly data</div>'

    html = f"""
<div class="container">
  <div class="kpis">
    <div class="kpi">
      <div class="k">Total mail records</div>
      <div class="v">{mail_count_total:,}</div>
    </div>
    <div class="kpi">
      <div class="k">Matches</div>
      <div class="v">{matches:,}</div>
    </div>
    <div class="kpi">
      <div class="k">Total revenue generated</div>
      <div class="v">{_fmt_money(revenue_total)}</div>
    </div>
    <div class="kpi">
      <div class="k">Avg mailers before engagement</div>
      <div class="v">{avg_mailers_before:.2f}</div>
    </div>
    <div class="kpi">
      <div class="k">Mailers per acquisition</div>
      <div class="v">{mailers_per_acq:.2f}</div>
    </div>
  </div>

  <div class="row">
    <div class="card list">
      <div class="h">Top Cities (matches)</div>
      <div class="scroll">
        {cities_section}
      </div>
    </div>

    <div class="card list">
      <div class="h">Top ZIP Codes (matches)</div>
      <div class="scroll">
        {zips_section}
      </div>
    </div>

    <div class="card chart">
      <div class="h">Matched Jobs by Month</div>
      <div class="chartwrap">
        {chart_section}
      </div>
    </div>
  </div>

  <div class="card">
    <div class="h">Sample of Matches</div>
    <div class="muted small">Sorted by most recent CRM date (falls back to mail date). Showing up to 200 rows.</div>
    <div class="tablewrap">
      <table>
        <thead>
          <tr>
            <th>Mail Dates</th>
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
        <tbody>
          {rows_section}
        </tbody>
      </table>
    </div>
  </div>
</div>

<style>
:root {{
  --brand: #0c2d4e;
  --accent: #759d40;
  --text: #0f172a;
  --muted: #64748b;
  --border: #e5e7eb;
  --card: #ffffff;
  --chip-hi: #dcfce7;
  --chip-mid: #fef9c3;
  --chip-lo: #fee2e2;
}}
.container {{ max-width: 1200px; margin: 0 auto; padding: 24px; color: var(--text); }}
.kpis {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(180px,1fr)); gap:12px; margin: 8px 0 16px; }}
.kpi {{ background:var(--card); border:1px solid var(--border); border-radius:14px; padding:14px; }}
.kpi .k {{ font-size:12px; color:var(--muted); font-weight:700; }}
.kpi .v {{ font-size:24px; font-weight:900; }}
.row {{ display:grid; grid-template-columns: 280px 280px 1fr; gap:12px; align-items:start; }}
.card {{ background:var(--card); border:1px solid var(--border); border-radius:14px; padding:14px; }}
.card .h {{ font-weight:800; margin-bottom:8px; }}
.card.list .scroll {{ max-height:220px; overflow:auto; border:1px dashed var(--border); border-radius:10px; padding:8px; }}
.li {{ display:flex; justify-content:space-between; align-items:center; padding:6px 8px; border-bottom:1px solid #f1f5f9; }}
.li:last-child {{ border-bottom:none; }}
.li .name {{ font-weight:700; }}
.li .count {{ font-variant-numeric: tabular-nums; color:var(--muted); }}
.card.chart .chartwrap {{ width:100%; overflow:auto; }}
.tablewrap {{ overflow:auto; }}
table {{ width:100%; border-collapse: collapse; }}
th, td {{ text-align:left; padding:10px 12px; border-bottom:1px solid #f1f5f9; vertical-align:top; }}
th {{ background:#f8fafc; font-size:13px; }}
.small {{ font-size:12px; }}
.muted {{ color:var(--muted); }}
.mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }}
.chip {{ display:inline-block; padding:4px 8px; border-radius:999px; font-weight:800; font-size:12px; }}
.chip.hi {{ background: var(--chip-hi); }}
.chip.mid {{ background: var(--chip-mid); }}
.chip.lo {{ background: var(--chip-lo); }}
@media (max-width: 900px) {{
  .row {{ grid-template-columns: 1fr; }}
}}
</style>
"""
    return html
