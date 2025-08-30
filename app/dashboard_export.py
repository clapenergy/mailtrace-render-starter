# app/dashboard_export.py
from __future__ import annotations
import html
import re
from typing import Tuple
import pandas as pd

# ---------- helpers: safe html ----------
def esc(s) -> str:
    if s is None:
        return ""
    return html.escape(str(s))

# ---------- unit parsing/formatting ----------
_UNIT_PAT = re.compile(
    r"(?:^|[\s,.-])(?:(apt|apartment|suite|ste|unit|bldg|fl|floor)\s*#?\s*([\w-]+)|#\s*([\w-]+))\s*$",
    re.IGNORECASE,
)

def _split_unit_from_line(addr1: str) -> Tuple[str, str]:
    """
    If addr1 ends with a unit indicator, split it off.
    Returns (street_part, unit_part) where unit_part like 'Apt 2' or 'Ste 200'.
    If no unit present, returns (addr1, '').
    """
    s = (addr1 or "").strip()
    if not s:
        return "", ""
    m = _UNIT_PAT.search(s)
    if not m:
        return s, ""
    kind = (m.group(1) or "").strip()
    u1 = (m.group(2) or "").strip()
    u2 = (m.group(3) or "").strip()
    if kind:
        unit = f"{kind.title()} {u1}"
    else:
        unit = f"#{u2}"
    street = s[: m.start()].rstrip(" ,.-")
    # normalize common labels
    unit_norm = (
        unit.replace("Apartment", "Apt")
            .replace("Aptartment", "Apt")
            .replace("Suite", "Suite")
            .replace("Ste", "Ste")
            .replace("Unit", "Unit")
            .replace("Floor", "Fl")
            .replace("Bldg", "Bldg")
    )
    return street, unit_norm

def _format_addr_with_unit(addr1: str, addr2: str) -> str:
    """
    '123 Main St, Apt 2' if there's a unit; otherwise just the street.
    If addr2 exists, we treat that as the unit. Otherwise we try to peel a unit off addr1 tail.
    """
    a1 = (addr1 or "").strip()
    a2 = (addr2 or "").strip()
    if a2:
        street, _inline_unit = _split_unit_from_line(a1)
        unit = a2
    else:
        street, unit = _split_unit_from_line(a1)
        if not unit:
            return a1
    return f"{street}, {unit}".strip(", ").strip()

def _format_city_state_zip(city: str, state: str, zipc: str) -> str:
    city = (city or "").strip().rstrip(".")
    state = (state or "").strip().upper()
    zipc = (zipc or "").strip()
    if city and state:
        return f"{city}, {state} {zipc}".strip()
    if city:
        return f"{city} {zipc}".strip()
    if state:
        return f"{state} {zipc}".strip()
    return zipc

# ---------- public: finalize for export ----------
def finalize_summary_for_export_v17(df: pd.DataFrame, **_ignore) -> pd.DataFrame:
    """
    Normalize/prepare the summary dataframe for export and display.
    Expected incoming columns include:
      - crm_job_date, crm_amount, crm_address1, crm_address2, crm_city, crm_state, crm_zip
      - address1, address2, city, state, zip
      - confidence, match_notes
    Extra kwargs are ignored so callers can pass more without breaking.
    """
    d = df.copy()

    # Ensure presence of columns
    for col in [
        "crm_job_date","crm_amount",
        "crm_address1","crm_address2","crm_city","crm_state","crm_zip",
        "address1","address2","city","state","zip",
        "confidence","match_notes"
    ]:
        if col not in d.columns:
            d[col] = ""

    # Parse CRM date into sortable key; keep original text for display
    def _to_sortable_date(s):
        if not isinstance(s, str):
            return pd.NaT
        z = s.strip()
        if not z:
            return pd.NaT
        for fmt in ("%Y-%m-%d","%m/%d/%Y","%Y/%m/%d","%m-%d-%Y","%d-%m-%y","%d-%m-%Y"):
            try:
                return pd.to_datetime(z, format=fmt, errors="raise")
            except Exception:
                continue
        # fuzzy fallback
        return pd.to_datetime(z, errors="coerce")

    d["_crm_dt"] = d["crm_job_date"].map(_to_sortable_date)

    # Build display strings with unit logic (mail + CRM sides)
    d["_mail_addr_disp"] = d.apply(
        lambda r: _format_addr_with_unit(r.get("address1",""), r.get("address2","")),
        axis=1
    )
    d["_mail_city_disp"] = d.apply(
        lambda r: _format_city_state_zip(r.get("city",""), r.get("state",""), r.get("zip","")),
        axis=1
    )
    d["_crm_addr_disp"] = d.apply(
        lambda r: _format_addr_with_unit(r.get("crm_address1",""), r.get("crm_address2","")),
        axis=1
    )
    d["_crm_city_disp"] = d.apply(
        lambda r: _format_city_state_zip(r.get("crm_city",""), r.get("crm_state",""), r.get("crm_zip","")),
        axis=1
    )

    # Normalized fields for robust groupings
    d["_crm_city_norm"]  = d["crm_city"].astype(str).str.strip().str.rstrip(".")
    d["_crm_state_norm"] = d["crm_state"].astype(str).str.strip().str.upper()

    # Sort newest CRM date first
    d = d.sort_values(by=["_crm_dt"], ascending=[False], na_position="last").reset_index(drop=True)
    return d

# ---------- inline SVG chart ----------
def _svg_month_barchart(monthly_df: pd.DataFrame) -> str:
    """
    Render a clean, self-contained horizontal bar chart using inline SVG.
    monthly_df has columns ["month" (Timestamp month start), "matches" (int)] sorted chronologically.
    """
    if monthly_df.empty:
        return '<div class="note">No dated matches</div>'

    # Chart geometry
    width = 700
    left_pad = 100
    right_pad = 40
    bar_height = 16
    v_gap = 10

    n = len(monthly_df)
    height = n * (bar_height + v_gap) + 20

    maxv = int(monthly_df["matches"].max()) if n else 0
    if maxv <= 0:
        maxv = 1

    # build bars
    y = 10
    rows = []
    for _, r in monthly_df.iterrows():
        label = r["month"].strftime("%Y-%m")
        v = int(r["matches"])
        # bar width scaled to drawable width
        drawable = width - left_pad - right_pad
        w = int((v / maxv) * drawable)
        # Row group
        rows.append(f"""
          <g>
            <text x="{left_pad-10}" y="{y+bar_height-4}" text-anchor="end" class="m-label">{esc(label)}</text>
            <rect x="{left_pad}" y="{y}" width="{w}" height="{bar_height}" rx="4" ry="4" class="m-bar" />
            <text x="{left_pad + w + 6}" y="{y+bar_height-4}" class="m-value">{v}</text>
          </g>
        """)
        y += bar_height + v_gap

    svg = f"""
    <svg viewBox="0 0 {width} {height}" width="100%" height="{height}">
      <style>
        .m-label {{ fill:#64748b; font: 12px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }}
        .m-value {{ fill:#0c2d4e; font: 12px/1.2 system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; font-weight: 800; }}
        .m-bar {{ fill:#b7cadc; }}
        @media (prefers-color-scheme: dark) {{
          .m-label {{ fill:#94a3b8; }}
          .m-value {{ fill:#cbd5e1; }}
          .m-bar {{ fill:#375a7c; }}
        }}
      </style>
      {''.join(rows)}
    </svg>
    """
    return svg

# ---------- public: render html dashboard ----------
def render_full_dashboard_v17(
    summary_df: pd.DataFrame,
    mail_total_count: int | None = None,
    **_ignore,
) -> str:
    d = summary_df.copy()

    # KPIs
    total_mail = mail_total_count if mail_total_count is not None else ""
    total_matches = len(d)

    def _to_float(x):
        if x is None: return 0.0
        s = str(x).strip().replace("$","").replace(",","")
        try:
            return float(s)
        except Exception:
            return 0.0
    total_revenue = d["crm_amount"].map(_to_float).sum()

    # ---- Top cities/zips (robust) ----
    city_counts = (
        d.groupby(["_crm_city_norm","_crm_state_norm"], dropna=False)
          .size().reset_index(name="matches")
          .rename(columns={"_crm_city_norm":"city","_crm_state_norm":"state"})
          .sort_values(["matches","city","state"], ascending=[False, True, True])
          .head(50)  # keep up to 50; we'll show 5 with scrollbar
    )
    zip_counts = (
        d.groupby(["crm_zip"], dropna=False)
          .size().reset_index(name="matches")
          .rename(columns={"crm_zip":"zip"})
          .sort_values(["matches","zip"], ascending=[False, True])
          .head(50)
    )

    # ---- Monthly matches (chronological) → SVG chart ----
    dm = d.dropna(subset=["_crm_dt"]).copy()
    if not dm.empty:
        dm["month"] = dm["_crm_dt"].dt.to_period("M").dt.to_timestamp()
        monthly = (
            dm.groupby("month").size().reset_index(name="matches")
              .sort_values("month")
        )
    else:
        monthly = pd.DataFrame(columns=["month","matches"])

    month_svg = _svg_month_barchart(monthly)

    # ----- Layout CSS (inline) -----
    styles = """
    <style>
      .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:16px; }
      .analytics-grid {
        display: grid;
        grid-template-columns: 2fr 1fr;
        grid-template-areas: "chart side";
        gap:16px;
      }
      .analytics-chart { grid-area: chart; display:flex; flex-direction:column; gap:10px; }
      .analytics-side { grid-area: side; display:grid; grid-template-rows: auto auto; gap:16px; }
      @media (max-width: 900px) {
        .analytics-grid {
          grid-template-columns: 1fr;
          grid-template-areas: "chart" "side";
        }
      }
      .kvlist { list-style:none; margin:0; padding:0; }
      .kvlist li {
        display:flex; align-items:center; justify-content:space-between;
        padding:8px 0; border-bottom:1px solid #f1f5f9; font-weight:600;
      }
      .scrollbox { max-height:220px; overflow-y:auto; padding-right:6px; }
    </style>
    """

    # KPI HTML
    kpi_html = f"""
    <div class="kpi-grid">
      <div class="card kpi"><div class="k">Total mail records</div><div class="v">{esc(total_mail)}</div></div>
      <div class="card kpi"><div class="k">Matches</div><div class="v">{total_matches}</div></div>
      <div class="card kpi"><div class="k">Total revenue</div><div class="v">${total_revenue:,.2f}</div></div>
    </div>
    """

    # Top cities/zips (scrollable, ~5 visible)
    def _city_list(df):
        items = []
        for _, r in df.iterrows():
            city = (r["city"] or "")
            state = r["state"] or ""
            items.append(f"<li><span>{esc(city)}, {esc(state)}</span><strong>{int(r['matches'])}</strong></li>")
        if not items:
            items = ["<li><span>—</span><strong>0</strong></li>"]
        return "<ul class='kvlist'>" + "".join(items) + "</ul>"

    def _zip_list(df):
        items = []
        for _, r in df.iterrows():
            z = str(r["zip"]) if r["zip"] is not None else ""
            items.append(f"<li><span>{esc(z)}</span><strong>{int(r['matches'])}</strong></li>")
        if not items:
            items = ["<li><span>—</span><strong>0</strong></li>"]
        return "<ul class='kvlist'>" + "".join(items) + "</ul>"

    analytics_html = f"""
    <div class="analytics-grid">
      <div class="card analytics-chart">
        <div class="k">Matched jobs by month</div>
        <div style="margin-top:8px">{month_svg}</div>
      </div>
      <div class="analytics-side">
        <div class="card">
          <div class="k">Top Cities</div>
          <div class="scrollbox">{_city_list(city_counts)}</div>
        </div>
        <div class="card">
          <div class="k">Top ZIPs</div>
          <div class="scrollbox">{_zip_list(zip_counts)}</div>
        </div>
      </div>
    </div>
    """

    # Summary table
    head = """
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
        <tbody>
    """
    rows_html = []
    for _, r in d.head(200).iterrows():
        rows_html.append(f"""
          <tr>
            <td>{esc(r.get('crm_job_date',''))}</td>
            <td>{esc(r.get('crm_amount',''))}</td>
            <td>{esc(r.get('_mail_addr_disp','')) or esc(r.get('address1',''))}</td>
            <td>{esc(r.get('_mail_city_disp',''))}</td>
            <td>{esc(r.get('_crm_addr_disp','')) or esc(r.get('crm_address1',''))}</td>
            <td>{esc(r.get('_crm_city_disp',''))}</td>
            <td>{esc(f"{int(r.get('confidence',0))}%")}</td>
            <td>{esc(r.get('match_notes',''))}</td>
          </tr>
        """)

    table_html = head + "\n".join(rows_html) + "\n</tbody></table>"

    # Final page
    html_out = (
        styles
        + kpi_html
        + analytics_html
        + """
        <div style="margin-top:18px;" class="note">Sample of Matches<br/>Sorted by most recent CRM date (falls back to mail date).</div>
        <div style="margin-top:8px;">""" + table_html + "</div>"
    )
    return html_out
