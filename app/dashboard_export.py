# app/dashboard_export.py
# Full dashboard renderer for MailTrace
# - KPIs: Total mail, Matches, Total revenue (+ optional Avg mailers before job, Mailers per acquisition)
# - Top 5 Cities & Top 5 ZIPs (scrollable)
# - "Matched Jobs by Month" chart: vertical bars, months on X-axis
# - Sample table: CRM Date | Amount | Mail Address | Mail City/State/Zip | CRM Address | CRM City/State/Zip | Confidence | Notes
# - No bucket breakdown; confidence shown as a % and notes concise
# - Robust to mixed/missing values

from __future__ import annotations
import io, base64, html, re
from typing import Optional
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # headless environments (Render)
import matplotlib.pyplot as plt


# -----------------------
# Helpers / formatting
# -----------------------
def _esc(x) -> str:
    return "" if x is None else html.escape(str(x))

def _money_to_float(s: str) -> float:
    try:
        return float(str(s).replace("$", "").replace(",", "").strip() or 0.0)
    except Exception:
        return 0.0

# Extract inline unit suffixes like "... Apt 2", "... #4", "... Ste 3"
_UNIT_PAT = re.compile(
    r"(?:^|[\s,.-])(?:(apt|apartment|suite|ste|unit|bldg|fl|floor)\s*#?\s*([\w-]+)|#\s*([\w-]+))\s*$",
    re.IGNORECASE,
)

def _split_unit_from_line(addr1: str):
    s = (addr1 or "").strip()
    if not s:
        return "", ""
    m = _UNIT_PAT.search(s)
    if not m:
        return s, ""
    kind = (m.group(1) or "").strip()
    u1 = (m.group(2) or "").strip()
    u2 = (m.group(3) or "").strip()
    unit = f"{kind.title()} {u1}" if kind else f"#{u2}"
    street = s[: m.start()].rstrip(" ,.-")
    return street, unit

def _format_addr_with_unit(addr1: str, addr2: str) -> str:
    """
    Display rule: show unit as a suffix `, UNIT` if either addr2 is present
    or we detect an inline unit in addr1. Keeps the original street text.
    """
    a1 = (addr1 or "").strip()
    a2 = (addr2 or "").strip()
    if a2:  # explicit unit column wins
        street, _ = _split_unit_from_line(a1)
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


# -----------------------
# Public API: finalizer
# -----------------------
def finalize_summary_for_export_v17(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize/clean columns so the dashboard & export are consistent.
    Expected downstream columns include:
      crm_job_date, crm_amount,
      address1,address2,city,state,zip,
      crm_address1,crm_address2,crm_city,crm_state,crm_zip,
      confidence, match_notes
    """
    d = df.copy()

    # Common alternative column names -> normalize
    rename_map = {
        "crmaddress1": "crm_address1", "crm_address1_original": "crm_address1",
        "crmaddress2": "crm_address2",
        "crmcity": "crm_city", "crmstate": "crm_state", "crmzip": "crm_zip",
        "crmdate": "crm_job_date", "job_date": "crm_job_date",
        "amount": "crm_amount", "value": "crm_amount",
        "confidence_percent": "confidence",
    }
    for k, v in rename_map.items():
        if k in d.columns and v not in d.columns:
            d.rename(columns={k: v}, inplace=True)

    # Ensure columns exist
    for col in [
        "crm_job_date","crm_amount",
        "address1","address2","city","state","zip",
        "crm_address1","crm_address2","crm_city","crm_state","crm_zip",
        "confidence","match_notes"
    ]:
        if col not in d.columns:
            d[col] = ""

    # Confidence as 0..100 int
    try:
        d["confidence"] = pd.to_numeric(d["confidence"], errors="coerce").fillna(0).clip(0, 100).astype(int)
    except Exception:
        d["confidence"] = 0

    # Amount: store as pretty $X,XXX.XX string for display
    if "crm_amount" in d.columns:
        def _fmt_money(x):
            v = _money_to_float(x)
            return f"${v:,.2f}" if v else ""
        d["crm_amount"] = d["crm_amount"].map(_fmt_money)

    # Clean notes "nan/NaN" → "none"
    def _fix_notes(x):
        if not isinstance(x, str) or not x:
            return x
        return x.replace("NaN", "none").replace("nan", "none")
    d["match_notes"] = d["match_notes"].map(_fix_notes)

    return d


# -----------------------
# Public API: dashboard HTML
# -----------------------
def render_full_dashboard_v17(summary_df: pd.DataFrame,
                              mail_total_count: Optional[int] = None,
                              **_ignore) -> str:
    """
    Return a full self-contained HTML fragment (no external assets).
    """
    d = summary_df.copy()

    # ---------- KPI numbers ----------
    total_mail = int(mail_total_count) if mail_total_count not in (None, "") else ""
    total_matches = len(d)
    total_revenue = d["crm_amount"].map(_money_to_float).sum()

    # Optional KPI: Avg mailers before job (if provided by upstream)
    avg_mailers_prior = None
    for candidate in ("mail_count_in_window", "mailers_prior", "mail_count_prior"):
        if candidate in d.columns:
            s = pd.to_numeric(d[candidate], errors="coerce")
            if s.notna().any():
                avg_mailers_prior = float(s.mean())
                break

    # Optional KPI: Mailers per acquisition (requires total_mail)
    mailers_per_acq = None
    if total_mail and total_matches:
        mailers_per_acq = total_mail / total_matches

    # ---------- Top lists ----------
    safe_city = d.get("crm_city", "").fillna("")
    safe_state = d.get("crm_state", "").fillna("")
    safe_zip = d.get("crm_zip", "").fillna("")

    city_counts = (pd.DataFrame({"crm_city": safe_city, "crm_state": safe_state})
                    .assign(_one=1)
                    .groupby(["crm_city","crm_state"], dropna=False)["_one"].sum()
                    .reset_index(name="matches")
                    .sort_values("matches", ascending=False).head(5))

    zip_counts = (pd.DataFrame({"crm_zip": safe_zip})
                    .assign(_one=1)
                    .groupby("crm_zip", dropna=False)["_one"].sum()
                    .reset_index(name="matches")
                    .sort_values("matches", ascending=False).head(5))

    # ---------- Monthly counts (vertical bars; months on X) ----------
    mdates = pd.to_datetime(d.get("crm_job_date", ""), errors="coerce")
    monthly_counts = (pd.DataFrame({"month": mdates})
                        .dropna()
                        .assign(month=lambda x: x["month"].dt.to_period("M"))
                        .groupby("month")
                        .size().reset_index(name="matches"))
    monthly_counts["month_str"] = monthly_counts["month"].astype(str)
    monthly_counts = monthly_counts.sort_values("month")  # chronological

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(monthly_counts["month_str"], monthly_counts["matches"])
    ax.set_xlabel("Month")
    ax.set_ylabel("Matches")
    ax.set_title("Matched Jobs by Month")
    for label in ax.get_xticklabels():
        label.set_rotation(45)
        label.set_horizontalalignment("right")
    fig.tight_layout()
    bio = io.BytesIO()
    plt.savefig(bio, format="png", dpi=150)
    plt.close(fig)
    month_png_b64 = base64.b64encode(bio.getvalue()).decode("ascii")
    month_img_tag = f'<img alt="Matched jobs by month" src="data:image/png;base64,{month_png_b64}" style="width:100%;max-width:1000px;">'

    # ---------- CSS ----------
    css = """
    <style>
      :root {
        --brand: #0c2d4e;
        --accent: #759d40;
        --on-brand: #ffffff;
        --text: #0f172a;
        --muted: #64748b;
        --border: #e5e7eb;
      }
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; color: var(--text); }
      .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(220px,1fr)); gap:16px; }
      .card { background:#fff; border:1px solid var(--border); border-radius:16px; padding:16px; box-shadow:0 2px 8px rgba(0,0,0,.04); }
      .k { color: var(--muted); font-size: 13px; }
      .v { font-size: 28px; font-weight: 900; }
      .lists { display:grid; grid-template-columns: 1fr 1fr; gap:16px; margin-top:16px; }
      @media (max-width: 900px) { .lists { grid-template-columns: 1fr; } }
      .kvlist { list-style:none; margin:0; padding:0; }
      .kvlist li { display:flex; align-items:center; justify-content:space-between; padding:8px 0; border-bottom:1px solid #f1f5f9; font-weight:600; }
      .scroll { max-height:220px; overflow-y:auto; padding-right:6px; }
      table { width:100%; border-collapse: collapse; background:#fff; border:1px solid var(--border); border-radius:12px; overflow:hidden; margin-top:16px; }
      th, td { text-align:left; padding:12px 14px; border-bottom:1px solid #f3f4f6; font-size:14px; }
      th { background:#f8fafc; }
      h2 { margin: 20px 0 8px; }
    </style>
    """

    # ---------- KPIs ----------
    kpi_items = [
        f'<div class="card"><div class="k">Total mail records</div><div class="v">{_esc(total_mail)}</div></div>',
        f'<div class="card"><div class="k">Matches</div><div class="v">{total_matches}</div></div>',
        f'<div class="card"><div class="k">Total revenue</div><div class="v">${total_revenue:,.2f}</div></div>',
    ]
    if avg_mailers_prior is not None:
        kpi_items.append(f'<div class="card"><div class="k">Avg. mailers before job</div><div class="v">{avg_mailers_prior:.2f}</div></div>')
    if mailers_per_acq is not None:
        kpi_items.append(f'<div class="card"><div class="k">Mailers per acquisition</div><div class="v">{mailers_per_acq:.2f}</div></div>')
    kpis_html = f'<div class="grid">{"".join(kpi_items)}</div>'

    # ---------- Top lists + Chart ----------
    def _city_ul(df):
        items = []
        for _, r in df.iterrows():
            items.append(f"<li><span>{_esc(r['crm_city'])}, {_esc(r['crm_state'])}</span><strong>{int(r['matches'])}</strong></li>")
        return "<ul class='kvlist'>" + "".join(items) + "</ul>"

    def _zip_ul(df):
        items = []
        for _, r in df.iterrows():
            items.append(f"<li><span>{_esc(str(r['crm_zip']))}</span><strong>{int(r['matches'])}</strong></li>")
        return "<ul class='kvlist'>" + "".join(items) + "</ul>"

    lists_html = f"""
      <div class="lists">
        <div class="card">
          <div class="k">Top Cities</div>
          <div class="scroll">{_city_ul(city_counts)}</div>
        </div>
        <div class="card">
          <div class="k">Top ZIPs</div>
          <div class="scroll">{_zip_ul(zip_counts)}</div>
        </div>
      </div>
      <div class="card" style="margin-top:16px;">
        <div class="k">Matched jobs by month</div>
        <div style="margin-top:8px">{month_img_tag}</div>
      </div>
    """

    # ---------- Sample table ----------
    d["_crm_dt"] = pd.to_datetime(d["crm_job_date"], errors="coerce")
    d["_mail_dt"] = pd.to_datetime(d.get("mail_date", ""), errors="coerce")
    d["_sort_dt"] = d["_crm_dt"].fillna(d["_mail_dt"])
    d = d.sort_values("_sort_dt", ascending=False)

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
        mail_addr = _format_addr_with_unit(r.get("address1",""), r.get("address2",""))
        crm_addr  = _format_addr_with_unit(r.get("crm_address1",""), r.get("crm_address2",""))
        rows_html.append(f"""
          <tr>
            <td>{_esc(r.get('crm_job_date',''))}</td>
            <td>{_esc(r.get('crm_amount',''))}</td>
            <td>{_esc(mail_addr)}</td>
            <td>{_esc(_format_city_state_zip(r.get('city',''), r.get('state',''), r.get('zip','')))}</td>
            <td>{_esc(crm_addr)}</td>
            <td>{_esc(_format_city_state_zip(r.get('crm_city',''), r.get('crm_state',''), r.get('crm_zip','')))}</td>
            <td>{_esc(f"{int(r.get('confidence',0))}%")}</td>
            <td>{_esc(r.get('match_notes',''))}</td>
          </tr>
        """)
    table_html = head + "\n".join(rows_html) + "\n</tbody></table>"

    # Return full HTML fragment
    return css + kpis_html + lists_html + "<h2>Sample of Matches</h2>" + table_html
