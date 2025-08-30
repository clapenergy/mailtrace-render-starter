# app/dashboard_export.py
from __future__ import annotations
import html
import io
import math
import re
from datetime import datetime
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
        unit = f"{kind.title()} {u1}".replace("Ste", "Ste").replace("Suite", "Suite")
    else:
        unit = f"#{u2}"
    street = s[: m.start()].rstrip(" ,.-")
    # Normalize casing for common unit kinds
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
    Build '123 Main St, Apt 2' (if we have a unit).
    - If addr2 provided, use that as unit.
    - Else try to peel unit off addr1 tail.
    """
    a1 = (addr1 or "").strip()
    a2 = (addr2 or "").strip()
    if a2:
        street, _inline_unit = _split_unit_from_line(a1)
        unit = a2
    else:
        street, unit = _split_unit_from_line(a1)
        if not unit:
            return a1  # nothing to append
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
def finalize_summary_for_export_v17(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize/prepare the summary dataframe for export and display.
    Expected incoming columns include:
      - crm_job_date, crm_amount, crm_address1, crm_address2, crm_city, crm_state, crm_zip
      - address1, address2, city, state, zip
      - confidence, match_notes
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

    # Parse date to sortable key; keep original text for display
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

    # Build display strings with unit logic
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

    # Sort newest CRM date first, then fallback to mail city/zip alpha if tie
    d = d.sort_values(by=["_crm_dt"], ascending=[False], na_position="last").reset_index(drop=True)

    return d

# ---------- public: render html dashboard ----------
def render_full_dashboard_v17(summary_df: pd.DataFrame, mail_total_count: int | None = None) -> str:
    d = summary_df.copy()

    # KPIs
    total_mail = mail_total_count if mail_total_count is not None else ""
    total_matches = len(d)
    # Total revenue (numeric sum of crm_amount where parsable)
    def _to_float(x):
        if x is None: return 0.0
        s = str(x).strip().replace("$","").replace(",","")
        try:
            return float(s)
        except Exception:
            return 0.0
    total_revenue = d["crm_amount"].map(_to_float).sum()

    # Top cities/zips (from CRM side for consistency)
    city_counts = (
        d.groupby(["crm_city","crm_state"], dropna=False)
          .size().reset_index(name="matches")
          .sort_values("matches", ascending=False)
          .head(6)
    )
    zip_counts = (
        d.groupby(["crm_zip"], dropna=False)
          .size().reset_index(name="matches")
          .sort_values("matches", ascending=False)
          .head(6)
    )

    # Monthly matches (by CRM job month)
    dm = d.dropna(subset=["_crm_dt"]).copy()
    dm["month"] = dm["_crm_dt"].dt.to_period("M").astype(str)
    monthly = (
        dm.groupby("month").size().reset_index(name="matches")
          .sort_values("month")
    )

    # Build KPI HTML
    kpi_html = f"""
    <div class="grid">
      <div class="card kpi"><div class="k">Total mail records</div><div class="v">{esc(total_mail)}</div></div>
      <div class="card kpi"><div class="k">Matches</div><div class="v">{total_matches}</div></div>
      <div class="card kpi"><div class="k">Total revenue</div><div class="v">${total_revenue:,.2f}</div></div>
    </div>
    """

    # Top cities/zips HTML
    def _bullets_city(df):
        out = []
        for _, r in df.iterrows():
            city = (r["crm_city"] or "").rstrip(".")
            state = r["crm_state"] or ""
            out.append(f"{esc(city)}, {esc(state)}: {int(r['matches'])}")
        return " &nbsp; ".join(out) if out else "—"

    def _bullets_zip(df):
        out = [f"{esc(str(r['crm_zip']))}: {int(r['matches'])}" for _, r in df.iterrows()]
        return " &nbsp; ".join(out) if out else "—"

    top_html = f"""
    <div class="grid">
      <div class="card">
        <div class="k">Top Cities</div>
        <div class="v" style="font-size:16px; font-weight:600">{_bullets_city(city_counts)}</div>
      </div>
      <div class="card">
        <div class="k">Top ZIPs</div>
        <div class="v" style="font-size:16px; font-weight:600">{_bullets_zip(zip_counts)}</div>
      </div>
    </div>
    """

    # Monthly simple bar (inline HTML/CSS bars)
    bars = []
    maxv = int(monthly["matches"].max()) if not monthly.empty else 0
    for _, r in monthly.iterrows():
        m = esc(r["month"])
        v = int(r["matches"])
        w = int( (v / maxv) * 100 ) if maxv > 0 else 0
        bars.append(f"""
          <div style="display:flex; align-items:center; gap:10px;">
            <div style="width:90px; color:#64748b; font-weight:700">{m}</div>
            <div style="flex:1; background:#eef2f7; border-radius:999px; overflow:hidden;">
              <div style="width:{w}%; height:10px;"></div>
            </div>
            <div style="width:50px; text-align:right; font-weight:800">{v}</div>
          </div>
        """)
    month_html = f"""
      <div class="card">
        <div class="k">Matched jobs by month</div>
        <div style="display:flex; flex-direction:column; gap:8px; margin-top:10px;">
          {''.join(bars) if bars else '<div class="note">No dated matches</div>'}
        </div>
      </div>
    """

    # Summary table (hide bucket; confidence next to notes already handled upstream)
    # We render: CRM Date | Amount | Mail Address | Mail City/State/Zip | CRM Address | CRM City/State/Zip | Confidence | Notes
    # Use the precomputed display columns for addresses with unit.
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

    # Put it all together
    html_out = f"""
    <div class="grid">
      {kpi_html}
      {top_html}
      {month_html}
    </div>
    <div style="margin-top:18px;" class="note">Sample of Matches<br/>Sorted by most recent CRM date (falls back to mail date).</div>
    <div style="margin-top:8px;">{table_html}</div>
    """
    return html_out
