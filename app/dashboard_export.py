# app/dashboard_export.py
# Dashboard-v4.3 — adds a leftmost "Mail Dates" column (from mail_dates_in_window).
# Keeps prior features: address rescue, KPIs, Top 5 lists with scroll, horizontal month chart,
# and color-coded confidence.

from __future__ import annotations
import io, base64, html, re
from typing import Optional, Tuple, List
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def _esc(x) -> str:
    return "" if x is None else html.escape(str(x))

def _money_to_float(s: str) -> float:
    try:
        return float(str(s).replace("$", "").replace(",", "").strip() or 0.0)
    except Exception:
        return 0.0

# ---------------- Address parsing helpers ----------------

_UNIT_PAT = re.compile(
    r"(?:^|[\s,.-])(?:(apt|apartment|suite|ste|unit|bldg|fl|floor)\s*#?\s*([\w-]+)|#\s*([\w-]+))\s*$",
    re.IGNORECASE,
)

_STREET_WORDS = {
    "street","st","st.","avenue","ave","ave.","av","av.","boulevard","blvd","blvd.","road","rd","rd.",
    "lane","ln","ln.","drive","dr","dr.","court","ct","ct.","circle","cir","cir.","parkway","pkwy","pkwy.",
    "place","pl","pl.","terrace","ter","ter.","trail","trl","trl.","highway","hwy","hwy.","way","wy","wy.",
    "pkway","common","cmn","cmn.","pkwy"
}

_CITY_ST_ZIP_TAIL = re.compile(
    r"\s*(?:,?\s*[A-Za-z .'\-]+)?\s*,?\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?\s*$"
)

def _strip_trailing_city_state_zip(addr1: str) -> str:
    s = (addr1 or "").strip()
    if not s:
        return s
    if _CITY_ST_ZIP_TAIL.search(s):
        s = _CITY_ST_ZIP_TAIL.sub("", s).rstrip(" ,.-")
    return s

def _looks_like_house_number_only(s: str) -> bool:
    return bool(re.fullmatch(r"\d{1,8}", (s or "").strip()))

def _split_unit_from_line(addr1: str) -> Tuple[str, str]:
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

def _city_has_streety_bits(city: str) -> bool:
    if not isinstance(city, str): return False
    t = re.findall(r"[A-Za-z0-9#']+", city.lower())
    has_num = any(tok.isdigit() for tok in t)
    has_street_word = any(tok in _STREET_WORDS for tok in t)
    return has_street_word or has_num

def _extract_street_from_city_loose(city: str, true_city_guess: str) -> Tuple[str, str]:
    s = (city or "").strip()
    if not s:
        return "", ""
    if true_city_guess and true_city_guess in s and true_city_guess != s:
        head = s[: s.rfind(true_city_guess)].rstrip(" ,.-")
        tail = true_city_guess
        if head and tail:
            return head, tail
    toks = re.findall(r"[A-Za-z0-9']+", s)
    if len(toks) >= 2:
        last = toks[-1]
        if last.isalpha() and last[0].isupper() and last.lower() not in _STREET_WORDS:
            head = " ".join(toks[:-1]).strip()
            tail = last
            if head and tail:
                return head, tail
        if len(toks) >= 2 and all(x.isalpha() and x[0].isupper() for x in toks[-2:]):
            head = " ".join(toks[:-2]).strip()
            tail = " ".join(toks[-2:])
            if head and tail:
                return head, tail
    if "," in s:
        head, tail = s.rsplit(",", 1)
        return head.strip(" ,.-"), tail.strip()
    return "", s

def _format_addr_with_unit(addr1: str, addr2: str, mail_city: str = "", mail_state: str = "", mail_zip: str = "") -> str:
    a1_raw = (addr1 or "").strip()
    a2 = (addr2 or "").strip()
    a1 = _strip_trailing_city_state_zip(a1_raw)

    if _looks_like_house_number_only(a1) and _city_has_streety_bits(mail_city):
        street_from_city, _ = _extract_street_from_city_loose(mail_city, mail_city)
        if street_from_city:
            a1 = (a1 + " " + street_from_city).strip()

    if a2:
        street, _ = _split_unit_from_line(a1)
        unit = a2
    else:
        street, unit = _split_unit_from_line(a1)
        if not unit:
            return street
    return f"{street}, {unit}".strip(", ").strip()

def _clean_city_for_display(city: str) -> str:
    s = (city or "").strip()
    if not s:
        return s
    _, city_clean = _extract_street_from_city_loose(s, s)
    if city_clean and city_clean != s:
        return city_clean
    return s

def _format_city_state_zip(city: str, state: str, zipc: str) -> str:
    city = _clean_city_for_display(city)
    state = (state or "").strip().upper()
    zipc = (zipc or "").strip()
    if city and state:
        return f"{city}, {state} {zipc}".strip()
    if city:
        return f"{city} {zipc}".strip()
    if state:
        return f"{state} {zipc}".strip()
    return zipc

# ---------------- Public API: finalize ----------------
def finalize_summary_for_export_v17(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    rename_map = {
        "crmaddress1": "crm_address1", "crm_address1_original": "crm_address1",
        "crmaddress2": "crm_address2",
        "crmcity": "crm_city", "crmstate": "crm_state", "crmzip": "crm_zip",
        "crmdate": "crm_job_date", "job_date": "crm_job_date",
        "amount": "crm_amount", "value": "crm_amount",
        "confidence_percent": "confidence",
        # allow passthrough of prior-constructed dates list if named slightly differently
        "mail_dates": "mail_dates_in_window",
    }
    for k, v in rename_map.items():
        if k in d.columns and v not in d.columns:
            d.rename(columns={k: v}, inplace=True)

    # Ensure expected columns exist
    for col in [
        "crm_job_date","crm_amount",
        "address1","address2","city","state","zip",
        "crm_address1","crm_address2","crm_city","crm_state","crm_zip",
        "confidence","match_notes","mail_count_in_window","mail_date",
        "mail_dates_in_window"
    ]:
        if col not in d.columns:
            d[col] = ""

    try:
        d["confidence"] = pd.to_numeric(d["confidence"], errors="coerce").fillna(0).clip(0,100).astype(int)
    except Exception:
        d["confidence"] = 0

    if "crm_amount" in d.columns:
        def _fmt_money(x):
            v = _money_to_float(x)
            return f"${v:,.2f}" if v else ""
        d["crm_amount"] = d["crm_amount"].map(_fmt_money)

    def _fix_notes(x):
        if not isinstance(x, str) or not x:
            return x
        return x.replace("NaN", "none").replace("nan", "none")
    d["match_notes"] = d["match_notes"].map(_fix_notes)

    # Normalize the mail dates list to a compact, safe string
    def _fmt_mail_dates(s):
        if s is None: return ""
        txt = str(s).strip()
        if not txt: return ""
        # collapse spaces after commas
        txt = re.sub(r"\s*,\s*", ", ", txt)
        return txt
    d["mail_dates_in_window"] = d["mail_dates_in_window"].map(_fmt_mail_dates)

    return d

# ---------------- Confidence color formatting ----------------
def _conf_span(v) -> str:
    """Return a <span> with class for colored confidence."""
    try:
        val = int(v)
    except Exception:
        return _esc(v)
    if val >= 94:
        klass = "conf-high"
    elif val >= 88:
        klass = "conf-mid"
    else:
        klass = "conf-low"
    return f'<span class="{klass}">{val}%</span>'

# ---------------- Public API: render ----------------
def render_full_dashboard_v17(summary_df: pd.DataFrame,
                              mail_total_count: Optional[int] = None,
                              **_ignore) -> str:
    d = summary_df.copy()

    # KPIs
    total_mail = int(mail_total_count) if mail_total_count not in (None, "") else ""
    total_matches = len(d)
    total_revenue = d["crm_amount"].map(_money_to_float).sum()

    avg_mailers_prior = None
    if "mail_count_in_window" in d.columns:
        s = pd.to_numeric(d["mail_count_in_window"], errors="coerce")
        if s.notna().any():
            avg_mailers_prior = float(s.mean())

    mailers_per_acq = None
    if total_mail and total_matches:
        mailers_per_acq = total_mail / total_matches

    # Top lists
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

    # Monthly chart (vertical bars, months on X)
    mdates = pd.to_datetime(d.get("crm_job_date", ""), errors="coerce")
    monthly_counts = (pd.DataFrame({"month": mdates})
                        .dropna()
                        .assign(month=lambda x: x["month"].dt.to_period("M"))
                        .groupby("month")
                        .size().reset_index(name="matches"))
    monthly_counts["month_str"] = monthly_counts["month"].astype(str)
    monthly_counts = monthly_counts.sort_values("month")

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

    # CSS (adds conf-high/mid/low classes)
    css = """
    <style>
      :root { --brand:#0c2d4e; --accent:#759d40; --on-brand:#fff; --text:#0f172a; --muted:#64748b; --border:#e5e7eb; }
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
      table { width:100%; border-collapse: collapse; background:#fff; border:1px solid var(--border); border-radius:12px; overflow:hidden; margin-top:16px; table-layout: fixed; }
      th, td { text-align:left; padding:12px 14px; border-bottom:1px solid #f3f4f6; font-size:14px; vertical-align: top; word-wrap: break-word; }
      th { background:#f8fafc; }
      h2 { margin: 20px 0 8px; }
      .conf-high { color:#15803d; font-weight:800; }  /* green */
      .conf-mid  { color:#b45309; font-weight:800; }  /* orange */
      .conf-low  { color:#b91c1c; font-weight:800; }  /* red */
      /* Give mail-dates a reasonable width */
      th.col-maildates, td.col-maildates { width: 180px; }
    </style>
    """

    # KPIs
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

    # Lists + Chart
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

    # Sort by most recent CRM date; fallback to mail date
    d["_crm_dt"] = pd.to_datetime(d.get("crm_job_date",""), errors="coerce")
    d["_mail_dt"] = pd.to_datetime(d.get("mail_date",""), errors="coerce")
    d["_sort_dt"] = d["_crm_dt"].fillna(d["_mail_dt"])
    d = d.sort_values("_sort_dt", ascending=False)

    # Table (add Mail Dates far-left)
    head = """
      <table>
        <thead>
          <tr>
            <th class="col-maildates">Mail Dates</th>
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
    rows_html: List[str] = []
    for _, r in d.head(200).iterrows():
        mail_addr = _format_addr_with_unit(
            r.get("address1",""), r.get("address2",""),
            r.get("city",""), r.get("state",""), r.get("zip","")
        )
        crm_addr  = _format_addr_with_unit(
            r.get("crm_address1",""), r.get("crm_address2",""),
            r.get("crm_city",""), r.get("crm_state",""), r.get("crm_zip","")
        )
        conf_html = _conf_span(r.get("confidence", ""))
        mail_dates = r.get("mail_dates_in_window", "")
        rows_html.append(f"""
          <tr>
            <td class="col-maildates">{_esc(mail_dates)}</td>
            <td>{_esc(r.get('crm_job_date',''))}</td>
            <td>{_esc(r.get('crm_amount',''))}</td>
            <td>{_esc(mail_addr)}</td>
            <td>{_esc(_format_city_state_zip(r.get('city',''), r.get('state',''), r.get('zip','')))}</td>
            <td>{_esc(crm_addr)}</td>
            <td>{_esc(_format_city_state_zip(r.get('crm_city',''), r.get('crm_state',''), r.get('crm_zip','')))}</td>
            <td>{conf_html}</td>
            <td>{_esc(r.get('match_notes',''))}</td>
          </tr>
        """)
    table_html = head + "\n".join(rows_html) + "\n</tbody></table>"

    return css + kpis_html + lists_html + "<h2>Sample of Matches</h2>" + table_html
