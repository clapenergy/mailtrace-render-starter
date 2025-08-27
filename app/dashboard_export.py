from __future__ import annotations
from datetime import datetime
import base64, io
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def _parse_dd_mm_yyyy(s: str):
    try:
        return datetime.strptime(s.strip(), "%d-%m-%Y").date()
    except Exception:
        return None

def to_dd_mm_yy_from_dd_mm_yyyy(s: str) -> str:
    if not isinstance(s, str) or s.strip()=="" or s.lower() in ("none","none provided"):
        return "None provided"
    d = _parse_dd_mm_yyyy(s)
    return d.strftime("%d-%m-%y") if d else s

def list_dates_to_short(s: str) -> str:
    if not isinstance(s, str) or s.strip()=="" or s.lower() in ("none","none provided"):
        return "None provided"
    parts = [p.strip() for p in s.split(",")]
    out = []; seen=set()
    for p in parts:
        d = _parse_dd_mm_yyyy(p)
        out.append(d.strftime("%d-%m-%y") if d else p)
    res=[]
    for x in out:
        if x not in seen:
            seen.add(x); res.append(x)
    return ", ".join(res) if res else "None provided"

def month_key_iso_from_dd_mm_yyyy(s: str) -> str | None:
    d = _parse_dd_mm_yyyy(s) if isinstance(s,str) else None
    if d is None: return None
    return f"{d.year}-{d.month:02d}"

def iso_to_mmyy(month_iso: str) -> str:
    try:
        y, m = month_iso.split("-")
        return datetime(int(y), int(m), 1).strftime("%m-%y")
    except Exception:
        return month_iso

def clean_addr2_blank(x):
    if not isinstance(x, str): return ""
    s = x.strip()
    if s.lower() in ("nan","none","none provided"): return ""
    return s

def finalize_summary_for_export_v17(summary: pd.DataFrame) -> pd.DataFrame:
    df = summary.copy()
    if "crm_address2_original" in df.columns:
        df["crm_address2_original"] = df["crm_address2_original"].apply(clean_addr2_blank)
    # crm_job_date and mail_dates_in_window are already short in our pipeline
    return df

def _kpi_card(title, value, sub=""):
    return f"""<div class="kpi"><div class="kpi-title">{title}</div><div class="kpi-value">{value}</div><div class="kpi-sub">{sub}</div></div>"""

def _conf_color(c):
    try: c = int(c)
    except: return "#999"
    if c >= 95: return "#0a0"
    if c >= 88: return "#c90"
    return "#c00"

def render_full_dashboard_v17(summary_short: pd.DataFrame, mail_count_total: int) -> str:
    total_matches = len(summary_short)
    mail_per_acq = (mail_count_total / total_matches) if total_matches else 0.0
    mail_counts = pd.to_numeric(summary_short.get("mail_count_in_window", pd.Series(dtype=str)), errors="coerce").fillna(0).astype(int)
    avg_mail_per_match = float(mail_counts.mean()) if len(mail_counts) else 0.0
    median_mail_per_match = float(mail_counts.median()) if len(mail_counts) else 0.0
    conf = pd.to_numeric(summary_short.get("confidence_percent", pd.Series(dtype=str)), errors="coerce").fillna(0).astype(int)
    p95 = int((conf >= 95).sum()); p88_94 = int(((conf >= 88) & (conf <= 94)).sum()); p_lt88 = int((conf < 88).sum())

    kpi_html = "<div class='kpi-row'>" +         _kpi_card("Total mail pieces", f"{mail_count_total:,}") +         _kpi_card("Total matches", f"{total_matches:,}") +         _kpi_card("Mail per acquisition", f"{mail_per_acq:.2f}", "Total mail / matches") +         _kpi_card("Avg mailers per match", f"{avg_mail_per_match:.2f}", f"Median {median_mail_per_match:.0f}") +         _kpi_card("Confidence ≥95%", f"{p95:,}", f"{(p95/total_matches*100 if total_matches else 0):.1f}%") +         _kpi_card("Confidence 88–94%", f"{p88_94:,}", f"{(p88_94/total_matches*100 if total_matches else 0):.1f}%") +         _kpi_card("Confidence <88%", f"{p_lt88:,}", f"{(p_lt88/total_matches*100 if total_matches else 0):.1f}%") +         "</div>"

    # Top cities/zips
    city_counts = summary_short.groupby(["crm_city","crm_state"], dropna=False).size().reset_index(name="matches")
    city_counts["crm_city"] = city_counts["crm_city"].fillna("")
    city_counts["crm_state"] = city_counts["crm_state"].fillna("")
    top5_cities = city_counts.sort_values("matches", ascending=False).head(5)
    city_rows = "".join(f"<tr><td>{r.crm_city}</td><td>{r.crm_state}</td><td style='text-align:right'>{r.matches:,}</td></tr>" for _, r in top5_cities.iterrows())

    zips = summary_short.get("crm_zip", pd.Series([], dtype=str)).fillna("")
    zip_counts = zips.replace({"": "Unknown"}).value_counts().reset_index()
    zip_counts.columns = ["zip_code","matches"]
    top5_zips = zip_counts.head(5)
    zip_rows = "".join(f"<tr><td>{r.zip_code}</td><td style='text-align:right'>{r.matches:,}</td></tr>" for _, r in top5_zips.iterrows())

    # Matches by month (mm-yy labels) derived from crm_job_date (dd-mm-yy)
    def to_iso(s):
        try:
            d = datetime.strptime(s, "%d-%m-%y").date()
            return d.strftime("%Y-%m")
        except Exception:
            return None
    months_iso = summary_short["crm_job_date"].apply(to_iso) if "crm_job_date" in summary_short.columns else pd.Series(dtype=object)
    by_month = months_iso.value_counts(dropna=True).sort_index()
    labels = [datetime.strptime(k, "%Y-%m").strftime("%m-%y") for k in by_month.index] if len(by_month) else []
    values = by_month.values if len(by_month) else []

    buf = io.BytesIO()
    fig, ax = plt.subplots()
    if len(values):
        ax.bar(range(len(values)), values)
        ax.set_xticks(range(len(values)))
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.set_xlabel("CRM Month (mm-yy)"); ax.set_ylabel("Matches")
    ax.set_title("Matches by Month")
    fig.tight_layout()
    fig.savefig(buf, format="png"); plt.close(fig)
    chart_data_uri = f"data:image/png;base64,{base64.b64encode(buf.getvalue()).decode('ascii')}"

    # Conditional growth
    growth_html = ""
    months_present = sorted([m for m in months_iso.dropna().unique()])
    if len(months_present) >= 4:
        tmp = summary_short.copy()
        tmp["month_iso"] = months_iso
        grp = tmp.groupby(["crm_zip","month_iso"]).size().reset_index(name="count")
        if not grp.empty:
            latest_month = grp["month_iso"].max()
            def prev_months(m, k):
                y, mo = map(int, m.split("-"))
                out = []
                for i in range(1, k+1):
                    yy, mm = y, mo - i
                    while mm <= 0:
                        yy -= 1; mm += 12
                    out.append(f"{yy}-{mm:02d}")
                return out
            baseline_months = prev_months(latest_month, 3)
            curr = grp[grp["month_iso"] == latest_month].groupby("crm_zip")["count"].sum().rename("last_month").reset_index()
            base = grp[grp["month_iso"].isin(baseline_months)].groupby("crm_zip")["count"].mean().rename("baseline_avg").reset_index()
            import numpy as np
            growth = pd.merge(curr, base, on="crm_zip", how="left")
            growth["baseline_avg"] = growth["baseline_avg"].fillna(0.0)
            growth["growth_pct"] = np.where(growth["baseline_avg"] > 0, (growth["last_month"] - growth["baseline_avg"]) / growth["baseline_avg"] * 100.0, np.nan)
            MIN_VOL = 10
            growth["eligible"] = growth["last_month"] >= MIN_VOL
            eligible = growth[growth["eligible"] & growth["growth_pct"].notna()]
            if not eligible.empty:
                eligible = eligible.sort_values(["growth_pct","last_month"], ascending=[False, False]).head(10)
                def f2(x): 
                    try: 
                        return f"{float(x):.2f}"
                    except: 
                        return ""
                rows = []
                for _, r in eligible.iterrows():
                    rows.append(f"<tr><td>{r.crm_zip}</td><td style='text-align:right'>{int(r.last_month)}</td><td style='text-align:right'>{f2(r.baseline_avg)}</td><td style='text-align:right'>{r.growth_pct:.1f}%</td></tr>")
                subhead = f"(Last month: {datetime.strptime(latest_month, '%Y-%m').strftime('%m-%y')}; Baseline: avg of {', '.join(datetime.strptime(m, '%Y-%m').strftime('%m-%y') for m in baseline_months)}) — min {MIN_VOL} matches"
                growth_html = f"""
<h2>Top Growth ZIP Codes</h2>
<div class="sub">{subhead}</div>
<table class="mini">
<thead><tr><th>ZIP</th><th>Last Month</th><th>Baseline Avg (3 mo)</th><th>Growth %</th></tr></thead>
<tbody>
{''.join(rows)}
</tbody>
</table>
"""

    # Table
    columns = list(summary_short.columns)
    th = "<thead><tr>" + "".join(f"<th>{c}</th>" for c in columns) + "</tr></thead>"
    def color(c):
        try: c=int(c)
        except: return "#999"
        if c>=95: return "#0a0"
        if c>=88: return "#c90"
        return "#c00"
    trs = []
    for _, r in summary_short.iterrows():
        tds=[]
        for ccol in columns:
            val = r.get(ccol, "")
            if ccol == "confidence_percent":
                tds.append(f'<td style="font-weight:600;color:{color(val)}">{val}%</td>')
            else:
                if ccol in ("mail_dates_in_window","crm_job_date") and (not isinstance(val,str) or val.strip()==""):
                    val = "None provided"
                tds.append(f"<td>{val}</td>")
        trs.append("<tr>" + "".join(tds) + "</tr>")
    tbody = "<tbody>" + "\n".join(trs) + "</tbody>"
    table_html = "<table>" + th + tbody + "</table>"

    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>Mailtrace Dashboard - v17</title>
<style>
body{{font-family:Arial;margin:24px}}
.kpi-row{{display:flex;flex-wrap:wrap;gap:16px;margin-bottom:18px}}
.kpi{{flex:1 1 220px;border:1px solid #eee;border-radius:8px;padding:12px;background:#fafafa}}
.kpi-title{{font-size:12px;color:#666;text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px}}
.kpi-value{{font-size:22px;font-weight:700}}
.kpi-sub{{font-size:12px;color:#888;margin-top:2px}}
.grid3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:24px;margin:12px 0 24px}}
.mini{{border-collapse:collapse;width:100%}}
.mini th,.mini td{{border:1px solid #eee;padding:6px;font-size:13px;text-align:left}}
.wrap{{max-height:70vh;overflow:auto;border:1px solid #eee}}
table{{border-collapse:collapse;width:100%}}
th,td{{border:1px solid #eee;padding:6px;text-align:left;font-size:13px}}
thead th{{position:sticky;top:0;background:#fff}}
tr:nth-child(even){{background:#fcfcfc}}
img{{max-width:100%;height:auto;border:1px solid #eee;border-radius:6px;background:#fff}}
.sub{{color:#666;font-size:12px;margin-bottom:6px}}
</style></head><body>
<h1>Mailtrace — KPI Overview</h1>
{kpi_html}
<div class="grid3">
  <div>
    <h2>Top Cities</h2>
    <table class="mini">
      <thead><tr><th>City</th><th>State</th><th>Matches</th></tr></thead>
      <tbody>{city_rows}</tbody>
    </table>
  </div>
  <div>
    <h2>Top ZIP Codes</h2>
    <table class="mini">
      <thead><tr><th>ZIP</th><th>Matches</th></tr></thead>
      <tbody>{zip_rows}</tbody>
    </table>
  </div>
  <div>
    <h2>Matches by Month</h2>
    <img src="{chart_data_uri}" alt="Matches by Month (mm-yy)"/>
  </div>
</div>
{growth_html}
<h2 style="margin-top:22px">Detailed Matches</h2>
<div class="wrap">
{table_html}
</div>
</body></html>"""
    return html
