# dashboard_export.py — builds the full dashboard HTML
import io, base64
from datetime import date
from typing import Optional
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt

BRAND = "#0c2d4e"
ACCENT = "#759d40"

def _fmt_money(x: float) -> str:
    try:
        return "${:,.2f}".format(float(x))
    except Exception:
        return "$0.00"

def _confidence_badge(c: int) -> str:
    if c >= 94:
        klass = "conf-high"
    elif c >= 88:
        klass = "conf-mid"
    else:
        klass = "conf-low"
    return f'<span class="badge {klass}">{c}%</span>'

def _month_key(d: str) -> Optional[str]:
    # d expected like "dd-mm-yy" from matcher; be tolerant
    if not d or not isinstance(d, str): return None
    try:
        # try dd-mm-yy
        dt = pd.to_datetime(d, dayfirst=True, errors="coerce")
        if pd.isna(dt): return None
        return dt.strftime("%b %Y")
    except Exception:
        return None

def _bar_chart_months(df: pd.DataFrame) -> str:
    """Returns a data-uri PNG of a horizontal bar chart (months on x-axis)."""
    if df.empty or "crm_date" not in df.columns:
        return ""

    months = df["crm_date"].map(_month_key).dropna()
    if months.empty:
        return ""

    counts = months.value_counts().sort_index(key=lambda s: pd.to_datetime(s, format="%b %Y"))
    # Plot
    fig = plt.figure(figsize=(9, 3.6), dpi=160)
    ax = fig.add_subplot(111)
    ax.bar(range(len(counts)), counts.values)
    ax.set_xticks(range(len(counts)))
    ax.set_xticklabels(counts.index.tolist(), rotation=45, ha="right")
    ax.set_ylabel("Matches")
    ax.set_xlabel("Month")
    ax.set_title("Matched Jobs by Month")
    fig.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    data = base64.b64encode(buf.getvalue()).decode("ascii")
    return f'<img class="chart" src="data:image/png;base64,{data}" alt="Matches by Month" />'

def render_full_dashboard(summary_df: pd.DataFrame,
                          mail_all_df: pd.DataFrame,
                          crm_all_df: pd.DataFrame,
                          brand_logo_url: str) -> str:
    # KPIs
    total_mail = len(mail_all_df) if isinstance(mail_all_df, pd.DataFrame) else 0
    total_matches = len(summary_df) if isinstance(summary_df, pd.DataFrame) else 0
    total_revenue = float(summary_df["amount"].sum()) if total_matches else 0.0

    # Avg mailers before engagement (count of mail_dates per row)
    def _count_mailers(s):
        s = str(s or "").strip()
        return 0 if not s else len([p for p in s.split(",") if p.strip()])
    avg_mailers = summary_df["mail_dates"].map(_count_mailers).mean() if total_matches else 0.0

    mailers_per_acq = (total_mail / total_matches) if total_matches else 0.0

    # Top Cities / Zips (from CRM side of matched rows)
    city_counts = pd.Series(dtype=int)
    zip_counts = pd.Series(dtype=int)
    if total_matches:
        city_pairs = (summary_df["_crm_city"].fillna("") + ", " + summary_df["_crm_state"].fillna("")).str.strip(", ")
        city_counts = city_pairs.value_counts().head(5)
        zip_counts = summary_df["_crm_zip5"].fillna("").value_counts().head(5)

    # Chart
    chart_html = _bar_chart_months(summary_df)

    # Summary table (hide bucket; place confidence before notes)
    table_rows = []
    if total_matches:
        # Keep up to 200 rows for speed; sorted already by matcher
        view = summary_df.head(200)
        for _, r in view.iterrows():
            badge = _confidence_badge(int(r.get("confidence", 0)))
            table_rows.append(f"""
            <tr>
              <td class="mono">{r.get('mail_dates','')}</td>
              <td class="mono">{r.get('crm_date','')}</td>
              <td class="mono">{_fmt_money(r.get('amount', 0.0))}</td>
              <td>{r.get('mail_address1','')}</td>
              <td class="muted">{r.get('mail_city_state_zip','')}</td>
              <td>{r.get('crm_address1','')}</td>
              <td class="muted">{r.get('crm_city_state_zip','')}</td>
              <td class="mono">{badge}</td>
              <td>{r.get('match_notes','')}</td>
            </tr>
            """)
    else:
        table_rows.append('<tr><td colspan="9" class="muted">No matches found.</td></tr>')

    # Build HTML
    return f"""
<div class="topbar">
  <div class="container nav">
    <a class="brand" href="/"><img class="logo" src="{brand_logo_url}" alt="MailTrace logo" /></a>
  </div>
</div>

<div class="container">
  <div class="grid kpis">
    <div class="card kpi"><div class="k">Total mail records</div><div class="v">{total_mail:,}</div></div>
    <div class="card kpi"><div class="k">Matches</div><div class="v">{total_matches:,}</div></div>
    <div class="card kpi"><div class="k">Total revenue generated</div><div class="v">{_fmt_money(total_revenue)}</div></div>
    <div class="card kpi"><div class="k">Avg mailers before engagement</div><div class="v">{avg_mailers:.2f}</div></div>
    <div class="card kpi"><div class="k">Mailers per acquisition</div><div class="v">{mailers_per_acq:.2f}</div></div>
  </div>

  <div class="row">
    <div class="card flex1">
      <h3>Top Cities (matches)</h3>
      <div class="scroll">
        {"".join(f'<div class="rowline"><span>{city}</span><b>{count}</b></div>' for city, count in city_counts.items()) or '<div class="muted">No data</div>'}
      </div>
    </div>
    <div class="card flex1">
      <h3>Top ZIP Codes (matches)</h3>
      <div class="scroll">
        {"".join(f'<div class="rowline"><span>{z}</span><b>{count}</b></div>' for z, count in zip_counts.items()) or '<div class="muted">No data</div>'}
      </div>
    </div>
  </div>

  <div class="card">
    {chart_html or '<div class="muted">No monthly data</div>'}
  </div>

  <div class="card">
    <h3>Sample of Matches</h3>
    <div class="note">Sorted by most recent CRM date (falls back to mail date). Showing up to 200 rows. Use <b>Download All</b> for the full CSV.</div>
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
          {''.join(table_rows)}
        </tbody>
      </table>
    </div>
    <form method="post" action="/download" style="margin-top:12px">
      <button class="button">Download All</button>
    </form>
  </div>
</div>
"""
