# app/dashboard_export.py - COMPLETE FILE with minimal fix
from __future__ import annotations
import math, re
import pandas as pd

BRAND = "#0c2d4e"
ACCENT = "#759d40"

def _safe_int(x, default=0):
    try:
        if isinstance(x, str) and x.endswith("%"):
            x = x[:-1]
        return int(float(x))
    except Exception:
        return default

def _safe_float(x, default=0.0):
    try:
        if isinstance(x, str):
            x = x.replace("$", "").replace(",", "").strip()
        return float(x)
    except Exception:
        return default

def _fmt_currency(x) -> str:
    try:
        val = _safe_float(x)
        return f"${val:,.2f}" if val > 0 else ""
    except:
        return ""

def _escape(s: str) -> str:
    if s is None:
        return ""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def finalize_summary_for_export_v17(summary: pd.DataFrame) -> pd.DataFrame:
    """Convert raw pipeline output to standardized format for dashboard"""
    
    if summary.empty:
        return pd.DataFrame({
            "Mail Dates": [], "CRM Date": [], "Amount": [], "Mail Address": [],
            "Mail City/State/Zip": [], "CRM Address": [], "CRM City/State/Zip": [],
            "Confidence": [], "Notes": []
        })
    
    df = summary.copy()
    
    # FIXED: Use exact column names from your matching output
    mail_dates = df.get("mail_dates_in_window", pd.Series([""] * len(df)))
    crm_dates = df.get("crm_job_date", pd.Series([""] * len(df)))
    amounts = df.get("crm_amount", pd.Series([""] * len(df))).apply(_fmt_currency)
    mail_addrs = df.get("matched_mail_full_address", pd.Series([""] * len(df)))
    
    # CRM address components
    crm_addr1 = df.get("crm_address1_original", pd.Series([""] * len(df)))
    crm_addr2 = df.get("crm_address2_original", pd.Series([""] * len(df)))
    
    # Build full CRM address
    def build_crm_address(addr1, addr2):
        addr1 = str(addr1 or "").strip()
        addr2 = str(addr2 or "").strip()
        if addr2:
            return f"{addr1}, {addr2}"
        return addr1
    
    crm_full_addr = pd.Series([
        build_crm_address(a1, a2) 
        for a1, a2 in zip(crm_addr1, crm_addr2)
    ])
    
    # CRM geography
    crm_city = df.get("crm_city", pd.Series([""] * len(df)))
    crm_state = df.get("crm_state", pd.Series([""] * len(df)))
    crm_zip = df.get("crm_zip", pd.Series([""] * len(df)))
    
    crm_geography = pd.Series([
        f"{city}, {state} {zip_code}".strip(", ") 
        for city, state, zip_code in zip(crm_city, crm_state, crm_zip)
    ])
    
    # Confidence and notes
    confidence = df.get("confidence_percent", pd.Series([0] * len(df))).apply(lambda x: _safe_int(x, 0))
    notes = df.get("match_notes", pd.Series([""] * len(df)))
    
    # Build final result
    result = pd.DataFrame({
        "Mail Dates": mail_dates,
        "CRM Date": crm_dates,
        "Amount": amounts,
        "Mail Address": mail_addrs,
        "Mail City/State/Zip": "",
        "CRM Address": crm_full_addr,
        "CRM City/State/Zip": crm_geography,
        "Confidence": confidence,
        "Notes": notes
    })
    
    # Store aux data for KPIs (keep your existing logic)
    raw_amounts = df.get("crm_amount", pd.Series([0] * len(df))).apply(_safe_float)
    result.__aux_amounts = raw_amounts
    result.__aux_crm_city = crm_city
    result.__aux_crm_state = crm_state
    result.__aux_crm_zip = crm_zip
    
    # Parse dates for charts
    def safe_date_parse(date_str):
        try:
            if not date_str or str(date_str).strip() in ["", "None provided"]:
                return pd.NaT
            date_str = str(date_str).strip()
            if re.match(r'\d{2}-\d{2}-\d{2}', date_str):
                return pd.to_datetime(date_str, format='%d-%m-%y')
            return pd.to_datetime(date_str, errors='coerce')
        except:
            return pd.NaT
    
    result.__aux_dates = crm_dates.apply(safe_date_parse)
    
    return result

def render_full_dashboard_v17(summary_df: pd.DataFrame, mail_total_count: int) -> str:
    """Render the complete dashboard HTML"""
    
    if summary_df.empty:
        return "<div style='padding: 40px; text-align: center;'>No matches found in your data.</div>"
    
    # Basic stats
    total_mail = int(mail_total_count or 0)
    total_matches = len(summary_df)
    
    # Revenue calculation
    try:
        amounts = getattr(summary_df, '__aux_amounts', pd.Series([0.0] * len(summary_df)))
        total_revenue = float(amounts.sum())
    except:
        total_revenue = 0.0
    
    # Calculate KPIs safely
    avg_mailers = 0.0
    mailers_per_acq = (total_mail / total_matches) if total_matches > 0 else 0.0
    
    # Top cities and zips
    try:
        crm_cities = getattr(summary_df, '__aux_crm_city', pd.Series([""]))
        crm_states = getattr(summary_df, '__aux_crm_state', pd.Series([""]))  
        crm_zips = getattr(summary_df, '__aux_crm_zip', pd.Series([""]))
        
        city_state = (crm_cities + ", " + crm_states).str.strip(", ")
        top_cities = city_state[city_state != ""].value_counts().head(5)
        top_zips = crm_zips[crm_zips.str.strip() != ""].value_counts().head(5)
    except:
        top_cities = pd.Series(dtype=object)
        top_zips = pd.Series(dtype=object)
    
    # Monthly data
    try:
        dates = getattr(summary_df, '__aux_dates', pd.Series([]))
        month_counts = dates.dropna().dt.to_period('M').value_counts().sort_index()
    except:
        month_counts = pd.Series(dtype=object)
    
    # Confidence color coding
    def conf_class(conf_val):
        try:
            conf = int(conf_val)
            if conf >= 94:
                return "conf-high"
            elif conf >= 88:
                return "conf-mid"
            else:
                return "conf-low"
        except:
            return "conf-low"
    
    # Build table rows
    rows = []
    for _, row in summary_df.iterrows():
        conf_val = row.get("Confidence", 0)
        rows.append(f"""
        <tr>
            <td class="mono">{_escape(row.get("Mail Dates", ""))}</td>
            <td class="mono">{_escape(row.get("CRM Date", ""))}</td> 
            <td class="mono">{_escape(row.get("Amount", ""))}</td>
            <td>{_escape(row.get("Mail Address", ""))}</td>
            <td>{_escape(row.get("Mail City/State/Zip", ""))}</td>
            <td>{_escape(row.get("CRM Address", ""))}</td>
            <td>{_escape(row.get("CRM City/State/Zip", ""))}</td>
            <td class="conf {conf_class(conf_val)}">{conf_val}%</td>
            <td>{_escape(row.get("Notes", ""))}</td>
        </tr>
        """)
    
    # Helper functions
    def render_top_list(data_series):
        if len(data_series) == 0:
            return "<div style='padding: 20px; text-align: center; color: #64748b;'>No data</div>"
        
        items = []
        for label, count in data_series.items():
            items.append(f"""
            <div style="display: flex; justify-content: space-between; padding: 8px; border-bottom: 1px dashed #eee;">
                <span style="font-weight: 600;">{_escape(str(label))}</span>
                <span style="color: #64748b;">{int(count)}</span>
            </div>
            """)
        return "".join(items)
    
    def render_chart(month_data):
        if len(month_data) == 0:
            return "<div style='padding: 20px; text-align: center; color: #64748b;'>No monthly data</div>"
        
        max_val = max(month_data.values) if len(month_data) > 0 else 1
        
        # Horizontal chart with months on x-axis
        chart_html = """
        <div style="display: flex; flex-direction: column; gap: 8px;">
            <div style="display: flex; align-items: end; gap: 4px; height: 200px; padding: 10px; border-bottom: 2px solid #e5e7eb;">
        """
        
        for month, count in month_data.items():
            height = int((count / max_val) * 160) if max_val > 0 else 0
            height = max(height, 8)
            
            chart_html += f"""
                <div style="display: flex; flex-direction: column; align-items: center; flex: 1;">
                    <div style="font-size: 11px; color: #64748b; margin-bottom: 4px;">{int(count)}</div>
                    <div style="width: 100%; height: {height}px; background: linear-gradient(to top, {BRAND}, {ACCENT}); border-radius: 4px 4px 0 0; margin-bottom: 4px;"></div>
                </div>
            """
        
        chart_html += """
            </div>
            <div style="display: flex; gap: 4px;">
        """
        
        for month, count in month_data.items():
            chart_html += f"""
                <div style="flex: 1; text-align: center; font-size: 11px; color: #64748b; transform: rotate(-45deg); transform-origin: center;">{month}</div>
            """
        
        chart_html += """
            </div>
        </div>
        """
        
        return chart_html
    
    # Generate the complete HTML
    html = f"""
    <style>
        :root {{
            --brand: {BRAND};
            --accent: {ACCENT};
            --bg: #ffffff;
            --text: #0f172a;
            --muted: #64748b;
            --card: #ffffff;
            --border: #e5e7eb;
        }}
        * {{ box-sizing: border-box; }}
        body {{ margin:0; padding:0; font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
        .grid-kpi {{ display:grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 16px 0; }}
        .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 16px; padding: 16px; box-shadow: 0 2px 10px rgba(0,0,0,0.04); }}
        .kpi {{ border-top: 4px solid var(--brand); }}
        .kpi .k {{ color: var(--muted); font-size: 13px; }}
        .kpi .v {{ font-size: 30px; font-weight: 900; }}
        .grid-top {{ display:grid; grid-template-columns: 1fr 1fr; gap: 16px; margin: 16px 0; }}
        table {{ width: 100%; border-collapse: collapse; background: #fff; border-radius: 12px; overflow: hidden; }}
        thead th {{ background: #f8fafc; text-align:left; padding: 12px 14px; border-bottom: 1px solid #f1f5f9; font-size: 13px; }}
        tbody td {{ padding: 12px 14px; border-bottom: 1px solid #f3f4f6; font-size: 14px; }}
        tbody tr:hover {{ background:#fafafa; }}
        .mono {{ font-family: ui-monospace, monospace; }}
        .conf {{ font-weight: 800; }}
        .conf-high {{ color: #065f46; }}
        .conf-mid {{ color: #92400e; }}
        .conf-low {{ color: #991b1b; }}
        @media (max-width: 900px) {{
            .grid-kpi {{ grid-template-columns: 1fr 1fr; }}
            .grid-top {{ grid-template-columns: 1fr; }}
        }}
    </style>
    
    <div class="container">
        <!-- KPIs -->
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
                <div class="v">{avg_mailers:.2f}</div>
            </div>
        </div>
        <div class="card kpi">
            <div class="k">Mailers per acquisition</div>
            <div class="v">{mailers_per_acq:.2f}</div>
        </div>
        
        <!-- Top Lists -->
        <div class="grid-top">
            <div class="card">
                <div class="k" style="margin-bottom:8px;">Top Cities (matches)</div>
                {render_top_list(top_cities)}
            </div>
            <div class="card">
                <div class="k" style="margin-bottom:8px;">Top ZIP Codes (matches)</div>
                {render_top_list(top_zips)}
            </div>
        </div>
        
        <!-- Chart -->
        <div class="card">
            <div class="k" style="margin-bottom:8px;">Matched Jobs by Month</div>
            {render_chart(month_counts)}
        </div>
        
        <!-- Table -->
        <div class="card">
            <div class="k" style="margin-bottom:8px;">Sample of Matches</div>
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
                        {"".join(rows)}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    """
    
    return html
