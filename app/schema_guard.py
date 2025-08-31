# app/schema_guard.py
# Schema Guard v1.0
# - Auto-detects required columns by header names and by cell content.
# - If anything is missing/ambiguous, returns an HTML mapping form so users pick columns.
# - Otherwise returns a ready-to-use mapping dict (canonical -> original header).

from __future__ import annotations
import re, html
from typing import Dict, List, Tuple, Optional
import pandas as pd

# ------------------------
# Canonical fields we want
# ------------------------
MAIL_REQUIRED = ["address1", "city", "state", "zip", "mail_date"]
MAIL_OPTIONAL = ["address2", "name"]
CRM_REQUIRED  = ["crm_address1", "crm_city", "crm_state", "crm_zip", "crm_job_date", "crm_amount"]
CRM_OPTIONAL  = ["crm_address2", "crm_name"]

# Header “hints” by canonical field (lowercased, substrings)
HINTS = {
    "address1":   ["address1", "addr1", "address", "street", "line1"],
    "address2":   ["address2", "addr2", "unit", "apt", "suite", "line2", "bldg", "building"],
    "city":       ["city", "town"],
    "state":      ["state", "st"],
    "zip":        ["zip", "zipcode", "zip_code", "postal", "postalcode", "zip5"],
    "mail_date":  ["mail_date", "mailed", "sent_date", "date_mailed", "mailing_date", "date"],
    "name":       ["name", "full_name", "customer", "recipient"],

    "crm_address1":  ["address1", "addr1", "address", "street", "line1"],
    "crm_address2":  ["address2", "addr2", "unit", "apt", "suite", "line2", "bldg", "building"],
    "crm_city":      ["city", "town"],
    "crm_state":     ["state", "st"],
    "crm_zip":       ["zip", "zipcode", "zip_code", "postal", "postalcode", "zip5"],
    "crm_job_date":  ["job_date", "date", "created_at", "date_entered", "datecreated", "install_date"],
    "crm_amount":    ["amount", "value", "job_value", "revenue", "invoice", "contract", "total", "$"],
    "crm_name":      ["name", "full_name", "customer", "client"],
}

DATE_RE = re.compile(
    r"^(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4})$"
)
MONEY_RE = re.compile(r"^\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?$")
ZIP_RE   = re.compile(r"^\d{5}(?:-\d{4})?$")
US_STATES = set("""
AL AK AZ AR CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN
MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA
WI WV WY
""".strip().split())

def _lc_set(seq: List[str]) -> set:
    return set([str(s).strip().lower() for s in seq if str(s).strip()])

def _sample_values(series: pd.Series, n: int = 5) -> List[str]:
    vals = []
    for v in series.dropna().astype(str).head(50):
        v = v.strip()
        if v:
            vals.append(v)
        if len(vals) >= n:
            break
    return vals

def _score_header(canonical: str, header: str) -> int:
    """Score a single header name vs canonical using substring hints."""
    h = header.strip().lower()
    hints = HINTS.get(canonical, [])
    score = 0
    for i, hint in enumerate(hints):
        if hint in h:
            score += max(10 - i, 1)  # earlier hints worth more
    if canonical.endswith("amount") and "$" in h:
        score += 2
    return score

def _looks_like_date_col(s: pd.Series) -> bool:
    s = s.dropna().astype(str).str.strip()
    if s.empty: return False
    probe = s.head(50)
    ok = 0
    for v in probe:
        if DATE_RE.match(v):
            ok += 1
            continue
        try:
            pd.to_datetime(v, errors="raise")
            ok += 1
        except Exception:
            pass
    return (ok / len(probe)) >= 0.7

def _looks_like_money_col(s: pd.Series) -> bool:
    s = s.dropna().astype(str).str.strip()
    if s.empty: return False
    probe = s.head(50)
    ok = 0
    for v in probe:
        if MONEY_RE.match(v.replace("USD", "").strip()):
            ok += 1
    return (ok / len(probe)) >= 0.7

def _looks_like_zip_col(s: pd.Series) -> bool:
    s = s.dropna().astype(str).str.strip()
    if s.empty: return False
    probe = s.head(50)
    ok = 0
    for v in probe:
        if ZIP_RE.match(v):
            ok += 1
    return (ok / len(probe)) >= 0.7

def _looks_like_state_col(s: pd.Series) -> bool:
    s = s.dropna().astype(str).str.strip().str.upper()
    if s.empty: return False
    probe = s.head(50)
    ok = 0
    for v in probe:
        if v in US_STATES:
            ok += 1
    return (ok / len(probe)) >= 0.7

def _looks_like_address1_col(s: pd.Series) -> bool:
    # Heuristic: many rows start with a number + word (e.g., "123 Main")
    s = s.dropna().astype(str).str.strip()
    if s.empty: return False
    probe = s.head(50)
    ok = 0
    for v in probe:
        if re.match(r"^\d{1,6}\s+\S+", v):
            ok += 1
    return (ok / len(probe)) >= 0.6

CONTENT_CHECK = {
    "mail_date": _looks_like_date_col,
    "crm_job_date": _looks_like_date_col,
    "crm_amount": _looks_like_money_col,
    "zip": _looks_like_zip_col,
    "crm_zip": _looks_like_zip_col,
    "state": _looks_like_state_col,
    "crm_state": _looks_like_state_col,
    "address1": _looks_like_address1_col,
    "crm_address1": _looks_like_address1_col,
}

def _auto_map(df: pd.DataFrame, need_fields: List[str]) -> Tuple[Dict[str,str], Dict[str,List[str]]]:
    """Return (mapping, ambiguous) using header hints first; mapping is canonical->original_header."""
    cols = list(df.columns)
    lc_cols = [c.lower().strip() for c in cols]

    mapping: Dict[str, str] = {}
    ambiguous: Dict[str, List[str]] = {}

    for canon in need_fields:
        # Rank by header score
        scored = []
        for hdr, lc in zip(cols, lc_cols):
            s = _score_header(canon, lc)
            if s > 0:
                scored.append((s, hdr))
        scored.sort(reverse=True)

        if scored:
            top_score = scored[0][0]
            cands = [hdr for s, hdr in scored if s == top_score]
            if len(cands) == 1:
                mapping[canon] = cands[0]
            else:
                ambiguous[canon] = cands[:5]  # too many options, ask user
        # else: leave unmapped for content inference

    return mapping, ambiguous

def _fill_by_content(df: pd.DataFrame, mapping: Dict[str,str], need_fields: List[str]) -> Tuple[Dict[str,str], Dict[str,List[str]]]:
    ambiguous: Dict[str, List[str]] = {}
    for canon in need_fields:
        if canon in mapping:
            continue
        checker = CONTENT_CHECK.get(canon)
        if not checker:
            continue
        cands = []
        for hdr in df.columns:
            try:
                if checker(df[hdr]):
                    cands.append(hdr)
            except Exception:
                pass
        if len(cands) == 1:
            mapping[canon] = cands[0]
        elif len(cands) > 1:
            ambiguous[canon] = cands[:5]
    return mapping, ambiguous

def analyze_dataframes(mail_df: pd.DataFrame, crm_df: pd.DataFrame) -> Tuple[Optional[Dict[str,str]], Optional[str]]:
    """
    Returns (mapping, html_or_none). If mapping is None, html contains the mapping UI to show.
    On success, mapping contains canonical->original header for both mail & crm.
    """
    mail_need = MAIL_REQUIRED + MAIL_OPTIONAL
    crm_need  = CRM_REQUIRED + CRM_OPTIONAL

    mail_map, mail_amb = _auto_map(mail_df, mail_need)
    crm_map,  crm_amb  = _auto_map(crm_df,  crm_need)

    # Content inference for missing
    mail_map, mail_amb2 = _fill_by_content(mail_df, mail_map, MAIL_REQUIRED)
    crm_map,  crm_amb2  = _fill_by_content(crm_df,  crm_map,  CRM_REQUIRED)

    # Merge ambiguous sets
    for k, v in mail_amb2.items():
        mail_amb.setdefault(k, v)
    for k, v in crm_amb2.items():
        crm_amb.setdefault(k, v)

    # Which required fields still missing?
    mail_missing = [f for f in MAIL_REQUIRED if f not in mail_map]
    crm_missing  = [f for f in CRM_REQUIRED  if f not in crm_map]

    # If nothing missing and no ambiguity, success
    if not mail_missing and not crm_missing and not mail_amb and not crm_amb:
        # Compact mapping keys with prefixes m:/c: so caller can split if needed
        joined = {f"mail:{k}": v for k, v in mail_map.items()}
        joined.update({f"crm:{k}": v for k, v in crm_map.items()})
        return joined, None

    # Build mapping UI HTML
    def opt_list(headers: List[str], selected: Optional[str] = None) -> str:
        ops = []
        for h in headers:
            sel = ' selected' if selected and h == selected else ''
            ops.append(f'<option value="{html.escape(h)}"{sel}>{html.escape(h)}</option>')
        return "\n".join(ops)

    def field_block(kind: str, missing: List[str], amb: Dict[str,List[str]], df: pd.DataFrame) -> str:
        if not missing and not amb:
            return ""
        headers = list(df.columns)
        samples = {h: _sample_values(df[h]) for h in headers}
        rows = []
        need = missing + list(amb.keys())
        for f in need:
            cands = amb.get(f, headers)
            rows.append(f"""
              <div class="map-row">
                <div class="left">
                  <div class="lbl">{html.escape(f)}</div>
                  <div class="hint">{ "Required" if (("mail" in kind and f in MAIL_REQUIRED) or ("crm" in kind and f in CRM_REQUIRED)) else "Optional" }</div>
                </div>
                <div class="right">
                  <select name="{kind}:{html.escape(f)}">
                    <option value="">-- choose a column --</option>
                    {opt_list(cands)}
                  </select>
                  <div class="samples">{html.escape(", ".join(samples.get(cands[0], [])[:3]))}</div>
                </div>
              </div>
            """)
        title = "Mail file: pick columns" if kind == "mail" else "CRM file: pick columns"
        return f'<div class="card"><h3>{title}</h3>{"".join(rows)}</div>'

    css = """
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; color:#0f172a; }
      .wrap { max-width: 960px; margin: 0 auto; }
      h2 { margin: 0 0 8px; }
      .lead { color:#475569; margin-bottom: 16px; }
      .grid { display:grid; grid-template-columns: 1fr; gap:16px; }
      .card { border:1px solid #e5e7eb; border-radius: 12px; padding: 16px; background:#fff; }
      .map-row { display:flex; gap:12px; align-items:flex-start; margin: 10px 0; }
      .map-row .left { width: 200px; }
      .map-row .lbl { font-weight: 800; }
      .map-row .hint { color:#64748b; font-size: 12px; }
      .map-row .right select { width: 100%; padding: 10px; border-radius: 8px; border:1px solid #cbd5e1; }
      .samples { color:#64748b; font-size: 12px; margin-top:6px; }
      .actions { display:flex; gap:12px; margin-top: 16px; }
      .btn { background:#0c2d4e; color:#fff; border:1px solid #0c2d4e; font-weight:800; padding:12px 18px; border-radius:10px; text-decoration:none; display:inline-block; }
      .btn.secondary { background:#fff; color:#0c2d4e; }
      .err { color:#b91c1c; margin: 8px 0; }
    </style>
    """

    mail_html = field_block("mail", mail_missing, mail_amb, mail_df)
    crm_html  = field_block("crm",  crm_missing,  crm_amb,  crm_df)

    html_out = f"""
    {css}
    <div class="wrap">
      <h2>We need a couple quick fixes</h2>
      <div class="lead">We couldn’t confidently identify some columns. Pick them below and we’ll continue.</div>
      <form method="POST" action="/map">
        <input type="hidden" name="upload_token" value="" />
        <div class="grid">
          {mail_html}
          {crm_html}
        </div>
        <div class="actions">
          <button class="btn" type="submit">Continue</button>
          <a class="btn secondary" href="/">Cancel</a>
        </div>
      </form>
    </div>
    """
    return None, html_out
