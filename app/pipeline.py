# app/pipeline.py — legacy matcher adapter + robust mail city/state/zip split
from __future__ import annotations
import re
import pandas as pd

# import your legacy matcher
try:
    from app.mailtrace_matcher import run_matching
except Exception:
    from .mailtrace_matcher import run_matching

def _find_col(df: pd.DataFrame, *cands):
    if df is None or df.empty: return None
    lower = {c.lower(): c for c in df.columns}
    for c in cands:
        hit = lower.get(c.lower())
        if hit: return hit
    return None

def _iso_date_from_ddmmyy(s: str) -> str:
    # legacy emits dd-mm-yy or “None provided”
    if not isinstance(s, str) or s.strip().lower().startswith("none"):
        return ""
    m = re.match(r"^(\d{2})-(\d{2})-(\d{2})$", s.strip())
    if not m: 
        return s  # already ISO or mixed
    dd, mm, yy = m.groups()
    yyyy = f"20{yy}" if int(yy) <= 69 else f"19{yy}"
    return f"{yyyy}-{mm}-{dd}"

_ZIP_RE = re.compile(r"(?:^|\s)(\d{5})(?:-(\d{4}))?\s*$")
_STATE_RE = re.compile(r"^[A-Za-z]{2}$")

def _split_mail_full(addr: str):
    """
    Robustly split "123 N Valley View Rd Apt 2 Dallas TX 75009-6643"
    -> (address1+address2, city, state, zip)
    Strategy: parse from right: ZIP(+4) -> STATE(AA) -> CITY (no digits).
    Leaves address1 as everything to the left of city.
    """
    if not isinstance(addr, str): 
        return ("", "", "", "")
    s = " ".join(addr.strip().split())  # squash spaces

    # ZIP(+4) from the right
    mz = _ZIP_RE.search(s)
    if not mz:
        # No ZIP found; give up gracefully
        return (s, "", "", "")
    zip5 = mz.group(1)
    plus4 = mz.group(2)
    zip_full = f"{zip5}-{plus4}" if plus4 else zip5
    s_left = s[:mz.start()].rstrip()

    # STATE (2 letters) before ZIP
    parts_left = s_left.split()
    if not parts_left:
        return (s_left, "", "", zip_full)
    state_tok = parts_left[-1].strip(".,")
    if _STATE_RE.match(state_tok):
        state = state_tok.upper()
        s_left = " ".join(parts_left[:-1]).rstrip()
    else:
        # No clear state token
        state = ""

    # CITY: take trailing tokens that contain no digits
    tokens = s_left.split()
    city_tokens = []
    while tokens and not any(ch.isdigit() for ch in tokens[-1]):
        city_tokens.insert(0, tokens.pop())  # build city from right
        # stop if the next left token clearly looks like part of the street (often has digits)
        if tokens and any(ch.isdigit() for ch in tokens[-1]):
            break
    city = " ".join(t.strip(".,") for t in city_tokens).strip()
    addr12 = " ".join(tokens).strip()

    return (addr12, city, state, zip_full)

def run_pipeline(mail_csv_path: str, crm_csv_path: str) -> pd.DataFrame:
    # Read raw (strings)
    mail_raw = pd.read_csv(mail_csv_path, dtype=str, keep_default_na=False)
    crm_raw  = pd.read_csv(crm_csv_path,  dtype=str, keep_default_na=False)

    # Run your legacy matcher (self-normalizes headers)
    legacy = run_matching(mail_raw.copy(), crm_raw.copy())

    # Pull a CRM amount column to show in dashboard
    amt_col = _find_col(crm_raw, "JobValue", "Amount", "Revenue", "Job Value", "Job_Value")
    id_col  = _find_col(crm_raw, "crm_id", "CustomerID", "customerid", "lead_id", "job_id", "id")
    crm_amount_by_id = {}
    if id_col and amt_col:
        crm_amount_by_id = dict(zip(crm_raw[id_col].astype(str), crm_raw[amt_col].astype(str)))

    rows = []
    for _, r in legacy.iterrows():
        # CRM bits
        crm_job_date_iso = _iso_date_from_ddmmyy(r.get("crm_job_date", ""))
        crm_city  = str(r.get("crm_city", "") or "").strip().rstrip(".")
        crm_state = str(r.get("crm_state", "") or "").strip().upper()
        crm_zip   = str(r.get("crm_zip", "") or "").strip()

        # CRM original address lines (for display)
        crm_a1 = str(r.get("crm_address1_original", "") or "")
        crm_a2 = str(r.get("crm_address2_original", "") or "")

        # Mail side (split the full string best-effort)
        mail_full = str(r.get("matched_mail_full_address", "") or "")
        mail_a12, mail_city, mail_state, mail_zip = _split_mail_full(mail_full)

        # Fallbacks: if splitter fails, borrow CRM geography for display only
        if not mail_city and crm_city:
            mail_city = crm_city
        if not mail_state and crm_state:
            mail_state = crm_state
        if not mail_zip and crm_zip:
            mail_zip = crm_zip

        # Confidence + notes
        conf = r.get("confidence_percent", 0)
        try:
            conf = int(conf)
        except Exception:
            conf = 0
        notes = str(r.get("match_notes", "") or "")

        # Amount
        amt = ""
        rid = str(r.get("crm_id", "") or "")
        if rid and rid in crm_amount_by_id:
            amt = crm_amount_by_id[rid]

        rows.append({
            # columns your dashboard/export expects
            "crm_job_date": crm_job_date_iso or r.get("crm_job_date", ""),
            "crm_city": crm_city,
            "crm_state": crm_state,
            "crm_zip": crm_zip,
            "crm_address1": crm_a1,
            "crm_address2": crm_a2,

            "address1": mail_a12,      # mail address line(s)
            "address2": "",            # unknown after split; keep blank
            "city": mail_city,
            "state": mail_state,
            "zip": mail_zip,

            "confidence": conf,
            "match_notes": notes,
            "crm_amount": amt,
        })

    return pd.DataFrame(rows)
