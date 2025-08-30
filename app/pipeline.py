# app/pipeline.py — use legacy mailtrace_matcher and adapt to dashboard columns
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
    # your legacy matcher emits dd-mm-yy; convert to ISO for sorting
    # “None provided” -> ""
    if not isinstance(s, str) or s.strip().lower().startswith("none"):
        return ""
    m = re.match(r"^(\d{2})-(\d{2})-(\d{2})$", s.strip())
    if not m: return s  # pass through if it’s already ISO-like
    dd, mm, yy = m.groups()
    yyyy = f"20{yy}" if int(yy) <= 69 else f"19{yy}"
    return f"{yyyy}-{mm}-{dd}"

def _split_mail_full(addr: str):
    """
    Split '123 Main St Apt 2 Dallas TX 75001' -> (address1+address2, city, state, zip).
    Falls back gracefully if we can't parse.
    """
    if not isinstance(addr, str): return ("", "", "", "")
    toks = addr.strip().split()
    if not toks: return ("", "", "", "")
    # ZIP = last token with 5 digits
    zip5 = ""
    if re.match(r"^\d{5}$", toks[-1]):
        zip5 = toks[-1]; toks = toks[:-1]
    # STATE = last 2 letters
    state = ""
    if toks and re.match(r"^[A-Za-z]{2}$", toks[-1]):
        state = toks[-1]; toks = toks[:-1]
    # CITY = take the tail tokens until we hit a digit (start of street number) from the left
    # Easier: assume city is remaining tail token(s) after street parts; use a small list of known TX cities if helpful.
    # Here we just take the last token chunk as city if any remain.
    city = " ".join(toks[-2:]) if len(toks) >= 2 and not any(ch.isdigit() for ch in toks[-1]) else (toks[-1] if toks and not any(ch.isdigit() for ch in toks[-1]) else "")
    # address1+2 = everything before city tokens
    if city:
        ctoks = city.split()
        addr_tokens = toks[:-len(ctoks)]
    else:
        addr_tokens = toks
    mail_addr12 = " ".join(addr_tokens).strip()
    return (mail_addr12, city, state, zip5)

def run_pipeline(mail_csv_path: str, crm_csv_path: str) -> pd.DataFrame:
    # Read raw (strings)
    mail_raw = pd.read_csv(mail_csv_path, dtype=str, keep_default_na=False)
    crm_raw  = pd.read_csv(crm_csv_path,  dtype=str, keep_default_na=False)

    # Run your legacy matcher (it self-normalizes headers)
    legacy = run_matching(mail_raw.copy(), crm_raw.copy())

    # Pull a CRM amount column to show in dashboard
    amt_col = _find_col(crm_raw, "JobValue", "Amount", "Revenue", "Job Value", "Job_Value")
    crm_amount_by_id = {}
    id_col = _find_col(crm_raw, "crm_id", "CustomerID", "customerid", "lead_id", "job_id", "id")
    if id_col and amt_col:
        crm_amount_by_id = dict(zip(crm_raw[id_col].astype(str), crm_raw[amt_col].astype(str)))

    rows = []
    for _, r in legacy.iterrows():
        # CRM bits
        crm_job_date_iso = _iso_date_from_ddmmyy(r.get("crm_job_date", ""))
        crm_city  = r.get("crm_city", "")
        crm_state = r.get("crm_state", "")
        crm_zip   = str(r.get("crm_zip", ""))

        # CRM address1/2 (originals from legacy)
        crm_a1 = r.get("crm_address1_original", "")
        crm_a2 = r.get("crm_address2_original", "")

        # Mail side (split the full string best-effort)
        mail_full = r.get("matched_mail_full_address", "")
        mail_a12, mail_city, mail_state, mail_zip = _split_mail_full(mail_full)

        # Confidence + notes
        conf = int(r.get("confidence_percent", 0)) if str(r.get("confidence_percent", "")).strip().isdigit() else 0
        notes = r.get("match_notes", "")

        # Amount
        amt = ""
        rid = str(r.get("crm_id", ""))
        if rid and rid in crm_amount_by_id:
            amt = crm_amount_by_id[rid]

        rows.append({
            # minimal set your dashboard/export expects
            "crm_job_date": crm_job_date_iso or r.get("crm_job_date", ""),
            "crm_city": crm_city,
            "crm_state": crm_state,
            "crm_zip": crm_zip,
            "crm_address1": crm_a1,
            "crm_address2": crm_a2,
            "address1": mail_a12,          # mail address line(s)
            "address2": "",                 # unknown after split; keep blank
            "city": mail_city,
            "state": mail_state,
            "zip": mail_zip,
            "confidence": conf,
            "match_notes": notes,
            "crm_amount": amt,
        })

    return pd.DataFrame(rows)
