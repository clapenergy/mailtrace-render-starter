# app/pipeline.py
# Glue code that reads the two CSVs, maps columns, and calls your v17 matcher.

import pandas as pd
from typing import Dict
from .matching_logic_v17 import match_mail_to_crm

# --- alias maps so we can accept many header variants from users ---
MAIL_ALIASES: Dict[str, str] = {
    # normalized -> possible user headers (lowercase compare)
    "address1": "address",         # Address, address1
    "address2": "address2",        # optional
    "city": "city",
    "state": "state",
    "zip": "zip",
    "mail_date": "maildate",       # MailDate, mailed, date, etc.
}
CRM_ALIASES: Dict[str, str] = {
    "crm_address1": "address",     # Address, address1
    "crm_address2": "address2",
    "crm_city": "city",
    "crm_state": "state",
    "crm_zip": "zip",
    "crm_job_date": "dateentered", # DateEntered, jobdate, etc.
}

def _find_col(df: pd.DataFrame, *candidates):
    """Return first matching column name (case-insensitive)."""
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None

def _alias_mail(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    # Accept common variants
    a1 = _find_col(df, "Address1", "Address")
    a2 = _find_col(df, "Address2")
    city = _find_col(df, "City", "MailCity")
    state = _find_col(df, "State", "MailState")
    zipc = _find_col(df, "Zip", "Zip5", "PostalCode")
    mdate = _find_col(df, "MailDate", "Mailed", "Date", "Mail Date")
    # Build standardized columns expected by matching_logic_v17
    out["address1"] = df[a1] if a1 else ""
    out["address2"] = df[a2] if a2 else ""
    out["city"] = df[city] if city else ""
    out["state"] = df[state] if state else ""
    out["zip"] = df[zipc] if zipc else ""
    out["mail_date"] = df[mdate] if mdate else ""
    return out.fillna("")

def _alias_crm(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    a1 = _find_col(df, "Address1", "Address")
    a2 = _find_col(df, "Address2")
    city = _find_col(df, "City")
    state = _find_col(df, "State")
    zipc = _find_col(df, "Zip", "Zip5", "PostalCode")
    jdate = _find_col(df, "DateEntered", "JobDate", "Date", "CreatedDate")
    out["crm_address1"] = df[a1] if a1 else ""
    out["crm_address2"] = df[a2] if a2 else ""
    out["crm_city"] = df[city] if city else ""
    out["crm_state"] = df[state] if state else ""
    out["crm_zip"] = df[zipc] if zipc else ""
    out["crm_job_date"] = df[jdate] if jdate else ""
    return out.fillna("")

def run_pipeline(mail_csv_path: str, crm_csv_path: str) -> pd.DataFrame:
    # Read as text; don’t infer NaN (we handle “nan/none” in matcher)
    mail_raw = pd.read_csv(mail_csv_path, dtype=str, keep_default_na=False)
    crm_raw  = pd.read_csv(crm_csv_path,  dtype=str, keep_default_na=False)

    mail_std = _alias_mail(mail_raw)
    crm_std  = _alias_crm(crm_raw)

    matches = match_mail_to_crm(mail_std, crm_std)

    # Attach original indices (useful for dashboard/export)
    # If the upstream wants IDs, we’ll try to include if present
    mail_id_col = _find_col(mail_raw, "MailID", "ID", "Mail Id")
    crm_id_col  = _find_col(crm_raw, "CustomerID", "ID", "Cust Id")

    if mail_id_col:
        matches["MailID"] = mail_raw.iloc[matches["mail_idx"]][mail_id_col].values
    if crm_id_col:
        matches["CustomerID"] = crm_raw.iloc[matches["crm_idx"]][crm_id_col].values

    return matches
