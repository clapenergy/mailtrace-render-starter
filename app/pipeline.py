# app/pipeline.py
import pandas as pd
from typing import Dict

# robust import for matching logic
try:
    from app.matching_logic_v17 import match_mail_to_crm
except ImportError:
    from .matching_logic_v17 import match_mail_to_crm


# --- alias maps ---
MAIL_ALIASES: Dict[str, str] = {
    "address1": "address",
    "address2": "address2",
    "city": "city",
    "state": "state",
    "zip": "zip",
    "mail_date": "maildate",
}
CRM_ALIASES: Dict[str, str] = {
    "crm_address1": "address",
    "crm_address2": "address2",
    "crm_city": "city",
    "crm_state": "state",
    "crm_zip": "zip",
    "crm_job_date": "dateentered",
    "JobValue": "jobvalue",  # NEW: carry job value
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
    out["address1"] = df[_find_col(df, "Address1", "Address")] if _find_col(df, "Address1", "Address") else ""
    out["address2"] = df[_find_col(df, "Address2")] if _find_col(df, "Address2") else ""
    out["city"]     = df[_find_col(df, "City")] if _find_col(df, "City") else ""
    out["state"]    = df[_find_col(df, "State")] if _find_col(df, "State") else ""
    out["zip"]      = df[_find_col(df, "Zip", "Zip5", "PostalCode")] if _find_col(df, "Zip", "Zip5", "PostalCode") else ""
    out["mail_date"]= df[_find_col(df, "MailDate", "Mailed", "Date", "Mail Date")] if _find_col(df, "MailDate", "Mailed", "Date", "Mail Date") else ""
    return out.fillna("")


def _alias_crm(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["crm_address1"] = df[_find_col(df, "Address1", "Address")] if _find_col(df, "Address1", "Address") else ""
    out["crm_address2"] = df[_find_col(df, "Address2")] if _find_col(df, "Address2") else ""
    out["crm_city"]     = df[_find_col(df, "City")] if _find_col(df, "City") else ""
    out["crm_state"]    = df[_find_col(df, "State")] if _find_col(df, "State") else ""
    out["crm_zip"]      = df[_find_col(df, "Zip", "Zip5", "PostalCode")] if _find_col(df, "Zip", "Zip5", "PostalCode") else ""
    out["crm_job_date"] = df[_find_col(df, "DateEntered", "JobDate", "Date", "CreatedDate")] if _find_col(df, "DateEntered", "JobDate", "Date", "CreatedDate") else ""
    # NEW: JobValue
    out["JobValue"]     = df[_find_col(df, "JobValue", "Amount", "Revenue")] if _find_col(df, "JobValue", "Amount", "Revenue") else 0
    return out.fillna("")


def run_pipeline(mail_csv_path: str, crm_csv_path: str) -> pd.DataFrame:
    # Read CSVs
    mail_raw = pd.read_csv(mail_csv_path, dtype=str, keep_default_na=False)
    crm_raw  = pd.read_csv(crm_csv_path, dtype=str, keep_default_na=False)

    mail_std = _alias_mail(mail_raw)
    crm_std  = _alias_crm(crm_raw)

    matches = match_mail_to_crm(mail_std, crm_std)

    # attach IDs if present
    mail_id_col = _find_col(mail_raw, "MailID", "ID", "Mail Id")
    crm_id_col  = _find_col(crm_raw, "CustomerID", "ID", "Cust Id")

    if mail_id_col:
        matches["MailID"] = mail_raw.iloc[matches["mail_idx"]][mail_id_col].values
    if crm_id_col:
        matches["CustomerID"] = crm_raw.iloc[matches["crm_idx"]][crm_id_col].values

    # carry JobValue into matches
    if "JobValue" in crm_std.columns:
        matches["JobValue"] = crm_std.iloc[matches["crm_idx"]]["JobValue"].values

    return matches
