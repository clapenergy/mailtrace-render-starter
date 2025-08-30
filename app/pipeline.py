# app/pipeline.py
# Reads the two CSVs, normalizes headers, runs v17 matching,
# and RETURNS a matches DataFrame that includes `crm_amount`
# (mapped from CRM columns like JobValue/Amount/Revenue).

from __future__ import annotations
import pandas as pd
from typing import Dict

# Robust import for matching core
try:
    from app.matching_logic_v17 import match_mail_to_crm
except Exception:
    from .matching_logic_v17 import match_mail_to_crm


# -------- helpers --------

def _find_col(df: pd.DataFrame, *candidates: str) -> str | None:
    """Return first matching column name (case-insensitive)."""
    if df is None or df.empty:
        return None
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        name = lower.get(cand.lower())
        if name:
            return name
    return None


# -------- aliasing (normalize incoming headers to what the matcher expects) --------

def _alias_mail(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["address1"]  = df[_find_col(df, "Address1", "Address")] if _find_col(df, "Address1", "Address") else ""
    out["address2"]  = df[_find_col(df, "Address2")] if _find_col(df, "Address2") else ""
    out["city"]      = df[_find_col(df, "City", "MailCity")] if _find_col(df, "City", "MailCity") else ""
    out["state"]     = df[_find_col(df, "State", "MailState")] if _find_col(df, "State", "MailState") else ""
    out["zip"]       = df[_find_col(df, "Zip", "Zip5", "PostalCode")] if _find_col(df, "Zip", "Zip5", "PostalCode") else ""
    out["mail_date"] = df[_find_col(df, "MailDate", "Mailed", "Date", "Mail Date")] if _find_col(df, "MailDate", "Mailed", "Date", "Mail Date") else ""
    return out.fillna("")


def _alias_crm(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["crm_address1"]  = df[_find_col(df, "Address1", "Address")] if _find_col(df, "Address1", "Address") else ""
    out["crm_address2"]  = df[_find_col(df, "Address2")] if _find_col(df, "Address2") else ""
    out["crm_city"]      = df[_find_col(df, "City")] if _find_col(df, "City") else ""
    out["crm_state"]     = df[_find_col(df, "State")] if _find_col(df, "State") else ""
    out["crm_zip"]       = df[_find_col(df, "Zip", "Zip5", "PostalCode")] if _find_col(df, "Zip", "Zip5", "PostalCode") else ""
    out["crm_job_date"]  = df[_find_col(df, "DateEntered", "JobDate", "Date", "CreatedDate")] if _find_col(df, "DateEntered", "JobDate", "Date", "CreatedDate") else ""
    # Map job value-like columns to the exact name the dashboard expects: `crm_amount`
    amt_col = _find_col(df, "JobValue", "Amount", "Revenue", "Job Value", "Job_Value")
    out["crm_amount"]    = df[amt_col].astype(str) if amt_col else ""
    return out.fillna("")


# -------- pipeline entry --------

def run_pipeline(mail_csv_path: str, crm_csv_path: str) -> pd.DataFrame:
    """
    Reads CSVs, applies header aliases, runs matching, and returns the matches
    including crm_amount + optional IDs if present.
    """
    # Read raw as text; we handle 'nan' downstream explicitly
    mail_raw = pd.read_csv(mail_csv_path, dtype=str, keep_default_na=False)
    crm_raw  = pd.read_csv(crm_csv_path,  dtype=str, keep_default_na=False)

    mail_std = _alias_mail(mail_raw)
    crm_std  = _alias_crm(crm_raw)

    # Run v17 matching (expects normalized columns from aliasers)
    matches = match_mail_to_crm(mail_std, crm_std)

    # Attach optional IDs (if user provided)
    mail_id_col = _find_col(mail_raw, "MailID", "ID", "Mail Id")
    crm_id_col  = _find_col(crm_raw, "CustomerID", "ID", "Cust Id", "Customer Id")

    if mail_id_col:
        matches["MailID"] = mail_raw.iloc[matches["mail_idx"]][mail_id_col].values
    if crm_id_col:
        matches["CustomerID"] = crm_raw.iloc[matches["crm_idx"]][crm_id_col].values

    # Attach CRM amount so dashboard table "Amount" fills in
    if "crm_amount" in crm_std.columns:
        matches["crm_amount"] = crm_std.iloc[matches["crm_idx"]]["crm_amount"].values
    else:
        matches["crm_amount"] = ""

    # Ensure strings (prevents NaN showing up)
    for c in ["crm_amount", "match_notes"]:
        if c in matches.columns:
            matches[c] = matches[c].fillna("").astype(str)

    return matches
