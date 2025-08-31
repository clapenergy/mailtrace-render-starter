# app/pipeline.py
# MailTrace pipeline – glue code between uploads and dashboard
# - Reads both CSVs as strings (no NaN coercion)
# - Calls your matcher (app.mailtrace_matcher.run_matching)
# - Returns the summary DataFrame expected by dashboard_export

from __future__ import annotations
import pandas as pd

# Use your fuzzy matcher with unit penalties & geo notes
# (produces: crm_job_date, crm_amount, matched_mail_full_address,
#  mail_dates_in_window, mail_count_in_window, confidence_percent, match_notes, etc.)
from app.mailtrace_matcher import run_matching  # noqa: F401


def run_pipeline(mail_csv_path: str, crm_csv_path: str) -> pd.DataFrame:
    """
    Read the uploaded CSVs, run matching, and return a normalized summary DataFrame
    that dashboard_export.finalize_summary_for_export_v17 can consume.
    """
    # Read strictly as text so weird values don’t become NaN
    mail_raw = pd.read_csv(mail_csv_path, dtype=str, keep_default_na=False)
    crm_raw = pd.read_csv(crm_csv_path, dtype=str, keep_default_na=False)

    # Your matcher internally canonicalizes headers (Address vs Street, Zip vs Postal, etc.)
    # and returns the summary table with the columns the dashboard expects.
    summary = run_matching(mail_raw, crm_raw)

    # Safety: ensure these columns exist so the renderer never breaks even if a CSV lacked data.
    for col, default in [
        ("crm_job_date", ""),
        ("crm_amount", ""),
        ("matched_mail_full_address", ""),
        ("mail_dates_in_window", ""),
        ("mail_count_in_window", 0),
        ("confidence_percent", 0),
        ("match_notes", ""),
        ("crm_city", ""),
        ("crm_state", ""),
        ("crm_zip", ""),
        ("crm_address1_original", ""),
        ("crm_address2_original", ""),
    ]:
        if col not in summary.columns:
            summary[col] = default

    # All done — app.py will pass this to finalize_summary_for_export_v17() and then render_full_dashboard_v17()
    return summary
