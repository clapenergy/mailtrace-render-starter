from __future__ import annotations
import pandas as pd
from .matcher import run_matching
def run_pipeline(mail_csv: str, crm_csv: str) -> pd.DataFrame:
    mail_df = pd.read_csv(mail_csv, dtype=str)
    crm_df = pd.read_csv(crm_csv, dtype=str)
    summary = run_matching(mail_df, crm_df)
    return summary
