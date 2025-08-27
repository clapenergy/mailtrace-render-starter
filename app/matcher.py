from __future__ import annotations
import pandas as pd, numpy as np
from difflib import SequenceMatcher
from datetime import datetime
from .normalize import normalize_address1, block_key, tokens, street_type_of, directional_in, UNIT_WORDS
DATE_FORMATS = ["%Y-%m-%d","%m/%d/%Y","%m-%d-%Y","%d-%m-%Y","%Y/%m/%d","%m/%d/%y","%d-%m-%y"]
def parse_date_any(s: str):
    if not isinstance(s, str) or s.strip()=="": return None
    z = s.strip().replace("/", "-")
    for fmt in DATE_FORMATS:
        try: 
            return datetime.strptime(z, fmt).date()
        except: 
            continue
    return None
def ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()
def address_similarity(a1: str, b1: str) -> float:
    na = normalize_address1(a1)
    nb = normalize_address1(b1)
    if not na or not nb:
        return 0.0
    return ratio(na, nb)
def score_row(mail_row, crm_row) -> tuple[int, list[str]]:
    a_mail = str(mail_row.get("address1",""))
    a_crm  = str(crm_row.get("address1",""))
    sim = address_similarity(a_mail, a_crm)
    score = int(round(sim * 100))
    if str(mail_row.get("postal_code","")).strip()[:5] and str(crm_row.get("postal_code","")).strip()[:5]:
        if str(mail_row["postal_code"]).strip()[:5] == str(crm_row["postal_code"]).strip()[:5]:
            score = min(100, score + 5)
    if str(mail_row.get("city","")).strip().lower() == str(crm_row.get("city","")).strip().lower():
        score = min(100, score + 2)
    if str(mail_row.get("state","")).strip().lower() == str(crm_row.get("state","")).strip().lower():
        score = min(100, score + 2)
    notes = []
    ta = tokens(a_crm); tb = tokens(a_mail)
    st_a = street_type_of(ta); st_b = street_type_of(tb)
    if st_a != st_b:
        if st_a or st_b:
            notes.append(f"{st_b or 'none'} vs {st_a or 'none'} (street type)")
    dir_a = directional_in(ta); dir_b = directional_in(tb)
    if dir_a != dir_b:
        if dir_a or dir_b:
            notes.append(f"{dir_b or 'none'} vs {dir_a or 'none'} (direction)")
    unit_a = str(crm_row.get('address2',"") or "")
    unit_b = str(mail_row.get('address2',"") or "")
    if bool(unit_a.strip()) != bool(unit_b.strip()):
        if unit_b.strip() and not unit_a.strip():
            notes.append(f"{unit_b.strip()} vs none (unit)")
        elif unit_a.strip() and not unit_b.strip():
            notes.append(f"none vs {unit_a.strip()} (unit)")
    elif unit_a.strip() and unit_b.strip() and unit_a.strip().lower()!=unit_b.strip().lower():
        notes.append(f"{unit_b.strip()} vs {unit_a.strip()} (unit)")
    if score >= 100 and not notes:
        return (100, ["perfect match"])
    return (min(100, score), notes)
def run_matching(mail_df: pd.DataFrame, crm_df: pd.DataFrame) -> pd.DataFrame:
    def canon(df, mapping):
        d = df.copy()
        d.columns = [c.lower().strip() for c in d.columns]
        for want, alts in mapping.items():
            if want in d.columns: continue
            for a in alts:
                if a in d.columns:
                    d.rename(columns={a: want}, inplace=True)
                    break
        for key in mapping.keys():
            if key not in d.columns: d[key] = ""
        return d
    mail_df = canon(mail_df, {
        "id": ["id","mail_id"],
        "address1": ["address1","addr1","address","street","line1"],
        "address2": ["address2","addr2","unit","line2"],
        "city": ["city","town"],
        "state": ["state","st"],
        "postal_code": ["postal_code","zip","zipcode","zip_code"],
        "sent_date": ["sent_date","date","mail_date"]
    })
    crm_df = canon(crm_df, {
        "crm_id": ["crm_id","id","lead_id","job_id"],
        "address1": ["address1","addr1","address","street","line1"],
        "address2": ["address2","addr2","unit","line2"],
        "city": ["city","town"],
        "state": ["state","st"],
        "postal_code": ["postal_code","zip","zipcode","zip_code"],
        "job_date": ["job_date","date","created_at"]
    })
    mail_df["_blk"] = mail_df["address1"].apply(block_key)
    crm_df["_blk"] = crm_df["address1"].apply(block_key)
    mail_df["_date"] = mail_df["sent_date"].apply(parse_date_any)
    crm_df["_date"] = crm_df["job_date"].apply(parse_date_any)
    mail_groups = {k:g for k,g in mail_df.groupby("_blk")}
    rows = []
    from datetime import datetime as _dt
    for _, c in crm_df.iterrows():
        blk = c["_blk"]
        candidates = mail_groups.get(blk, None)
        if candidates is None or len(candidates)==0:
            continue
        if c["_date"]:
            cand = candidates[(candidates["_date"].isna()) | (candidates["_date"] <= c["_date"])]
        else:
            cand = candidates
        if cand.empty:
            continue
        best = None; best_score = -1; best_notes = []
        for _, m in cand.iterrows():
            s, notes = score_row(m, c)
            if s > best_score or (s==best_score and (m.get("_date") or _dt.min.date()) < (best.get("_date") if best is not None else _dt.max.date())):
                best = m; best_score = s; best_notes = notes
        dates = []
        for _, m in cand.iterrows():
            d = m.get("_date")
            dates.append(d if d else None)
        def fmt_short(d): return d.strftime("%d-%m-%y") if d else None
        dates_sorted = sorted([d for d in dates if d is not None])
        mail_dates_list = ", ".join(fmt_short(d) for d in dates_sorted) if dates_sorted else "None provided"
        full_mail = " ".join([str(best.get("address1","")).strip(),
                              (str(best.get("address2","")).strip() or ""),
                              str(best.get("city","")).strip(),
                              str(best.get("state","")).strip(),
                              str(best.get("postal_code","")).strip()]).replace("  ", " ").strip()
        out = {
            "crm_id": c.get("crm_id",""),
            "crm_address1_original": c.get("address1",""),
            "crm_address2_original": (c.get("address2","") or ""),
            "crm_city": c.get("city",""),
            "crm_state": c.get("state",""),
            "crm_zip": str(c.get("postal_code","")),
            "crm_job_date": (c["_date"].strftime("%d-%m-%y") if c["_date"] else "None provided"),
            "matched_mail_id": best.get("id",""),
            "matched_mail_full_address": full_mail.replace(" None", "").replace(" none", ""),
            "mail_dates_in_window": mail_dates_list,
            "mail_count_in_window": len(dates_sorted),
            "confidence_percent": int(best_score),
            "match_notes": ("; ".join(best_notes) if best_notes else "perfect match"),
        }
        rows.append(out)
    return pd.DataFrame(rows)
