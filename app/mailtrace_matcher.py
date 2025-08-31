# mailtrace_matcher.py — “showcase” matcher with fuzzy handling + mail date aggregation
from __future__ import annotations
import re
from datetime import datetime, date
from difflib import SequenceMatcher
from typing import List, Tuple, Optional
import pandas as pd

# --- Normalization dictionaries ---
STREET_TYPES = {
    "street":"street","st":"street","st.":"street",
    "road":"road","rd":"road","rd.":"road",
    "avenue":"avenue","ave":"avenue","ave.":"avenue","av":"avenue","av.":"avenue",
    "boulevard":"boulevard","blvd":"boulevard","blvd.":"boulevard",
    "lane":"lane","ln":"lane","ln.":"lane",
    "drive":"drive","dr":"drive","dr.":"drive",
    "court":"court","ct":"court","ct.":"court",
    "circle":"circle","cir":"circle","cir.":"circle",
    "parkway":"parkway","pkwy":"parkway","pkwy.":"parkway","pkway":"parkway",
    "highway":"highway","hwy":"highway","hwy.":"highway",
    "terrace":"terrace","ter":"terrace","ter.":"terrace",
    "place":"place","pl":"place","pl.":"place",
    "way":"way","wy":"way","wy.":"way",
    "trail":"trail","trl":"trail","trl.":"trail",
    "alley":"alley","aly":"alley","aly.":"alley",
    "common":"common","cmn":"common","cmn.":"common",
    "park":"park"
}
DIRECTIONALS = {
    "n":"north","n.":"north","north":"north",
    "s":"south","s.":"south","south":"south",
    "e":"east","e.":"east","east":"east",
    "w":"west","w.":"west","west":"west",
    "ne":"northeast","ne.":"northeast",
    "nw":"northwest","nw.":"northwest",
    "se":"southeast","se.":"southeast",
    "sw":"southwest","sw.":"southwest",
}
UNIT_WORDS = {"apt","apartment","suite","ste","unit","#","bldg","floor","fl"}

def _squash_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _norm_token(tok: str) -> str:
    t = tok.lower().strip(".,")
    if t in STREET_TYPES: return STREET_TYPES[t]
    if t in DIRECTIONALS: return DIRECTIONALS[t]
    return t

def normalize_address1(s: str) -> str:
    """Lowercase, remove punctuation (keep '#'), expand abbrevs, unify spaces."""
    if not isinstance(s, str): return ""
    s = s.replace("-", " ")
    s = re.sub(r"[^\w#\s]", " ", s)
    parts = [_norm_token(p) for p in s.lower().split() if p.strip()]
    return _squash_ws(" ".join(parts))

def tokens(s: str) -> List[str]:
    return [t for t in normalize_address1(s).split() if t]

def street_type_of(tok_list: List[str]) -> Optional[str]:
    if not tok_list: return None
    last = tok_list[-1]
    return last if last in STREET_TYPES.values() else None

def directional_in(tok_list: List[str]) -> Optional[str]:
    for t in tok_list:
        if t in DIRECTIONALS.values():
            return t
    return None

# --- blocking by first word + first letter of second word (fast pre-filter)
def block_key(addr1: str) -> str:
    if not isinstance(addr1, str): return ""
    toks = [t for t in _squash_ws(addr1).split() if t]
    if not toks: return ""
    first = toks[0]
    second_initial = toks[1][0] if len(toks) > 1 else ""
    return f"{first}|{second_initial}".lower()

# --- date parsing (+ tolerant formats)
DATE_FORMATS = ["%Y-%m-%d","%m/%d/%Y","%d-%m-%Y","%Y/%m/%d","%m-%d-%Y","%d/%m/%Y"]
def parse_date_any(s: str) -> Optional[date]:
    if not isinstance(s, str) or not s.strip(): return None
    z = re.sub(r"[^\d/-]", "", s.strip()).replace("/", "-")
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(z, fmt).date()
        except Exception:
            continue
    return None

def fmt_dd_mm_yy(d: Optional[date]) -> str:
    return d.strftime("%d-%m-%y") if isinstance(d, date) else "None provided"

# --- amounts
def parse_amount(x) -> float:
    if x is None: return 0.0
    s = str(x)
    s = s.replace("$","").replace(",","").strip()
    try:
        return float(s)
    except Exception:
        return 0.0

# --- similarity
def _ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def address_similarity(a1: str, b1: str) -> float:
    na, nb = normalize_address1(a1), normalize_address1(b1)
    if not na or not nb: return 0.0
    return _ratio(na, nb)

def score_row(mail_row: pd.Series, crm_row: pd.Series) -> Tuple[int, List[str]]:
    a_mail = str(mail_row.get("address1", ""))
    a_crm  = str(crm_row.get("address1", ""))
    sim = address_similarity(a_mail, a_crm)
    score = int(round(sim * 100))

    # Postal code + city/state bonuses (capped 100)
    mz = str(mail_row.get("postal_code", "")).strip()
    cz = str(crm_row.get("postal_code", "")).strip()
    if mz[:5] and cz[:5] and mz[:5] == cz[:5]:
        score = min(100, score + 5)

    mcity = str(mail_row.get("city","")).strip().lower()
    ccity = str(crm_row.get("city","")).strip().lower()
    if mcity and ccity and mcity == ccity:
        score = min(100, score + 2)

    mstate = str(mail_row.get("state","")).strip().lower()
    cstate = str(crm_row.get("state","")).strip().lower()
    if mstate and cstate and mstate == cstate:
        score = min(100, score + 2)

    # Notes: street type, direction, unit, city/state diffs
    notes: List[str] = []
    ta, tb = tokens(a_crm), tokens(a_mail)
    st_a, st_b = street_type_of(ta), street_type_of(tb)
    if st_a != st_b and (st_a or st_b):
        notes.append(f"{st_b or 'none'} vs {st_a or 'none'} (street type)")

    dir_a, dir_b = directional_in(ta), directional_in(tb)
    if dir_a != dir_b and (dir_a or dir_b):
        notes.append(f"{dir_b or 'none'} vs {dir_a or 'none'} (direction)")

    unit_a = str(crm_row.get("address2", "") or "").strip()
    unit_b = str(mail_row.get("address2", "") or "").strip()
    if bool(unit_a) != bool(unit_b):
        # heavier hit than before to reflect your request
        score = max(0, score - 8)
        notes.append(f"{unit_b or 'none'} vs {unit_a or 'none'} (unit)")
    elif unit_a and unit_b and unit_a.lower() != unit_b.lower():
        score = max(0, score - 12)
        notes.append(f"{unit_b} vs {unit_a} (unit)")

    # explicit city/state note if differs
    if mcity and ccity and mcity != ccity:
        notes.append(f"{mail_row.get('city')} vs {crm_row.get('city')} (city)")
        score = min(score, 74)  # keep as noticeable

    if mstate and cstate and mstate != cstate:
        notes.append(f"{mail_row.get('state')} vs {crm_row.get('state')} (state)")
        score = min(score, 74)

    if score >= 100 and not notes:
        return 100, ["perfect match"]
    return max(0, min(100, score)), notes


# --- Canonicalize columns + run matching ---
def _canon_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    d = df.copy()
    d.columns = [c.lower().strip() for c in d.columns]
    for want, alts in mapping.items():
        if want in d.columns: 
            continue
        for a in alts:
            if a.lower() in d.columns:
                d.rename(columns={a.lower(): want}, inplace=True)
                break
    # ensure keys exist
    for key in mapping.keys():
        if key not in d.columns:
            d[key] = ""
    return d

def run_matching(mail_df: pd.DataFrame, crm_df: pd.DataFrame) -> pd.DataFrame:
    # Canonicalize
    mail_df = _canon_columns(mail_df, {
        "id": ["id","mailid","mail_id"],
        "address1": ["address1","addr1","address","street","line1"],
        "address2": ["address2","addr2","unit","line2","suite"],
        "city": ["city","town","mailcity"],
        "state": ["state","st","mailstate"],
        "postal_code": ["zip","zipcode","zip_code","postal_code","zip5"],
        "sent_date": ["sent_date","maildate","mailed","date","mail_date"]
    })
    crm_df = _canon_columns(crm_df, {
        "crm_id": ["crm_id","id","customerid","lead_id","job_id"],
        "address1": ["address1","addr1","address","street","line1"],
        "address2": ["address2","addr2","unit","line2","suite"],
        "city": ["city","town"],
        "state": ["state","st"],
        "postal_code": ["zip","zipcode","zip_code","postal_code","zip5"],
        "job_date": ["job_date","dateentered","created_at","date","jobdate"],
        "job_value": ["jobvalue","value","amount","revenue","total"]
    })

    # Parsed helpers
    mail_df["_blk"]  = mail_df["address1"].apply(block_key)
    crm_df["_blk"]   = crm_df["address1"].apply(block_key)
    mail_df["_date"] = mail_df["sent_date"].apply(parse_date_any)
    crm_df["_date"]  = crm_df["job_date"].apply(parse_date_any)
    crm_df["_amt"]   = crm_df["job_value"].apply(parse_amount)

    # Group mail by block for quick candidate fetch
    mail_groups = {k: g for k, g in mail_df.groupby("_blk")}

    rows: List[dict] = []
    for _, c in crm_df.iterrows():
        blk = c["_blk"]
        candidates = mail_groups.get(blk)
        if candidates is None or candidates.empty:
            continue

        # Only consider mail on/before CRM date if CRM has a date; else include all
        if c["_date"]:
            cand = candidates[(candidates["_date"].isna()) | (candidates["_date"] <= c["_date"])]
        else:
            cand = candidates
        if cand.empty:
            continue

        # Score each candidate and pick the best
        best = None
        best_score = -1
        best_notes: List[str] = []
        for _, m in cand.iterrows():
            s, notes = score_row(m, c)
            if s > best_score or (s == best_score and (m.get("_date") or date.min) < (best.get("_date") if best is not None else date.max)):
                best, best_score, best_notes = m, s, notes

        # Collect all prior mail dates (sorted)
        prior_dates = []
        for _, m in cand.iterrows():
            d = m.get("_date")
            if isinstance(d, date):
                prior_dates.append(d)
        prior_sorted = sorted(prior_dates)
        mail_dates_list = ", ".join(fmt_dd_mm_yy(d) for d in prior_sorted) if prior_sorted else ""

        # Build full mail address “Street, Unit” pattern (unit after street, with comma)
        mail_addr1 = str(best.get("address1","")).strip()
        mail_unit  = str(best.get("address2","")).strip()
        mail_full_street = f"{mail_addr1}{', ' + mail_unit if mail_unit else ''}"

        rows.append({
            "mail_dates": mail_dates_list,                          # LEFTMOST in table
            "crm_date": fmt_dd_mm_yy(c.get("_date")),
            "amount": c.get("_amt", 0.0),
            "mail_address1": mail_full_street,
            "mail_city_state_zip": f"{best.get('city','')}, {best.get('state','')} {str(best.get('postal_code',''))}".replace(" ,", ",").replace("  ", " ").strip().strip(","),
            "crm_address1": str(c.get("address1","")).strip() + (f", {str(c.get('address2','')).strip()}" if str(c.get('address2','')).strip() else ""),
            "crm_city_state_zip": f"{c.get('city','')}, {c.get('state','')} {str(c.get('postal_code',''))}".replace(" ,", ",").replace("  ", " ").strip().strip(","),
            "confidence": int(best_score),
            "match_notes": "; ".join(best_notes) if best_notes else "perfect match",
            # for KPIs
            "_crm_city": c.get("city",""),
            "_crm_state": c.get("state",""),
            "_crm_zip5": str(c.get("postal_code",""))[:5] if c.get("postal_code","") else "",
        })

    df = pd.DataFrame(rows)

    # Sort for the summary: newest CRM date first (fallback to empty)
    # Convert crm_date back to parseable for sorting
    if not df.empty:
        def _parse_sortable(d):
            return parse_date_any(d) or date(1900,1,1)
        df["_sort_dt"] = df["crm_date"].map(_parse_sortable)
        df = df.sort_values("_sort_dt", ascending=False).drop(columns=["_sort_dt"])

    return df
