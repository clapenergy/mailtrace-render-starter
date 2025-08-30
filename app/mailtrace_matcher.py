# app/mailtrace_matcher.py
# Dashboard-v4: fuzzy matching with explicit unit penalties baked into confidence
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
UNIT_WORDS = {"apt","apartment","suite","ste","unit","#","bldg","fl","floor"}

# --- Helpers ---
def _squash_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _norm_token(tok: str) -> str:
    t = tok.lower().strip(".,")
    if t in STREET_TYPES: return STREET_TYPES[t]
    if t in DIRECTIONALS: return DIRECTIONALS[t]
    return t

def normalize_address1(s: str) -> str:
    """Lowercase, remove punctuation (keep '#'), expand common abbrevs, unify spaces."""
    if not isinstance(s, str): return ""
    s = s.replace("-", " ")  # treat hyphens as spaces
    s = re.sub(r"[^\w#\s]", " ", s)  # strip punctuation except '#'
    parts = [_norm_token(p) for p in s.lower().split() if p.strip()]
    return _squash_ws(" ".join(parts))

def block_key(addr1: str) -> str:
    """Blocking by first word + first letter of second word (fast pre-filter)."""
    if not isinstance(addr1, str): return ""
    toks = [t for t in _squash_ws(addr1).split() if t]
    if not toks: return ""
    first = toks[0]
    second_initial = toks[1][0] if len(toks) > 1 else ""
    return f"{first}|{second_initial}".lower()

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

# --- Dates ---
DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y",
    "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%y", "%d-%m-%y"
]

def parse_date_any(s: str) -> Optional[date]:
    if not isinstance(s, str) or not s.strip(): return None
    z = s.strip().replace("/", "-")  # unify / and -
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(z, fmt).date()
        except Exception:
            continue
    return None

def fmt_dd_mm_yy(d: Optional[date]) -> str:
    return d.strftime("%d-%m-%y") if isinstance(d, date) else "None provided"

# --- Unit normalization ---
_UNIT_EXTRACT = re.compile(
    r"(?:^|[\s,.-])(?:(apt|apartment|suite|ste|unit|bldg|fl|floor)\s*#?\s*([\w-]+)|#\s*([\w-]+))\s*$",
    re.IGNORECASE,
)

def normalize_unit_text(u: str) -> tuple[str, str]:
    """
    Returns (label, number) normalized.
    Examples:
      "Apt 2" -> ("unit", "2")
      "STE B" -> ("unit", "b")
      "#4"    -> ("unit", "4")
    """
    if not isinstance(u, str) or not u.strip():
        return ("", "")
    s = u.strip()
    m = _UNIT_EXTRACT.search(s)
    if m:
        label = (m.group(1) or "").lower()
        num = (m.group(2) or m.group(3) or "").strip().lower()
        if label in ("apt","apartment","suite","ste","unit","bldg","fl","floor") or label == "":
            label = "unit"  # normalize all to 'unit'
        return (label, num)
    # Nothing matched; fall back to basic parse like "2B", "B-2"
    txt = re.sub(r"[^A-Za-z0-9-]", "", s).lower()
    if txt:
        return ("unit", txt)
    return ("", "")

# --- Scoring ---
def _ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def address_similarity(a1: str, b1: str) -> float:
    na, nb = normalize_address1(a1), normalize_address1(b1)
    if not na or not nb: return 0.0
    return _ratio(na, nb)

def score_row(mail_row: pd.Series, crm_row: pd.Series) -> Tuple[int, List[str]]:
    """Return (confidence_0_100, mismatch_notes[]) for a mail ↔ CRM candidate pair."""
    a_mail = str(mail_row.get("address1", ""))
    a_crm  = str(crm_row.get("address1", ""))
    sim = address_similarity(a_mail, a_crm)      # 0..1
    score = int(round(sim * 100))                # 0..100 base

    # Bonuses for geo (capped at 100)
    mz = str(mail_row.get("postal_code", "")).strip()
    cz = str(crm_row.get("postal_code", "")).strip()
    if mz[:5] and cz[:5] and mz[:5] == cz[:5]:
        score = min(100, score + 5)

    if str(mail_row.get("city", "")).strip().lower() == str(crm_row.get("city", "")).strip().lower():
        score = min(100, score + 2)

    if str(mail_row.get("state", "")).strip().lower() == str(crm_row.get("state", "")).strip().lower():
        score = min(100, score + 2)

    notes: List[str] = []

    # Street-type note (kept as note only; similarity usually captures this)
    ta, tb = tokens(a_crm), tokens(a_mail)
    st_a, st_b = street_type_of(ta), street_type_of(tb)
    if st_a != st_b and (st_a or st_b):
        notes.append(f"{st_b or 'none'} vs {st_a or 'none'} (street type)")

    # Directional note (e.g., N, S)
    dir_a, dir_b = directional_in(ta), directional_in(tb)
    if dir_a != dir_b and (dir_a or dir_b):
        notes.append(f"{dir_b or 'none'} vs {dir_a or 'none'} (direction)")

    # ---- NEW: Unit penalties that affect score ----
    unit_a = str(crm_row.get("address2", "") or "").strip()
    unit_b = str(mail_row.get("address2", "") or "").strip()
    lab_a, num_a = normalize_unit_text(unit_a)
    lab_b, num_b = normalize_unit_text(unit_b)

    if not num_a and not num_b:
        # no units on either side -> no penalty
        pass
    elif (num_a and not num_b) or (num_b and not num_a):
        # one side has a unit, the other does not -> medium penalty
        score = max(0, score - 10)
        notes.append(f"{unit_b or 'none'} vs {unit_a or 'none'} (unit)")
    else:
        # both sides have a unit number; compare normalized numbers (labels don't matter)
        if num_a != num_b:
            score = max(0, score - 18)  # significant penalty
            notes.append(f"{unit_b or 'none'} vs {unit_a or 'none'} (unit)")
        else:
            # numbers match -> no penalty; keep note only if labels are wildly different (optional)
            if lab_a != lab_b and (lab_a or lab_b):
                # You can comment this out if you don't want a note for label differences
                notes.append(f"{(lab_b + ' ' + num_b).strip()} vs {(lab_a + ' ' + num_a).strip()} (unit label)")

    # Cap score 0..100
    score = max(0, min(100, score))

    if score >= 100 and not notes:
        return 100, ["perfect match"]
    return score, notes

# --- Canonicalize columns and run matching ---
def _canon_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    d = df.copy()
    d.columns = [c.lower().strip() for c in d.columns]
    for want, alts in mapping.items():
        if want in d.columns: continue
        for a in alts:
            if a in d.columns:
                d.rename(columns={a: want}, inplace=True)
                break
    for key in mapping.keys():
        if key not in d.columns:
            d[key] = ""
    return d

def run_matching(mail_df: pd.DataFrame, crm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns one row per CRM record with:
      - original CRM fields
      - best-match mail (full address)
      - list of all prior mail dates (dd-mm-yy), count
      - confidence_percent (0..100, capped)
      - match_notes ("perfect match" or concise diffs)
    """
    # Canonicalize columns
    mail_df = _canon_columns(mail_df, {
        "id": ["id","mail_id"],
        "address1": ["address1","addr1","address","street","line1"],
        "address2": ["address2","addr2","unit","line2"],
        "city": ["city","town"],
        "state": ["state","st"],
        "postal_code": ["postal_code","zip","zipcode","zip_code"],
        "sent_date": ["sent_date","date","mail_date"],
    })
    crm_df = _canon_columns(crm_df, {
        "crm_id": ["crm_id","id","lead_id","job_id"],
        "address1": ["address1","addr1","address","street","line1"],
        "address2": ["address2","addr2","unit","line2"],
        "city": ["city","town"],
        "state": ["state","st"],
        "postal_code": ["postal_code","zip","zipcode","zip_code"],
        "job_date": ["job_date","date","created_at"],
        "amount": ["amount","value","job_value","revenue"],
    })

    # Blocking keys + parsed dates
    mail_df["_blk"]  = mail_df["address1"].apply(block_key)
    crm_df["_blk"]   = crm_df["address1"].apply(block_key)
    mail_df["_date"] = mail_df["sent_date"].apply(parse_date_any)
    crm_df["_date"]  = crm_df["job_date"].apply(parse_date_any)

    # Group mail by block
    mail_groups = {k: g for k, g in mail_df.groupby("_blk")}

    out_rows: List[dict] = []
    for _, c in crm_df.iterrows():
        blk = c["_blk"]
        candidates = mail_groups.get(blk)
        if candidates is None or candidates.empty:
            continue

        # Date window: include mail with date <= CRM date (if CRM date known)
        if c["_date"]:
            cand = candidates[(candidates["_date"].isna()) | (candidates["_date"] <= c["_date"])]
        else:
            cand = candidates
        if cand.empty:
            continue

        # Score & pick best; tie-breaker = earliest mail date
        best = None
        best_score = -1
        best_notes: List[str] = []
        for _, m in cand.iterrows():
            s, notes = score_row(m, c)
            if s > best_score or (s == best_score and (m.get("_date") or date.min) < (best.get("_date") if best is not None else date.max)):
                best, best_score, best_notes = m, s, notes

        # Collect all prior mail dates (sorted oldest→newest)
        prior_dates = []
        for _, m in cand.iterrows():
            d = m.get("_date")
            prior_dates.append(d if isinstance(d, date) else None)
        prior_sorted = [d for d in sorted([d for d in prior_dates if d is not None])]
        mail_dates_list = ", ".join(fmt_dd_mm_yy(d) for d in prior_sorted) if prior_sorted else "None provided"

        # Build full mail address (original form; blank addr2 if missing)
        full_mail = " ".join([
            str(best.get("address1", "")).strip(),
            (str(best.get("address2", "")).strip() or ""),
            str(best.get("city", "")).strip(),
            str(best.get("state", "")).strip(),
            str(best.get("postal_code", "")).strip()
        ]).replace("  ", " ").strip()

        out_rows.append({
            "crm_id": c.get("crm_id", ""),
            "crm_address1_original": c.get("address1", ""),
            "crm_address2_original": (c.get("address2", "") or ""),
            "crm_city": c.get("city", ""),
            "crm_state": c.get("state", ""),
            "crm_zip": str(c.get("postal_code", "")),
            "crm_job_date": fmt_dd_mm_yy(c.get("_date")),
            "crm_amount": c.get("amount", ""),
            "matched_mail_id": best.get("id", ""),
            "matched_mail_full_address": full_mail.replace(" None", "").replace(" none", ""),
            "mail_dates_in_window": mail_dates_list,
            "mail_count_in_window": len(prior_sorted),
            "confidence_percent": int(best_score),
            "match_notes": ("; ".join(best_notes) if best_notes else "perfect match"),
        })

    return pd.DataFrame(out_rows)

# --- Optional convenience: run directly on two CSV paths ---
def run_matching_from_csv(mail_csv: str, crm_csv: str) -> pd.DataFrame:
    mail = pd.read_csv(mail_csv, dtype=str)
    crm  = pd.read_csv(crm_csv, dtype=str)
    return run_matching(mail, crm)

if __name__ == "__main__":
    # Minimal CLI behavior when run directly:
    import argparse
    p = argparse.ArgumentParser(description="MailTrace matching logic (summary CSV to stdout).")
    p.add_argument("--mail", required=True, help="Path to mail CSV")
    p.add_argument("--crm", required=True, help="Path to CRM CSV")
    p.add_argument("--out", default="", help="Optional: write summary CSV to this path")
    args = p.parse_args()

    df = run_matching_from_csv(args.mail, args.crm)
    if args.out:
        df.to_csv(args.out, index=False)
        print(f"[ok] wrote {args.out}")
    else:
        print(df.head().to_string(index=False))
