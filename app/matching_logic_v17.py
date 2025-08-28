# matching_logic_v17.py
# MailTrace v17 — matching core (standalone)
# ------------------------------------------------------------
# - Blocking: ZIP + address1_stem + month(date)
# - Street type normalization (blvd -> boulevard, trl -> trail, etc.)
# - Unit handling penalties as agreed
# - City/state/zip must align (normalized)
# - Confidence buckets: >=94, 88–94, <88
# - Notes: uses "none" only inside match_notes (never changes your data)
#
# Usage:
#   from matching_logic_v17 import match_mail_to_crm
#   matches = match_mail_to_crm(mail_df, crm_df)
#   # matches columns include: mail_idx, crm_idx, confidence, bucket, match_notes, ...
#

from __future__ import annotations
import re
import pandas as pd
from datetime import datetime

# --------------------------
# Normalization helpers
# --------------------------
_ST_TYPE_MAP = {
    "st": "street", "street": "street",
    "ave": "avenue", "av": "avenue", "avenue": "avenue",
    "blvd": "boulevard", "boul": "boulevard", "boulevard": "boulevard",
    "rd": "road", "road": "road",
    "dr": "drive", "drive": "drive",
    "ln": "lane", "lane": "lane",
    "trl": "trail", "trl.": "trail", "trail": "trail",
    "ter": "terrace", "terr": "terrace", "terrace": "terrace",
    "cir": "circle", "circle": "circle",
    "ct": "court", "court": "court",
    "pl": "place", "place": "place",
    "pkwy": "parkway", "parkway": "parkway",
    "hwy": "highway", "highway": "highway",
}

_UNIT_HINTS = ("unit", "apt", "suite", "ste", "bldg", "fl", "floor", "#")

_WS = re.compile(r"\s+")
_NON_ALNUM = re.compile(r"[^a-z0-9]")
_LEADING_NUM = re.compile(r"^\d+")
_MONTH_FMT = "%Y-%m"  # matching month key

def _lower_strip(x: str) -> str:
    return _WS.sub(" ", str(x).strip().lower())

def _nan_like(x) -> bool:
    if x is None: return True
    s = str(x).strip().lower()
    return s == "" or s == "nan" or s == "none"

def normalize_unit(s: str | None) -> str:
    if _nan_like(s): return ""
    s = _lower_strip(s)
    # Strip leading markers like "unit", "apt", "suite", "#" etc.
    s = s.replace("#", " # ")
    tokens = [t for t in _WS.split(s) if t]
    # drop tokens like 'unit','apt','suite'
    cleaned = [t for t in tokens if t not in _UNIT_HINTS]
    return " ".join(cleaned)

def normalize_city(s: str | None) -> str:
    if _nan_like(s): return ""
    return _WS.sub(" ", _NON_ALNUM.sub(" ", s.lower())).strip()

def normalize_state(s: str | None) -> str:
    if _nan_like(s): return ""
    return re.sub(r"[^a-z]", "", s.lower())

def normalize_zip(s: str | None) -> str:
    if _nan_like(s): return ""
    # keep 5-digit for blocking; drop +4
    digits = re.sub(r"[^0-9]", "", str(s))
    return digits[:5]

def _split_address_tokens(addr: str) -> list[str]:
    # Keep alnum tokens, strip punctuation
    return [t for t in _WS.split(_NON_ALNUM.sub(" ", addr.lower())) if t]

def normalize_address1(addr: str | None) -> dict:
    """
    Returns:
      {
        'orig': original string or '',
        'house_num': '123',
        'name_tokens': ['main'],
        'street_type': 'street' (normalized) or ''
        'stem': '123 main'  (house number + name tokens only)
      }
    """
    result = {"orig": "", "house_num": "", "name_tokens": [], "street_type": "", "stem": ""}
    if _nan_like(addr):
        return result
    addr = _lower_strip(addr)
    tokens = _split_address_tokens(addr)
    if not tokens:
        return result

    # house number = leading numeric token (if present)
    if _LEADING_NUM.match(tokens[0]):
        house_num = tokens[0]
        rest = tokens[1:]
    else:
        house_num = ""
        rest = tokens[:]

    st_type = ""
    if rest:
        last = rest[-1]
        if last in _ST_TYPE_MAP:
            st_type = _ST_TYPE_MAP[last]
            rest = rest[:-1]

    result["orig"] = addr
    result["house_num"] = house_num
    result["name_tokens"] = rest
    result["street_type"] = st_type
    # stem = house_num + name tokens (no street type)
    name_part = " ".join(rest)
    stem = " ".join([x for x in [house_num, name_part] if x])
    result["stem"] = stem
    return result

def parse_date_to_month(s: str | None) -> str:
    """
    Accepts various date formats; returns YYYY-MM month key for blocking.
    We accept dd-mm-yy, mm/dd/yy, yyyy-mm-dd, etc.
    """
    if _nan_like(s): return ""
    s = str(s).strip()
    # Try common formats
    fmts = ["%d-%m-%y", "%d-%m-%Y", "%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%Y/%m/%d"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime(_MONTH_FMT)
        except ValueError:
            continue
    # Fallback: try pandas parser
    try:
        dt = pd.to_datetime(s, errors="coerce", utc=False)
        if pd.isna(dt): return ""
        if hasattr(dt, "to_pydatetime"):
            dt = dt.to_pydatetime()
        return dt.strftime(_MONTH_FMT)
    except Exception:
        return ""

# --------------------------
# Scoring & notes
# --------------------------
def _compare_street_type(m_type: str, c_type: str) -> tuple[int, str]:
    if m_type == "" and c_type == "":
        return 0, ""
    if m_type == c_type:
        return 0, ""
    # penalty if types differ, but only if stems matched already
    return -6, f"{c_type or 'none'} vs {m_type or 'none'} (street type)"

def _compare_unit(m_unit: str, c_unit: str) -> tuple[int, str]:
    mu = normalize_unit(m_unit)
    cu = normalize_unit(c_unit)
    if mu == "" and cu == "":
        return 0, ""  # explicitly no penalty
    if mu == "" and cu != "":
        return -8, f"{cu} vs none (unit)"
    if mu != "" and cu == "":
        return -8, f"{'none'} vs {mu} (unit)"
    if mu == cu:
        return 0, ""
    # both present but different
    return -20, f"{cu} vs {mu} (unit)"

def _require_geo_same(mail_row, crm_row) -> bool:
    # require city/state/zip equality after normalization
    m_city = normalize_city(mail_row.get("city"))
    c_city = normalize_city(crm_row.get("crm_city"))
    m_state = normalize_state(mail_row.get("state"))
    c_state = normalize_state(crm_row.get("crm_state"))
    m_zip = normalize_zip(mail_row.get("zip"))
    c_zip = normalize_zip(crm_row.get("crm_zip"))
    return (m_city == c_city) and (m_state == c_state) and (m_zip == c_zip) and m_zip != ""

def _same_month(mail_row, crm_row) -> bool:
    m = parse_date_to_month(mail_row.get("mail_date"))
    c = parse_date_to_month(crm_row.get("crm_job_date"))
    return m != "" and c != "" and m == c

def _confidence_bucket(score: int) -> str:
    if score >= 94: return ">=94"
    if score >= 88: return "88–94"
    return "<88"

def _join_notes(*parts: str) -> str:
    parts = [p for p in parts if p]
    return "; ".join(parts)

# --------------------------
# Match core
# --------------------------
def match_mail_to_crm(mail_df: pd.DataFrame, crm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a DataFrame of matches with columns:
      - mail_idx, crm_idx            (original indices)
      - confidence (int 0..100)
      - bucket ('>=94','88–94','<88')
      - match_notes (text; uses 'none' wording only inside notes)
      - mail_date, crm_job_date, city/state/zip, address1/address2 (originals)
    Requires the following columns (can be alias-mapped upstream):
      mail_df: ['address1','address2','city','state','zip','mail_date']
      crm_df:  ['crm_address1','crm_address2','crm_city','crm_state','crm_zip','crm_job_date']
    """
    # Normalize address1 for blocking
    m_norm = mail_df[["address1","address2","city","state","zip","mail_date"]].copy()
    c_norm = crm_df[["crm_address1","crm_address2","crm_city","crm_state","crm_zip","crm_job_date"]].copy()

    # Build stems & keys
    m_addr = m_norm["address1"].map(normalize_address1)
    c_addr = c_norm["crm_address1"].map(normalize_address1)

    m_stem = m_addr.map(lambda d: d["stem"])
    c_stem = c_addr.map(lambda d: d["stem"])

    m_type = m_addr.map(lambda d: d["street_type"])
    c_type = c_addr.map(lambda d: d["street_type"])

    m_month = m_norm["mail_date"].map(parse_date_to_month)
    c_month = c_norm["crm_job_date"].map(parse_date_to_month)

    m_zip5 = m_norm["zip"].map(normalize_zip)
    c_zip5 = c_norm["crm_zip"].map(normalize_zip)

    # Blocking keys
    m_key = m_zip5 + "|" + m_stem + "|" + m_month
    c_key = c_zip5 + "|" + c_stem + "|" + c_month

    m_blocks = m_key.to_frame("key").reset_index().rename(columns={"index":"mail_idx"})
    c_blocks = c_key.to_frame("key").reset_index().rename(columns={"index":"crm_idx"})

    # Inner join on block key
    pairs = m_blocks.merge(c_blocks, on="key", how="inner")

    out_rows = []
    for _, row in pairs.iterrows():
        mi = row["mail_idx"]; ci = row["crm_idx"]
        mrow = m_norm.loc[mi]
        crow = c_norm.loc[ci]

        # Guard: geo must align
        if not _require_geo_same(mrow, crow):
            continue
        # Guard: same month (already in block, but re-check if any empty)
        if not _same_month(mrow, crow):
            continue

        # Require exact stem equality (already in block), else skip
        if m_stem.loc[mi] == "" or c_stem.loc[ci] == "" or m_stem.loc[mi] != c_stem.loc[ci]:
            continue

        # Start score
        score = 100
        notes = []

        # Street type penalty (if different)
        st_pen, st_note = _compare_street_type(m_type.loc[mi], c_type.loc[ci])
        if st_pen != 0:
            score += st_pen
            if st_note:
                notes.append(st_note)

        # Unit penalty
        u_pen, u_note = _compare_unit(mrow.get("address2"), crow.get("crm_address2"))
        if u_pen != 0:
            score += u_pen
            if u_note:
                notes.append(u_note)

        # Clamp 0..100
        score = max(0, min(100, score))
        bucket = _confidence_bucket(score)

        # Build friendly notes (replace 'none' only in notes; never modify source data)
        match_notes = _join_notes(*notes)

        out_rows.append({
            "mail_idx": mi,
            "crm_idx": ci,
            "confidence": int(score),
            "bucket": bucket,
            "match_notes": match_notes,
            # pass through a few useful columns for downstream / display
            "mail_date": mrow.get("mail_date"),
            "crm_job_date": crow.get("crm_job_date"),
            "city": mrow.get("city"),
            "state": mrow.get("state"),
            "zip": mrow.get("zip"),
            "address1": mrow.get("address1"),
            "address2": mrow.get("address2"),
            "crm_address1": crow.get("crm_address1"),
            "crm_address2": crow.get("crm_address2"),
        })

    return pd.DataFrame(out_rows)

# --------------------------
# Optional: dedup helper (MASTER)
# --------------------------
def dedup_exact_address_date(df: pd.DataFrame,
                             addr_col: str,
                             date_col: str) -> pd.DataFrame:
    """
    Exact dedup on (normalized address1 stem, YYYY-MM-DD date string).
    Use separately per source type (mail vs crm) before merging into MASTER.
    """
    # normalize address to stem + keep original date string as-is
    stem = df[addr_col].map(lambda a: normalize_address1(a)["stem"])
    key = stem.astype(str) + "||" + df[date_col].astype(str)
    keep = ~key.duplicated(keep="first")
    return df.loc[keep].copy()
