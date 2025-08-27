from __future__ import annotations
import re
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
UNIT_WORDS = {"apt","apartment","suite","ste","unit","#"}
def squash_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()
def norm_token(tok: str) -> str:
    t = tok.lower().strip(".,")
    if t in STREET_TYPES: return STREET_TYPES[t]
    if t in DIRECTIONALS: return DIRECTIONALS[t]
    return t
def normalize_address1(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.replace("-", " ")
    s = re.sub(r"[^\w#\s]", " ", s)
    parts = [norm_token(p) for p in s.lower().split() if p.strip()]
    return squash_ws(" ".join(parts))
def block_key(addr1: str) -> str:
    if not isinstance(addr1, str): return ""
    toks = [t for t in squash_ws(addr1).split() if t]
    if not toks: return ""
    first = toks[0]
    second_initial = toks[1][0] if len(toks) > 1 else ""
    return f"{first}|{second_initial}".lower()
def tokens(s: str) -> list[str]:
    return [t for t in normalize_address1(s).split() if t]
def street_type_of(tokens_list: list[str]) -> str|None:
    if not tokens_list: return None
    last = tokens_list[-1]
    return last if last in STREET_TYPES.values() else None
def directional_in(tokens_list: list[str]) -> str|None:
    for t in tokens_list:
        if t in DIRECTIONALS.values():
            return t
    return None
