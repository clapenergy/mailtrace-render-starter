import os
import io
import re
from difflib import SequenceMatcher
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, session

PASSWORD = os.environ.get("MAILTRACE_PASSWORD", "").strip()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB

ALLOWED_EXTENSIONS = {"csv"}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

# --- Normalization helpers ---

DIRECTION_MAP = {
    "north":"n", "n.":"n", "n":"n",
    "south":"s", "s.":"s", "s":"s",
    "east":"e", "e.":"e", "e":"e",
    "west":"w", "w.":"w", "w":"w",
    "northeast":"ne","ne.":"ne","ne":"ne",
    "northwest":"nw","nw.":"nw","nw":"nw",
    "southeast":"se","se.":"se","se":"se",
    "southwest":"sw","sw.":"sw","sw":"sw",
}

SUFFIX_MAP = {
    "street":"st", "str":"st", "st.":"st", "st":"st",
    "avenue":"ave","av":"ave","ave.":"ave","ave":"ave",
    "road":"rd","rd.":"rd","rd":"rd",
    "drive":"dr","dr.":"dr","dr":"dr",
    "lane":"ln","ln.":"ln","ln":"ln",
    "boulevard":"blvd","blvd.":"blvd","blvd":"blvd",
    "court":"ct","ct.":"ct","ct":"ct",
    "place":"pl","pl.":"pl","pl":"pl",
    "terrace":"ter","ter.":"ter","ter":"ter",
    "parkway":"pkwy","pkwy.":"pkwy","pky":"pkwy",
    "highway":"hwy","hwy.":"hwy","hway":"hwy",
    "route":"rt","rt.":"rt","rte":"rt",
    "circle":"cir","cir.":"cir","cir":"cir",
    "boulevd":"blvd",
    "way":"way","driveway":"drwy",
    "trail":"trl","trl.":"trl","trl":"trl",
    "place":"pl",
    "plaza":"plz","plz.":"plz","plz":"plz"
}

UNIT_RE = re.compile(r"\b(apt|apartment|unit|ste|suite|#|fl|floor|bldg|building)\s*([a-z0-9\-]+)", re.I)
ZIP_RE = re.compile(r"^\s*(\d{5})(?:-\d{4})?\s*$")

CITY_CANDIDATES = {"city","town","municipality","locality","place"}
STATE_CANDIDATES = {"state","st","province","region"}
ZIP_CANDIDATES = {"zip","zip_code","zipcode","postal","postal_code"}

def normalize_token(tok):
    t = tok.strip().lower()
    t = t.replace(".", "").replace(",", "")
    # Ordinals: 1st -> 1, 2nd -> 2, etc.
    t = re.sub(r"\b(\d+)(st|nd|rd|th)\b", r"\1", t)
    if t in DIRECTION_MAP:
        t = DIRECTION_MAP[t]
    if t in SUFFIX_MAP:
        t = SUFFIX_MAP[t]
    return t

def canonical_street(address):
    if address is None:
        return "", "", ""
    s = str(address).strip().lower()
    s = re.sub(r"[.,]", " ", s)
    s = re.sub(r"\s+", " ", s)

    # Extract unit (keep for scoring)
    unit_match = UNIT_RE.search(s)
    unit = unit_match.group(2).lower() if unit_match else ""
    s_no_unit = UNIT_RE.sub(" ", s)

    # split tokens
    toks = [normalize_token(t) for t in s_no_unit.split() if t.strip()]
    if not toks:
        return "", unit, ""

    # Heuristic: first token is likely number, last is suffix, middle is name (plus directionals)
    number = toks[0] if toks and toks[0].isdigit() else ""
    # remove likely number from the list for reconstruction
    core = toks[1:] if number else toks[:]

    # Reduce repeated direction tokens (e.g., "n n")
    core = [t for i,t in enumerate(core) if i==0 or t != core[i-1]]

    # Normalize: compress to "name suffix" form; if last token is a known suffix, keep it
    suffix = core[-1] if core and core[-1] in set(SUFFIX_MAP.values()) else ""
    name_parts = core[:-1] if suffix else core[:]
    name = " ".join(name_parts)

    base = " ".join([x for x in [number, name, suffix] if x])
    base = re.sub(r"\s+", " ", base).strip()
    return base, unit, name

def guess_col(df, candidates_set):
    # Return first matching column ignoring case/whitespace/underscores
    def norm(c): return re.sub(r"[\s_]+", "", c.strip().lower())
    cols_norm = {norm(c): c for c in df.columns}
    for target in candidates_set:
        if target in cols_norm:
            return cols_norm[target]
    return None

def guess_address_col(df):
    cands = {"address1","address_1","address","street","street1","addr1","addr_1","addressline1","address line 1"}
    # case-insensitive match
    for c in df.columns:
        if re.sub(r"[\s_]+","",c.lower()) in cands:
            return c
    # fallback to first text-like column
    return df.columns[0]

def build_key_parts(df, addr_col):
    # Return dataframe with canonical street (no unit), unit, optional city/state/zip
    city_col = guess_col(df, CITY_CANDIDATES) or ""
    state_col = guess_col(df, STATE_CANDIDATES) or ""
    zip_col = guess_col(df, ZIP_CANDIDATES) or ""

    base_list = []
    units = []
    for v in df[addr_col].fillna("").tolist():
        base, unit, _ = canonical_street(v)
        base_list.append(base)
        units.append(unit)

    out = pd.DataFrame({
        "__base__": base_list,
        "__unit__": units
    }, index=df.index)

    if city_col:
        out["__city__"] = df[city_col].fillna("").str.strip().str.lower()
    else:
        out["__city__"] = ""

    if state_col:
        out["__state__"] = df[state_col].fillna("").str.strip().str.lower()
    else:
        out["__state__"] = ""

    if zip_col:
        z = df[zip_col].astype(str).str.extract(ZIP_RE)[0].fillna("")
        out["__zip__"] = z
    else:
        out["__zip__"] = ""

    return out, {"city": city_col, "state": state_col, "zip": zip_col}

def exact_join(mail_keys, crm_keys):
    # Join on base + optional city/state/zip when present
    join_cols = ["__base__"]
    # Only include geo columns if both sides have non-empty entries
    for c in ["__city__", "__state__", "__zip__"]:
        if (mail_keys[c] != "").any() and (crm_keys[c] != "").any():
            join_cols.append(c)
    merged = mail_keys.join(crm_keys, lsuffix="_mail", rsuffix="_crm")
    for c in join_cols:
        merged = merged[merged[f"{c}_mail"] == merged[f"{c}_crm"]]
    return merged.index.get_level_values(0), merged.index.get_level_values(1)

def fuzzy_candidates(mail_row, crm_df, same_geo=True):
    # narrow by city/zip when available to reduce false positives
    subset = crm_df
    if same_geo:
        if mail_row["__zip__"]:
            subset = subset[subset["__zip__"] == mail_row["__zip__"]]
        elif mail_row["__city__"]:
            subset = subset[subset["__city__"] == mail_row["__city__"]]
    return subset

def fuzzy_match(mail_keys, crm_keys, unmatched_mail_idx, ratio_cutoff=0.94):
    matches = []
    for i in unmatched_mail_idx:
        mail_row = mail_keys.loc[i]
        if not mail_row["__base__"]:
            continue
        subset = fuzzy_candidates(mail_row, crm_keys)
        best_j = None
        best_ratio = 0.0
        for j, crm_row in subset.iterrows():
            r = SequenceMatcher(None, mail_row["__base__"], crm_row["__base__"]).ratio()
            if r > best_ratio:
                best_ratio = r
                best_j = j
        if best_j is not None and best_ratio >= ratio_cutoff:
            matches.append((i, best_j, best_ratio))
    return matches

def score_pair(mail_unit, crm_unit, base_equal, fuzzy_ratio=None):
    # Base confidence
    if base_equal:
        score = 100
        mtype = "exact"
    else:
        # fuzzy
        score = int(round(100 * (fuzzy_ratio or 0.0)))
        mtype = "fuzzy"

    notes = []
    if mail_unit and crm_unit:
        if mail_unit == crm_unit:
            score += 10
            notes.append("unit match")
        else:
            score -= 25
            notes.append(f"unit mismatch: {mail_unit} vs {crm_unit}")
    elif mail_unit and not crm_unit:
        score -= 10
        notes.append("mail has unit, crm none")
    elif not mail_unit and crm_unit:
        score -= 10
        notes.append("crm has unit, mail none")
    else:
        notes.append("no units")

    score = max(0, min(100, score))
    return score, mtype, ", ".join(notes)

@app.route("/", methods=["GET", "POST"])
def index():
    if PASSWORD:
        if request.method == "POST" and "password" in request.form:
            if request.form.get("password") == PASSWORD:
                session["authed"] = True
                return redirect(url_for("index"))
            else:
                flash("Wrong password.")
        if not session.get("authed"):
            return render_template("gate.html")
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload():
    mail_file = request.files.get("mail_csv")
    crm_file = request.files.get("crm_csv")
    if not mail_file or not crm_file:
        flash("Please upload both CSV files.")
        return redirect(url_for("index"))
    if not (allowed_file(mail_file.filename) and allowed_file(crm_file.filename)):
        flash("Only .csv files are allowed.")
        return redirect(url_for("index"))

    try:
        mail_df = pd.read_csv(mail_file)
    except Exception:
        mail_file.stream.seek(0)
        mail_df = pd.read_csv(mail_file, encoding_errors="ignore")

    try:
        crm_df = pd.read_csv(crm_file)
    except Exception:
        crm_file.stream.seek(0)
        crm_df = pd.read_csv(crm_file, encoding_errors="ignore")

    mail_addr = guess_address_col(mail_df)
    crm_addr = guess_address_col(crm_df)

    # Store data in session
    session["mail_json"] = mail_df.to_json(orient="records")
    session["crm_json"] = crm_df.to_json(orient="records")

    return render_template(
        "confirm_mapping.html",
        mail_cols=list(mail_df.columns),
        crm_cols=list(crm_df.columns),
        mail_guess=mail_addr,
        crm_guess=crm_addr,
    )

@app.route("/run", methods=["POST"])
def run():
    mail_addr = request.form.get("mail_addr")
    crm_addr = request.form.get("crm_addr")

    mail_json = session.get("mail_json")
    crm_json = session.get("crm_json")
    if not all([mail_addr, crm_addr, mail_json, crm_json]):
        flash("Missing data. Please upload again.")
        return redirect(url_for("index"))

    mail_df = pd.read_json(io.StringIO(mail_json))
    crm_df = pd.read_json(io.StringIO(crm_json))

    # Build keys
    mail_keys, mail_geo_cols = build_key_parts(mail_df, mail_addr)
    crm_keys, crm_geo_cols = build_key_parts(crm_df, crm_addr)

    # Exact join first
    try:
        left_idx, right_idx = exact_join(mail_keys, crm_keys)
        exact_pairs = list(zip(left_idx.tolist(), right_idx.tolist()))
    except Exception:
        exact_pairs = []

    used_mail = set(i for i,_ in exact_pairs)
    used_crm = set(j for _,j in exact_pairs)

    # Fuzzy for remaining
    unmatched_mail = [i for i in mail_keys.index if i not in used_mail]
    crm_remaining = crm_keys.drop(index=list(used_crm)) if used_crm else crm_keys
    fuzzy_pairs = []
    if len(unmatched_mail) and len(crm_remaining):
        for (i,j,ratio) in fuzzy_match(mail_keys, crm_remaining, unmatched_mail, ratio_cutoff=0.94):
            fuzzy_pairs.append((i,j,ratio))
            used_mail.add(i)
            used_crm.add(j)

    # Build final merged table
    rows = []
    for i,j in exact_pairs:
        m = mail_keys.loc[i]; c = crm_keys.loc[j]
        score, mtype, notes = score_pair(m["__unit__"], c["__unit__"], base_equal=True)
        row = {
            "mail_index": i, "crm_index": j,
            "match_type": mtype, "confidence_pct": score, "match_notes": notes
        }
        rows.append(row)
    for i,j,ratio in fuzzy_pairs:
        m = mail_keys.loc[i]; c = crm_keys.loc[j]
        score, mtype, notes = score_pair(m["__unit__"], c["__unit__"], base_equal=False, fuzzy_ratio=ratio)
        row = {
            "mail_index": i, "crm_index": j,
            "match_type": mtype, "confidence_pct": score, "match_notes": f"{notes} (ratio {round(ratio,3)})"
        }
        rows.append(row)

    if not rows:
        kpis = {
            "mail_rows": int(len(mail_df)),
            "crm_rows": int(len(crm_df)),
            "matches": 0,
            "match_rate_vs_mail_%": 0.0,
            "match_rate_vs_crm_%": 0.0,
            "avg_confidence_%": 0.0,
        }
        session["export_csv"] = ""
        return render_template(
            "result.html",
            kpis=kpis, columns=[], rows=[], csv_len=0,
            mail_addr=mail_addr, crm_addr=crm_addr
        )

    matches_df = pd.DataFrame(rows)

    # Join back selected columns for preview/export
    mail_cols_keep = [mail_addr]
    for extra in ["name","Name","full_name","Full Name","id","ID","MailID"]:
        if extra in mail_df.columns:
            mail_cols_keep.append(extra)
    crm_cols_keep = [crm_addr]
    for extra in ["name","Name","full_name","Full Name","id","ID","CustomerID"]:
        if extra in crm_df.columns:
            crm_cols_keep.append(extra)

    out = matches_df.merge(mail_df[mail_cols_keep], left_on="mail_index", right_index=True, how="left")
    out = out.merge(crm_df[crm_cols_keep], left_on="crm_index", right_index=True, how="left")

    # KPIs
    kpis = {
        "mail_rows": int(len(mail_df)),
        "crm_rows": int(len(crm_df)),
        "matches": int(len(out)),
        "match_rate_vs_mail_%": round(100.0 * (len(out) / max(1, len(mail_df))), 2),
        "match_rate_vs_crm_%": round(100.0 * (len(out) / max(1, len(crm_df))), 2),
        "avg_confidence_%": round(float(out["confidence_pct"].mean()), 1),
    }

    # Preview columns
    preview_cols = []
    if mail_addr in out.columns: preview_cols.append(mail_addr)
    if crm_addr in out.columns: preview_cols.append(crm_addr)
    preview_cols += ["match_type", "confidence_pct", "match_notes"]
    for c in out.columns:
        if c not in preview_cols:
            preview_cols.append(c)
    preview = out[preview_cols].head(500)

    # Export (CSV)
    export_cols = preview_cols
    export_df = out[export_cols].copy()
    session["export_csv"] = export_df.to_csv(index=False)

    return render_template(
        "result.html",
        kpis=kpis,
        columns=list(preview.columns),
        rows=preview.values.tolist(),
        csv_len=len(session["export_csv"].encode("utf-8")),
        mail_addr=mail_addr,
        crm_addr=crm_addr,
    )

@app.route("/download", methods=["POST"])
def download():
    csv_text = session.get("export_csv", "")
    out = io.BytesIO(csv_text.encode("utf-8"))
    out.seek(0)
    return send_file(out, mimetype="text/csv", as_attachment=True, download_name="mailtrace_matches.csv")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))