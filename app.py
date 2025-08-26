import os
import io
import re
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, session
from werkzeug.utils import secure_filename

# Optional: basic password gate. Set MAILTRACE_PASSWORD in env to enable.
PASSWORD = os.environ.get("MAILTRACE_PASSWORD", "").strip()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB

ALLOWED_EXTENSIONS = {"csv"}

# ---------- Utils
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def abbreviate(st):
    # Common street abbreviations
    pairs = [
        (r"\bavenue\b", "ave"),
        (r"\bav\b", "ave"),
        (r"\bave\b", "ave"),
        (r"\bstreet\b", "st"),
        (r"\bstr\b", "st"),
        (r"\broad\b", "rd"),
        (r"\bdrive\b", "dr"),
        (r"\bdr\b", "dr"),
        (r"\broad\b", "rd"),
        (r"\bboulevard\b", "blvd"),
        (r"\bplace\b", "pl"),
        (r"\bcourt\b", "ct"),
        (r"\bct\b", "ct"),
        (r"\bterrace\b", "ter"),
        (r"\bparkway\b", "pkwy"),
        (r"\bhighway\b", "hwy"),
        (r"\broute\b", "rt"),
        (r"\bsaint\b", "st"),
    ]
    s = " " + st + " "
    for pat, rep in pairs:
        s = re.sub(pat, rep, s)
    return s.strip()

def clean_text(x):
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    s = s.replace(".", "").replace(",", "")
    s = re.sub(r"\s+", " ", s)
    s = abbreviate(s)
    return s

UNIT_PAT = re.compile(r"\b(apt|apartment|unit|ste|suite|#)\s*([a-z0-9\-]+)", re.I)

def extract_unit(s):
    m = UNIT_PAT.search(s or "")
    if not m:
        return ""
    return m.group(2).lower()

def strip_unit(s):
    return UNIT_PAT.sub(" ", s or "").strip()

def guess_address_col(df):
    # Try common address column names in order
    candidates = [
        "address1","address_1","address","street","street1","addr1","addr_1","address line 1","Address1","Address","Street"
    ]
    for c in candidates:
        if c in df.columns:
            return c
    # Fallback: first text-like column
    return df.columns[0]

def normalize_df(df, addr_col):
    df = df.copy()
    df["__addr_raw__"] = df[addr_col].fillna("")
    df["__unit__"] = df["__addr_raw__"].map(extract_unit)
    df["__addr_nounit__"] = df["__addr_raw__"].map(clean_text).map(strip_unit).map(clean_text)
    return df

def score_match(row):
    # Simple confidence heuristic:
    # 100 if normalized base matches exactly; -25 penalty if units mismatch; +10 if units match
    base_equal = row["__addr_nounit___mail"] == row["__addr_nounit___crm"]
    score = 100 if base_equal else 0
    notes = []
    if base_equal:
        if row["__unit___mail"] and row["__unit___crm"]:
            if row["__unit___mail"] == row["__unit___crm"]:
                score += 10
                notes.append("unit match")
            else:
                score -= 25
                notes.append(f"unit mismatch: {row['__unit___mail']} vs {row['__unit___crm']}")
        elif row["__unit___mail"] and not row["__unit___crm"]:
            score -= 10
            notes.append("mail has unit, crm none")
        elif not row["__unit___mail"] and row["__unit___crm"]:
            score -= 10
            notes.append("crm has unit, mail none")
        else:
            notes.append("no units")
    else:
        # Not a base match: very low score; keep for debugging if needed
        score = 0
        notes.append("base address differs")
    # Bound scores
    if score < 0: score = 0
    if score > 100: score = 100
    return score, (", ".join(notes) if notes else "")

# ---------- Routes
@app.route("/", methods=["GET", "POST"])
def index():
    # Optional password gate
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

    # Try to guess address columns
    mail_addr = guess_address_col(mail_df)
    crm_addr = guess_address_col(crm_df)

    # If our guess is obviously wrong (e.g., identical first column with weird name), offer manual mapping
    return render_template("confirm_mapping.html",
                           mail_cols=list(mail_df.columns),
                           crm_cols=list(crm_df.columns),
                           mail_guess=mail_addr,
                           crm_guess=crm_addr,
                           mail_json=mail_df.to_json(orient="records"),
                           crm_json=crm_df.to_json(orient="records"))

@app.route("/run", methods=["POST"])
def run():
    # Receive mapping + data payloads as JSON strings
    mail_addr = request.form.get("mail_addr")
    crm_addr = request.form.get("crm_addr")
    mail_json = request.form.get("mail_json")
    crm_json = request.form.get("crm_json")
    if not all([mail_addr, crm_addr, mail_json, crm_json]):
        flash("Missing inputs. Please upload again.")
        return redirect(url_for("index"))

    mail_df = pd.read_json(io.StringIO(mail_json))
    crm_df = pd.read_json(io.StringIO(crm_json))

    if mail_addr not in mail_df.columns or crm_addr not in crm_df.columns:
        flash("Selected columns not found. Please try again.")
        return redirect(url_for("index"))

    # Normalize
    mail_df = normalize_df(mail_df, mail_addr)
    crm_df = normalize_df(crm_df, crm_addr)

    # Inner join on normalized base address
    merged = pd.merge(
        mail_df.add_suffix("_mail"),
        crm_df.add_suffix("_crm"),
        left_on="__addr_nounit___mail",
        right_on="__addr_nounit___crm",
        how="inner"
    )

    # Score & notes
    scores = []
    notes = []
    for _, r in merged.iterrows():
        s, n = score_match(r)
        scores.append(s)
        notes.append(n)
    merged["confidence"] = scores
    merged["match_notes"] = notes

    # KPIs
    kpis = {
        "mail_rows": int(len(mail_df)),
        "crm_rows": int(len(crm_df)),
        "matches": int(len(merged)),
        "match_rate_vs_mail_%": round(100.0 * (len(merged) / max(1, len(mail_df))), 2),
        "match_rate_vs_crm_%": round(100.0 * (len(merged) / max(1, len(crm_df))), 2),
        "avg_confidence": round(float(merged["confidence"].mean()) if len(merged) else 0.0, 2)
    }

    # Build preview table (limit 500 rows)
    preview = merged.copy()
    # Optional: choose a subset of columns to display first
    front_cols = [
        f"{mail_addr}_mail", f"{crm_addr}_crm",
        "__unit___mail", "__unit___crm",
        "confidence", "match_notes"
    ]
    ordered_cols = [c for c in front_cols if c in preview.columns] + [c for c in preview.columns if c not in front_cols]
    preview = preview[ordered_cols]

    preview = preview.head(500)

    # CSV for download
    out_csv = io.StringIO()
    export_df = merged.drop(columns=[c for c in merged.columns if c.startswith("__")], errors="ignore")
    export_df.to_csv(out_csv, index=False)
    csv_bytes = out_csv.getvalue().encode("utf-8")

    return render_template(
        "result.html",
        kpis=kpis,
        columns=list(preview.columns),
        rows=preview.values.tolist(),
        csv_len=len(csv_bytes)
    )

@app.route("/download", methods=["POST"])
def download():
    # Recompute from posted JSON (stateless, no persistence)
    mail_addr = request.form.get("mail_addr")
    crm_addr = request.form.get("crm_addr")
    mail_json = request.form.get("mail_json")
    crm_json = request.form.get("crm_json")

    mail_df = pd.read_json(io.StringIO(mail_json))
    crm_df = pd.read_json(io.StringIO(crm_json))

    mail_df = normalize_df(mail_df, mail_addr)
    crm_df = normalize_df(crm_df, crm_addr)

    merged = pd.merge(
        mail_df.add_suffix("_mail"),
        crm_df.add_suffix("_crm"),
        left_on="__addr_nounit___mail",
        right_on="__addr_nounit___crm",
        how="inner"
    )

    scores = []
    notes = []
    for _, r in merged.iterrows():
        s, n = score_match(r)
        scores.append(s)
        notes.append(n)
    merged["confidence"] = scores
    merged["match_notes"] = notes

    export_df = merged.drop(columns=[c for c in merged.columns if c.startswith("__")], errors="ignore")
    out = io.BytesIO()
    export_df.to_csv(out, index=False)
    out.seek(0)
    return send_file(out, mimetype="text/csv", as_attachment=True, download_name="mailtrace_matches.csv")
