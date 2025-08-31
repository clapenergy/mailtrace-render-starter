# app.py — MailTrace web app (Flask 3 compatible) - FIXED VERSION
import os, io, tempfile, traceback, logging
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, session
from markupsafe import Markup
import pandas as pd

# Your pipeline & renderer
from app.pipeline import run_pipeline
from app.dashboard_export import finalize_summary_for_export_v17, render_full_dashboard_v17

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Session + upload limits
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-production")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB

# FIXED: Better error handler with more specific error handling
@app.errorhandler(Exception)
def handle_exception(e):
    tb = traceback.format_exc()
    logger.error(f"Application error: {tb}")
    
    # Don't expose full traceback in production
    if app.debug:
        return f"<pre>DEBUG ERROR:\n{tb}</pre>", 500
    else:
        return render_template("error.html", error_message=str(e)), 500

# FIXED: Add specific error handlers for common issues
@app.errorhandler(413)
def too_large(e):
    flash("File too large. Please use files smaller than 200MB.")
    return redirect(url_for("index"))

@app.errorhandler(404)
def not_found(e):
    return render_template("404.html"), 404

# Health check for Render
@app.route("/healthz")
def healthz():
    return "OK", 200

@app.route("/", methods=["GET"])
def index():
    # Clear any old session data
    session.pop('export_csv', None)
    return render_template("index.html")

@app.route("/run", methods=["POST"])
def run():
    try:
        # FIXED: Better file validation
        mail_file = request.files.get("mail_csv")
        crm_file = request.files.get("crm_csv")
        
        if not mail_file or not crm_file:
            flash("Please upload both CSV files.")
            return redirect(url_for("index"))
        
        if mail_file.filename == '' or crm_file.filename == '':
            flash("Please select valid CSV files.")
            return redirect(url_for("index"))
        
        # FIXED: Validate file extensions
        def is_csv(filename):
            return filename and filename.lower().endswith('.csv')
        
        if not is_csv(mail_file.filename) or not is_csv(crm_file.filename):
            flash("Please upload only CSV files (.csv extension required).")
            return redirect(url_for("index"))
        
        logger.info(f"Processing files: {mail_file.filename}, {crm_file.filename}")
        
        # FIXED: Better temporary file handling with explicit cleanup
        mf_path = None
        cf_path = None
        
        try:
            # Save uploads to temp files (pipeline expects file paths)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as mf:
                mail_file.save(mf)
                mf_path = mf.name
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as cf:
                crm_file.save(cf)
                cf_path = cf.name
            
            logger.info(f"Saved temp files: {mf_path}, {cf_path}")
            
            # FIXED: Validate CSV files can be read
            try:
                mail_test = pd.read_csv(mf_path, nrows=1)
                crm_test = pd.read_csv(cf_path, nrows=1)
                logger.info(f"CSV validation passed. Mail cols: {list(mail_test.columns)}, CRM cols: {list(crm_test.columns)}")
            except Exception as e:
                flash(f"Invalid CSV format: {str(e)}")
                return redirect(url_for("index"))
            
            # Run pipeline → summary
            logger.info("Starting pipeline processing...")
            summary = run_pipeline(mf_path, cf_path)
            
            if summary.empty:
                flash("No matches found between your CSV files. Please check that your address columns contain similar data.")
                return redirect(url_for("index"))
            
            logger.info(f"Pipeline complete. Found {len(summary)} matches.")
            
            # Normalize for export/render
            summary_v17 = finalize_summary_for_export_v17(summary)
            
            # FIXED: Better mail count calculation with error handling
            try:
                mail_count_total = len(pd.read_csv(mf_path, dtype=str, keep_default_na=False))
            except Exception as e:
                logger.warning(f"Could not count mail records: {e}")
                mail_count_total = len(summary_v17)
            
            # FIXED: More robust notes cleaning
            def _fix_notes(x):
                if pd.isna(x) or x is None:
                    return ""
                if not isinstance(x, str):
                    x = str(x)
                return x.replace("NaN", "none").replace("nan", "none").replace("None", "none")
            
            if "Notes" in summary_v17.columns:
                summary_v17["Notes"] = summary_v17["Notes"].map(_fix_notes)
            
            logger.info("Generating dashboard HTML...")
            html = render_full_dashboard_v17(summary_v17, mail_count_total)
            
            if not html or len(html.strip()) == 0:
                raise Exception("Dashboard HTML generation failed - empty output")
            
            # FIXED: Better CSV handling for session storage
            try:
                csv_text = summary_v17.to_csv(index=False)
                session["export_csv"] = csv_text
                csv_len = len(csv_text.encode("utf-8"))
                logger.info(f"Stored CSV in session, {csv_len} bytes")
            except Exception as e:
                logger.error(f"Failed to prepare CSV export: {e}")
                csv_len = 0
            
            # FIXED: More explicit Markup usage and validation
            try:
                safe_html = Markup(html)
                logger.info("Dashboard rendering complete, returning results page")
                return render_template("result.html", dashboard_html=safe_html, csv_len=csv_len)
            except Exception as e:
                logger.error(f"Failed to render results template: {e}")
                flash("Dashboard generation completed but display failed. Please try again.")
                return redirect(url_for("index"))
                
        finally:
            # FIXED: Always clean up temp files
            for path in [mf_path, cf_path]:
                if path and os.path.exists(path):
                    try:
                        os.unlink(path)
                        logger.info(f"Cleaned up temp file: {path}")
                    except Exception as e:
                        logger.warning(f"Could not delete temp file {path}: {e}")
    
    except Exception as e:
        logger.error(f"Error in run route: {traceback.format_exc()}")
        flash(f"Processing failed: {str(e)}")
        return redirect(url_for("index"))

@app.route("/download", methods=["POST"])
def download():
    try:
        csv_text = session.get("export_csv")
        
        if not csv_text:
            flash("No data available for download. Please process your files first.")
            return redirect(url_for("index"))
        
        # FIXED: Better file download handling
        output = io.BytesIO()
        output.write(csv_text.encode("utf-8"))
        output.seek(0)
        
        logger.info("Sending CSV download")
        return send_file(
            output,
            mimetype="text/csv",
            as_attachment=True,
            download_name="mailtrace_matches.csv"
        )
    
    except Exception as e:
        logger.error(f"Download error: {e}")
        flash("Download failed. Please try processing your files again.")
        return redirect(url_for("index"))

# FIXED: Add a simple error template fallback
@app.route("/debug")
def debug_info():
    if not app.debug:
        return "Debug mode disabled", 403
    
    info = {
        "Python path": os.sys.path[:3],
        "Working directory": os.getcwd(),
        "Flask version": getattr(__import__('flask'), '__version__', 'unknown'),
        "Session keys": list(session.keys()),
        "Environment vars": {k: v for k, v in os.environ.items() if 'SECRET' not in k}
    }
    
    return f"<pre>{info}</pre>"

if __name__ == "__main__":
    # FIXED: Better development vs production configuration
    debug_mode = os.environ.get("FLASK_ENV") == "development"
    port = int(os.environ.get("PORT", 5000))
    
    if debug_mode:
        app.debug = True
        logger.info("Running in DEBUG mode")
    
    logger.info(f"Starting Flask app on port {port}")
    app.run(host="0.0.0.0", port=port, debug=debug_mode)
