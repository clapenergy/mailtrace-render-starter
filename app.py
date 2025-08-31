# app.py — DEBUG VERSION - Shows exactly what data is flowing through
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

@app.errorhandler(Exception)
def handle_exception(e):
    tb = traceback.format_exc()
    logger.error(f"Application error: {tb}")
    return f"<pre>DEBUG ERROR:\n{tb}</pre>", 500

@app.route("/healthz")
def healthz():
    return "OK", 200

@app.route("/", methods=["GET"])
def index():
    session.pop('export_csv', None)
    return render_template("index.html")

@app.route("/run", methods=["POST"])
def run():
    try:
        mail_file = request.files.get("mail_csv")
        crm_file = request.files.get("crm_csv")
        
        if not mail_file or not crm_file:
            flash("Please upload both CSV files.")
            return redirect(url_for("index"))
        
        logger.info(f"Processing files: {mail_file.filename}, {crm_file.filename}")
        
        # Save files properly
        mf_path = None
        cf_path = None
        
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as mf:
                mail_file.save(mf)
                mf_path = mf.name
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as cf:
                crm_file.save(cf)
                cf_path = cf.name
            
            # DEBUG: Check what's in the files
            mail_test = pd.read_csv(mf_path, nrows=3)
            crm_test = pd.read_csv(cf_path, nrows=3)
            logger.info(f"MAIL CSV COLUMNS: {list(mail_test.columns)}")
            logger.info(f"MAIL SAMPLE:\n{mail_test.to_string()}")
            logger.info(f"CRM CSV COLUMNS: {list(crm_test.columns)}")
            logger.info(f"CRM SAMPLE:\n{crm_test.to_string()}")
            
            # Run pipeline
            logger.info("Starting pipeline...")
            summary = run_pipeline(mf_path, cf_path)
            
            # DEBUG: Check pipeline output
            logger.info(f"PIPELINE OUTPUT SHAPE: {summary.shape}")
            logger.info(f"PIPELINE COLUMNS: {list(summary.columns)}")
            if len(summary) > 0:
                logger.info(f"PIPELINE SAMPLE:\n{summary.head(2).to_string()}")
            else:
                logger.error("PIPELINE RETURNED EMPTY DATAFRAME!")
                flash("No matches found. Check your CSV data and column formats.")
                return redirect(url_for("index"))
            
            # Finalize for export
            logger.info("Finalizing summary...")
            summary_v17 = finalize_summary_for_export_v17(summary)
            
            # DEBUG: Check finalized output
            logger.info(f"FINALIZED SHAPE: {summary_v17.shape}")
            logger.info(f"FINALIZED COLUMNS: {list(summary_v17.columns)}")
            if len(summary_v17) > 0:
                logger.info(f"FINALIZED SAMPLE:\n{summary_v17.head(2).to_string()}")
                
                # Check for empty data
                for col in summary_v17.columns:
                    non_empty = summary_v17[col].dropna().astype(str).str.strip()
                    non_empty = non_empty[non_empty != ""].shape[0]
                    logger.info(f"Column '{col}': {non_empty} non-empty values")
            
            # Get mail count
            try:
                mail_count_total = len(pd.read_csv(mf_path, dtype=str))
            except:
                mail_count_total = len(summary_v17)
            
            logger.info(f"Mail count: {mail_count_total}")
            
            # Generate dashboard
            logger.info("Generating dashboard...")
            html = render_full_dashboard_v17(summary_v17, mail_count_total)
            
            # Check CSV export
            csv_text = summary_v17.to_csv(index=False)
            session["export_csv"] = csv_text
            csv_len = len(csv_text.encode("utf-8"))
            
            logger.info(f"Dashboard HTML length: {len(html)}")
            logger.info(f"CSV length: {csv_len}")
            
            return render_template("result.html", dashboard_html=Markup(html), csv_len=csv_len)
                
        finally:
            # Clean up temp files
            for path in [mf_path, cf_path]:
                if path and os.path.exists(path):
                    try:
                        os.unlink(path)
                    except:
                        pass
    
    except Exception as e:
        logger.error(f"Error in run route: {traceback.format_exc()}")
        flash(f"Processing failed: {str(e)}")
        return redirect(url_for("index"))

@app.route("/download", methods=["POST"])
def download():
    try:
        csv_text = session.get("export_csv")
        
        if not csv_text:
            flash("No data available for download.")
            return redirect(url_for("index"))
        
        output = io.BytesIO()
        output.write(csv_text.encode("utf-8"))
        output.seek(0)
        
        return send_file(
            output,
            mimetype="text/csv",
            as_attachment=True,
            download_name="mailtrace_matches.csv"
        )
    
    except Exception as e:
        logger.error(f"Download error: {e}")
        flash("Download failed.")
        return redirect(url_for("index"))

if __name__ == "__main__":
    debug_mode = True  # Force debug for now
    port = int(os.environ.get("PORT", 5000))
    app.debug = True
    logger.info("Starting Flask app in DEBUG mode")
    app.run(host="0.0.0.0", port=port, debug=debug_mode)
