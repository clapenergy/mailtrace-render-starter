# wsgi.py — makes sure the repo root is importable and loads app.py safely
import os, sys, importlib.util

BASE_DIR = os.path.dirname(__file__)
# Ensure the repo root (where /app and app.py live) is importable
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

APP_FILE = os.path.join(BASE_DIR, "app.py")

spec = importlib.util.spec_from_file_location("rootapp", APP_FILE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

# Expose the Flask app object for Gunicorn
app = getattr(mod, "app")
