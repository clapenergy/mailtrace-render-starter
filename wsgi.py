# wsgi.py — ensures the repo root is importable and loads app.py safely
import os, sys, importlib.util

BASE_DIR = os.path.dirname(__file__)

# Make sure Python can import from the repo root (where /app and app.py live)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

APP_FILE = os.path.join(BASE_DIR, "app.py")

# Load app.py as a module named 'rootapp'
spec = importlib.util.spec_from_file_location("rootapp", APP_FILE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

# Expose the Flask app object for Gunicorn
app = getattr(mod, "app")
