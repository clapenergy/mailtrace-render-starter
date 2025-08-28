# wsgi.py — avoids the name clash between /app (package) and app.py (module)
import os
import importlib.util

BASE_DIR = os.path.dirname(__file__)
APP_FILE = os.path.join(BASE_DIR, "app.py")

spec = importlib.util.spec_from_file_location("rootapp", APP_FILE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

# This is what Gunicorn will serve
app = getattr(mod, "app")
