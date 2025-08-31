# wsgi.py — load app.py safely
import os, sys, importlib.util

BASE_DIR = os.path.dirname(__file__)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

APP_FILE = os.path.join(BASE_DIR, "app.py")
spec = importlib.util.spec_from_file_location("rootapp", APP_FILE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

app = getattr(mod, "app")
