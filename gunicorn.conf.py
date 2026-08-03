# Gunicorn config for eventlet worker
# NOTE: eventlet.monkey_patch() is done in main.py (first lines) — 
# doing it here is too late since gunicorn already imported threading.
import os

worker_class = "eventlet"
workers = 1
bind = f"0.0.0.0:{os.environ.get('PORT', 10000)}"
