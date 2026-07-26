"""Shared pytest fixtures. Adds each service directory to sys.path so tests
can `import predictor`, `import app`, `import crypto_producer` without an
editable install."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for sub in ("ml-service", "producer", "web-dashboard/backend"):
    sys.path.insert(0, os.path.join(ROOT, sub))
