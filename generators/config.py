"""
generators/config.py
Global configuration constants for beacon-data generators.
"""

import datetime
import os

GLOBAL_SEED = 42

DEMO_TODAY = datetime.date(2026, 3, 31)
DATA_START = datetime.date(2025, 4, 1)
DATA_END = datetime.date(2026, 3, 31)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
