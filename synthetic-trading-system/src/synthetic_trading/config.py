from __future__ import annotations

import os
from pathlib import Path

WS_URL = os.getenv("DERIV_PUBLIC_WS_URL", "wss://api.derivws.com/trading/v1/options/ws/public")
DEFAULT_TIMEOUT = float(os.getenv("DERIV_REQUEST_TIMEOUT_SECONDS", "30"))
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
