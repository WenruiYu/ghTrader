from __future__ import annotations

from ghtrader.control.app_factory import create_app
from ghtrader.control.websocket_manager import ConnectionManager, mount_dashboard_websocket

# Compatibility shim:
# - keeps uvicorn entrypoint (`ghtrader.control.app:app`)
# - keeps module-level symbols used by tests/integrations
app = create_app()
