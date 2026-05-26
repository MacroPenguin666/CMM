#!/usr/bin/env python3
"""Launch the China Policy Monitor dashboard."""
from policy_monitor.dashboard import app

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=False)
