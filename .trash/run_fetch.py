#!/usr/bin/env python3
"""Shim — delegates to backend.cli. Prefer: cmm-fetch <command>"""
from backend.cli import main
if __name__ == "__main__":
    main()
