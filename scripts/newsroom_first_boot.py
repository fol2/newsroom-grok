#!/usr/bin/env python3
"""First-boot bring-up and health CLI for the trusted-operator host."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from newsroom.first_boot import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
