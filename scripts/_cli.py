"""Zero-arg CLI wrappers for console_scripts entry points.

Dead OpenClaw / Discord / GDELT / news_pool wrappers stay retired.
Only first-boot remains on the Grok Bot host path.
"""
from __future__ import annotations

import sys


def newsroom_first_boot() -> None:
    from newsroom.first_boot import main
    raise SystemExit(main(sys.argv[1:]))
