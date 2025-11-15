# -*- coding: utf-8 -*-
from typing import Any, Dict

def sync_from_pluggy(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    # Stub local/DEV: não chama API
    return {
        "imported": 0,
        "skipped": 0,
        "statements": 0,
        "message": "Pluggy stub: no sync executed.",
        "details": {},
    }
