import json
from typing import Any


def to_json_text(value: Any) -> str | None:
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return None


def from_json_text(text: str | None) -> Any:
    if text is None:
        return None
    return json.loads(text)
