from __future__ import annotations

import logging
from typing import Any


class KeyValueAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        extra_fields = kwargs.pop("extra_fields", {})
        if extra_fields:
            kv = " ".join(f"{k}={extra_fields[k]!r}" for k in sorted(extra_fields))
            msg = f"{msg} | {kv}"
        return msg, kwargs


def configure_logging(debug_mode: bool) -> None:
    level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def get_logger(name: str) -> KeyValueAdapter:
    return KeyValueAdapter(logging.getLogger(name), {})
