"""Structured JSON console logging (optional).

Every operation is *always* persisted as one JSON line to the JSONL event log
(``mri.observability.runtime.emit``). Independently, the process can also write
JSON lines to the console so container log collectors (Airflow, Docker) see the
same canonical fields. Console JSON is opt-in via ``MRI_JSON_LOG_CONSOLE=1`` so
plain `print` output (manifests, ``--json`` payloads) is never polluted —
stdout keeps carrying machine-readable command output and console JSON goes to
stderr.

Log records never carry secrets: the formatted line is the event produced by
:meth:`mri.observability.events.PipelineEvent.as_dict`, whose payload is
already redacted.
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from collections.abc import Mapping

from mri.observability.events import PipelineEvent, now_timestamp

CONSOLE_JSON_ENV = "MRI_JSON_LOG_CONSOLE"
LOGGER_NAME = "mri.observability"

_handler_configured = False


class JsonLineFormatter(logging.Formatter):
    """Emit one compact JSON line per log record.

    When the record carries an ``event`` attribute (dict) it is printed as-is;
    otherwise a minimal ``{timestamp, level, logger, message}`` record is
    emitted (with the exception trace when one is attached).
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = getattr(record, "event", None)
        if not isinstance(payload, Mapping):
            payload = {
                "timestamp": now_timestamp(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            if record.exc_info:
                payload["exc"] = "".join(traceback.format_exception(*record.exc_info)).rstrip()
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)


def console_json_enabled() -> bool:
    return os.environ.get(CONSOLE_JSON_ENV, "").strip() != ""


def configure_json_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure (idempotently) the structured JSON handler for stderr."""
    global _handler_configured
    logger = get_logger()
    if not _handler_configured:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonLineFormatter())
        logger.addHandler(handler)
        _handler_configured = True
    logger.setLevel(level)
    logger.propagate = False
    return logger


def get_logger(name: str = LOGGER_NAME) -> logging.Logger:
    return logging.getLogger(name)


def console_when_enabled(event: PipelineEvent) -> None:
    """Log the event as JSON on stderr only when console JSON is enabled."""
    if not console_json_enabled():
        return
    logger = configure_json_logging()
    logger.info("%s", event.message, extra={"event": event.as_dict()})


__all__ = [
    "CONSOLE_JSON_ENV",
    "JsonLineFormatter",
    "console_json_enabled",
    "configure_json_logging",
    "console_when_enabled",
    "get_logger",
]
