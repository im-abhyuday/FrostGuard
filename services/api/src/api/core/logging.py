"""
Structured logging module for ColdChainIQ API.
Injects request_id context and redacts sensitive credentials and tokens.
"""

from contextvars import ContextVar
import logging
import re
import sys
from typing import Any, Dict
from pythonjsonlogger import jsonlogger

# Context variable tracking active request_id per coroutine/thread
request_id_ctx_var: ContextVar[str] = ContextVar("request_id", default="")


def get_request_id() -> str:
    """Returns the active request_id or empty string."""
    return request_id_ctx_var.get()


def set_request_id(req_id: str) -> None:
    """Sets the active request_id."""
    request_id_ctx_var.set(req_id)


class RedactionJsonFormatter(jsonlogger.JsonFormatter):
    """
    JSON Formatter that automatically sanitizes sensitive fields:
    passwords, tokens, and bearer authorization credentials.
    """
    SENSITIVE_KEYS = {
        "password",
        "current_password",
        "new_password",
        "confirm_password",
        "access_token",
        "refresh_token",
        "token",
        "authorization",
        "secret",
    }

    BEARER_PATTERN = re.compile(r"Bearer\s+([A-Za-z0-9\-_=]+\.[A-Za-z0-9\-_=]+\.?[A-Za-z0-9\-_=]*)", re.IGNORECASE)

    def process_log_record(self, log_record: Dict[str, Any]) -> Dict[str, Any]:
        req_id = get_request_id()
        if req_id and "request_id" not in log_record:
            log_record["request_id"] = req_id

        # Recursively redact
        return self._redact(log_record)

    def _redact(self, data: Any) -> Any:
        if isinstance(data, dict):
            new_dict = {}
            for k, v in data.items():
                if k.lower() in self.SENSITIVE_KEYS:
                    new_dict[k] = "[REDACTED]"
                else:
                    new_dict[k] = self._redact(v)
            return new_dict
        elif isinstance(data, list):
            return [self._redact(item) for item in data]
        elif isinstance(data, str):
            # Redact Authorization: Bearer tokens
            if "bearer" in data.lower():
                return self.BEARER_PATTERN.sub("Bearer [REDACTED]", data)
            return data
        return data


def setup_api_logging(log_level: str = "INFO") -> None:
    """Configures root logger with structured JSON redaction formatter."""
    handler = logging.StreamHandler(sys.stdout)
    formatter = RedactionJsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
