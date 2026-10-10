import json
import logging
import sys
from datetime import UTC, datetime

# color_message is uvicorn's copy of the message with terminal colours.
_NOT_CONTEXT = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {
    "message",
    "asctime",
    "taskName",
    "color_message",
}


class JsonFormatter(logging.Formatter):
    """One JSON object per line; whatever is passed in `extra` becomes a field."""

    def format(self, record: logging.LogRecord) -> str:
        moment = datetime.fromtimestamp(record.created, UTC)
        entry: dict[str, object] = {
            "time": moment.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        entry.update((key, value) for key, value in vars(record).items() if key not in _NOT_CONTEXT)
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)
    # aiokafka logs every heartbeat hiccup at INFO; warnings are enough here.
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
