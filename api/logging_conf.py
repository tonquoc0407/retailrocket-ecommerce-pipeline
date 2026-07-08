import json
import logging
import sys

# fields the request middleware attaches; pulled onto the top level of the json log
REQUEST_FIELDS = ("method", "path", "status", "latency_ms")


class JsonFormatter(logging.Formatter):
    def format(self, record):
        out = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for f in REQUEST_FIELDS:
            if hasattr(record, f):
                out[f] = getattr(record, f)
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out)


def configure_logging():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
