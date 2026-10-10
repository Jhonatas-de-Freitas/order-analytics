import uvicorn

from order_analytics.config import Settings
from order_analytics.logs import configure_logging

settings = Settings()
configure_logging(settings.log_level)
# log_config=None keeps uvicorn from installing its own handlers, so its lines come out
# in the same JSON format as the rest.
uvicorn.run(
    "order_analytics.main:app",
    host="0.0.0.0",
    port=settings.port,
    log_config=None,
    access_log=False,
)
