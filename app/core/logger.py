import logging.config
from app.core.config import settings

LOG_LEVEL = settings.LOG_LEVEL

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {
        "": {  # root logger
            "level": LOG_LEVEL,
            "handlers": ["default"],
            "propagate": False,
        },
        "app": {
            "level": LOG_LEVEL,
            "handlers": ["default"],
            "propagate": False,
        }
    },
}

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger('app')
