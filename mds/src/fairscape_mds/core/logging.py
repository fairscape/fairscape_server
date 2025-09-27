import logging
from logging.config import dictConfig

logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
	datefmt="%Y-%m-%d %H:%M:%S",
)

log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s\t[%(levelname)s]\t%(name)s\t%(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
				"file": {
					"class": "logging.FileHandler",
					"level": "INFO",
					"filename": "fastapi.log",
					"mode": "a"
				}
    },
    "loggers": {
        "request": {"handlers": ["console"], "level": "DEBUG", "propagate": False},
    },
    "root": {"handlers": ["file"], "level": "DEBUG"},
}

dictConfig(log_config)

crudLogger = logging.getLogger("crud")
crudLogger.setLevel(logging.INFO)

requestLogger = logging.getLogger("request")
requestLogger.setLevel(logging.INFO)