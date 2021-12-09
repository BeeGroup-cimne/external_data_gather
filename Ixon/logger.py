import logging
import logging.config
import sys
from datetime import datetime


def setup_logger(log_name):
    # LOGFILE = 'logs/{0}.{1}.log'.format(
    #     log_name,
    #     datetime.now().strftime('%Y%m%dT%H%M%S'))

    DEFAULT_LOGGING = {
        'version': 1,
        'formatters': {
            'standard': {
                'format': '%(asctime)s %(levelname)s: %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
            'simple': {
                'format': '%(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
                'level': 'DEBUG',
                'stream': sys.stdout,
            }
            # 'file': {
            #     'class': 'logging.FileHandler',
            #     'formatter': 'standard',
            #     'level': 'INFO',
            #     'filename': LOGFILE,
            #     'mode': 'w',
            # },
        },
        'loggers': {
            __name__: {
                'level': 'DEBUG',
                'handlers': ['console'],
                'propagate': False,
            },
        }
    }

    logging.basicConfig(level=logging.ERROR)
    logging.config.dictConfig(DEFAULT_LOGGING)
    return logging.getLogger(__name__)
