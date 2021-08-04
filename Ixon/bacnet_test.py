import logging
import logging.config
import os
import sys
from datetime import datetime

import BAC0
import pandas as pd


def setup_logger():
    LOGFILE = 'logs/{0}.{1}.log'.format(
        os.path.basename(__file__),
        datetime.now().strftime('%Y%m%dT%H%M%S'))

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
            },
            'file': {
                'class': 'logging.FileHandler',
                'formatter': 'simple',
                'level': 'INFO',
                'filename': LOGFILE,
                'mode': 'w',
            },
        },
        'loggers': {
            __name__: {
                'level': 'DEBUG',
                'handlers': ['console', 'file'],
                'propagate': False,
            },
        }
    }

    logging.basicConfig(level=logging.ERROR)
    logging.config.dictConfig(DEFAULT_LOGGING)
    return logging.getLogger(__name__)


if __name__ == '__main__':

    """
    Primer de Maig: 10.187.10.1/16 10.187.113.206:47808 9000 10.81.182.10 primer_maig.csv
    Flix: 10.187.10.1/16 10.187.43.146:47808 9000 192.168.140.99 template.csv
    """

    # Init config
    log = setup_logger()
    if len(sys.argv) < 5:
        log.error('$ python %s < my_ip > < bbmdAddress > < bbmTTL > < bacnet_device > < file >' % sys.argv[0])

    my_ip = sys.argv[1]
    bbmdIP = sys.argv[2]
    bbmdTTL = int(sys.argv[3])
    main_device = sys.argv[4]
    filename = sys.argv[5]

    bacnet = BAC0.lite(ip=my_ip, bbmdAddress=bbmdIP, bbmdTTL=bbmdTTL)

    # bacnet.discover(networks='known')
    # print(bacnet.devices)

    df = pd.read_csv('input/' + filename)

    for index, row in df.iterrows():
        try:
            val = bacnet.read('%s %s %s presentValue' % (main_device, row['BACnet Type'], row['Object ID']))
            log.info("BACnet [READ]: Address: %s , Type: %s , ID: %s , Value: %s" % (
                main_device, row['BACnet Type'], row['Object ID'], val))
        except Exception as ex:
            log.error("BACnet [READ]: Address: %s , Type: %s , ID: %s" % (
                main_device, row['BACnet Type'], row['Object ID']))
    exit(0)
