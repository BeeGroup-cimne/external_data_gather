import sys

import BAC0
import pandas as pd

from Ixon.logger import setup_logger

if __name__ == '__main__':

    """
    Primer de Maig: 10.187.10.1/16 10.187.113.206:47808 9000 10.81.182.10 primer_maig.csv
    Flix: 10.187.10.1/16 10.187.43.146:47808 9000 192.168.140.99 template.csv
    """

    # Init config
    log = setup_logger('bacnet_test')
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
