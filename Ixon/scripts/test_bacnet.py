import BAC0

if __name__ == '__main__':
    value = "-"
    vpn_ip = "-"
    bbdmTTL = 900
    bacnet = BAC0.connect(ip=value + '/16', bbmdAddress=vpn_ip + ':47808', bbmdTTL=900)
    bacnet.discover(networks='known')
    print(bacnet.devices)
