import BAC0

if __name__ == '__main__':
    value = "10.187.10.14"
    vpn_ip = "10.187.130.76"
    bbdmTTL = 900
    bacnet = BAC0.connect(ip=value + '/16', bbmdAddress=vpn_ip + ':47808', bbmdTTL=900)
    bacnet.discover(networks='known')
    devices = bacnet.devices
    device_value = bacnet.read(f"{device['bacnet_device_ip']} {device['type']} {device['object_id']} presentValue")