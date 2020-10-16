from sysdata.private_config import get_private_config_key_value
from syscore.objects import missing_data


def load_private_key():
    """
    Tries to load IB conf

    :return: ib_ip, ib_prt
    """
    ib_ipaddress = "ib_ipaddress"
    ib_port = "ib_port"

    ib_ip = get_private_config_key_value(ib_ipaddress)
    ib_prt = get_private_config_key_value(ib_port)

    if ib_ip is missing_data:
        # no private key
        print("No private conf IP found for IB")
        ib_ip = '127.0.0.1'

    if ib_prt is missing_data:
        # no private key
        print("No private conf PORT found for IB")
        ib_prt = '7496'

    return ib_ip, ib_prt
