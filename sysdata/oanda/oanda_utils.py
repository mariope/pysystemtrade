
import yaml

from syscore.fileutils import get_filename_for_package

OANDA_PRIVATE_KEY_FILE = get_filename_for_package("private.private_config.yaml")

def load_private_key(key_file=OANDA_PRIVATE_KEY_FILE):
    """
    Tries to load a private key

    :return: key
    """

    try:
        with open(key_file) as file_to_parse:
            yaml_dict = yaml.load(file_to_parse)
        oanda_token = yaml_dict["oanda_api"]
        oanda_id = yaml_dict["oanda_id"]
    except:
        # no private key
        print("No private key found for OANDA - you will be subject to data limits")
        oanda_token = None
        oanda_id = None

    return oanda_token, oanda_id
