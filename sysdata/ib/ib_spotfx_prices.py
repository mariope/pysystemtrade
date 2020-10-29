"""
Get FX data from IB for currencies
"""
from sysdata.ib.ib_utils import load_private_key
from sysdata.fx.spotfx import fxPricesData, fxPrices
from syscore.fileutils import get_filename_for_package

from ib_insync import *
import pandas as pd

ib_ip, ib_port = load_private_key()
ib = IB()
ib.connect(ib_ip, ib_port, clientId=900)

NOT_IN_IB_MSG = "You can't add, delete, or get a list of codes for Quandl FX data"
IB_CCY_CONFIG_FILE = get_filename_for_package(
    "sysdata.ib.IbFXConfig.csv")


class ibFxPricesData(fxPricesData):
    def __repr__(self):
        return "Quandl FX price data"

    def _get_fx_prices_without_checking(self, currency_code):
        ibcode = self._get_ibcode(currency_code)
        try:
            contract = Forex(ibcode)
            bars = ib.reqHistoricalData(contract,
                                        endDateTime='',
                                        durationStr='10 Y',
                                        barSizeSetting='1 day',
                                        whatToShow='TRADES',
                                        useRTH=True,
                                        formatDate=1)
            contract_data = util.df(bars)
            # contract_data.drop(['average', 'barCount'], axis=1, inplace=True)
            # contract_data.columns = ['DATETIME', 'open', 'high', 'low', 'close', 'volume']
            # contract_data = contract_data.set_index('DATETIME')
        except Exception as exception:
            self.log.warn(
                "Can't get IB data for %s error %s" %
                (ibcode, exception))
            return fxPrices.create_empty()

        fx_prices = fx_prices.Rate

        return fxPrices(fx_prices)

    def get_list_of_fxcodes(self):
        config_data = self._get_ib_fx_config()
        return list(config_data.CODE)

    def _get_ibcode(self, currency_code):
        config_data = self._get_ib_fx_config()
        ibcode = config_data[config_data.CODE == currency_code].IBCODE.values[0]

        return ibcode

    def _get_ib_fx_config(self):
        try:
            config_data = pd.read_csv(IB_CCY_CONFIG_FILE)
        except BaseException:
            raise Exception("Can't read file %s" % IB_CCY_CONFIG_FILE)

        return config_data
