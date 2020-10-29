"""
Get data from oanda for CFDs

"""
from sysdata.futures.contracts import futuresContract
from sysdata.futures.futures_per_contract_prices import futuresContractPriceData, futuresContractPrices
from syscore.fileutils import get_filename_for_package
from sysdata.ib.ib_utils import load_private_key

from ib_insync import *
import pandas as pd
# util.startLoop()

IB_UNDATED_CFD_CONFIG_FILE = get_filename_for_package(
    "sysdata.ib.undated_cfd_conf.IbUndatedCFDConfig.csv"
)

ib_ip, ib_port = load_private_key()
ib = IB()
ib.connect(ib_ip, ib_port, clientId=905)

class ibUndatedCFDPriceData(futuresContractPriceData):
    """
    Class to specifically get individual futures price data for ib
    """

    def __init__(self):

        super().__init__()

        self.name = "simData connection for individual futures contracts prices, IB"

    def __repr__(self):
        return self.name

    def get_prices_for_contract_object(self, contract_object):
        """
        We do this because we have no way of checking if IB has something without actually trying to get it
        """
        return self._get_prices_for_contract_object_no_checking(contract_object)

    def _get_prices_for_contract_object_no_checking(self, futures_contract_object):
        """

        :param futures_contract_object: futuresContract
        :return: futuresContractPrices
        """
        self.log.label(instrument_code=futures_contract_object)
        print("futures_contract_object: ", futures_contract_object)
        print("futures_contract_object: ", type(futures_contract_object))
        try:
            contract = CFD(futures_contract_object, currency='USD', exchange='SMART')
            bars = ib.reqHistoricalData(contract,
                                        endDateTime='',
                                        durationStr='10 Y',
                                        barSizeSetting='1 day',
                                        whatToShow='TRADES',
                                        useRTH=True,
                                        formatDate=1)
            contract_data = util.df(bars)
            contract_data.drop(['average', 'barCount'], axis=1, inplace=True)
            contract_data.columns = ['DATETIME', 'open', 'high', 'low', 'close', 'volume']
            contract_data = contract_data.set_index('DATETIME')
            print(contract_data.tail())

        except Exception as exception:
            self.log.warn("Can't get IB data for %s error %s" % (futures_contract_object, exception))
            return futuresContractPrices.create_empty()

        try:
            data = ibUndatedCFDPrices(contract_data)

        except:
            self.log.error(
                "IB API error: data fields are not as expected %s" % ",".join(list(df.columns)))
            return futuresContractPrices.create_empty()

        # apply multiplier
        factor = oanda_contract.get_dividing_factor()
        data = data / factor

        return df

USE_DEFAULT = object()

class ibUndatedCFDPrices(futuresContractPrices):
    """
    Parses IB format into our format

    Does any transformations needed to price etc
    """

    def __init__(self, contract_data):
        print(contract_data.tail(3))
        try:
            print("trying try")
            contract_data.columns = ['OPEN', 'HIGH', 'LOW', 'FINAL', 'VOLUME']
            contract_data=contract_data[['OPEN', 'FINAL', 'HIGH', 'LOW', 'VOLUME']]
            new_data = contract_data.copy()
            # new_data = pd.DataFrame(dict(OPEN=contract_data['Open'],
            #                              FINAL=contract_data['Close'],
            #                              HIGH=contract_data['High'],
            #                              LOW=contract_data['Low']))
            print("NEWDATA")
            print(new_data.tail(2))
            print("end trying try")
        except AttributeError:
            try:
                new_data = pd.DataFrame(dict(OPEN=contract_data.Open,
                                         FINAL=contract_data.Close,
                                         HIGH=contract_data.High,
                                         LOW=contract_data.Low))
            except AttributeError:
                try:
                    new_data = pd.DataFrame(dict(OPEN=contract_data.Open,
                                                 FINAL=contract_data.Settle,
                                                 HIGH=contract_data.High,
                                                 LOW=contract_data.Low))
                except:
                    raise Exception(
                        "IB API error: data fields %s are not as expected" % ",".join(list(contract_data.columns)))

        super().__init__(new_data)
