"""
Get data from IB for futures

"""

from sysdata.futures.contracts import futuresContract
from sysdata.futures.futures_per_contract_prices import (
    futuresContractPriceData,
    futuresContractPrices,
)
from syscore.fileutils import get_filename_for_package
from sysdata.ib.ib_utils import load_private_key

from ib_insync import *
import pandas as pd

IB_FUTURES_CONFIG_FILE = get_filename_for_package(
    "sysdata.ib.IbFuturesConfig.csv"
)

ib_ip, ib_port = load_private_key()
ib = IB()
ib.connect(ib_ip, ib_port, clientId=900)

class ibFuturesConfiguration(object):
    def __init__(self, config_file=IB_FUTURES_CONFIG_FILE):

        self._config_file = config_file

    def get_list_of_instruments(self):
        config_data = self._get_config_information()

        return list(config_data.index)

    def get_instrument_config(self, instrument_code):

        if instrument_code not in self.get_list_of_instruments():
            raise Exception(
                "Instrument %s missing from config file %s"
                % (instrument_code, self._config_file)
            )

        config_data = self._get_config_information()
        data_for_code = config_data.loc[instrument_code]

        return data_for_code

    def _get_config_information(self):
        """
        Get configuration information

        :return: dict of config information relating to self.instrument_code
        """

        try:
            config_data = pd.read_csv(self._config_file)
        except BaseException:
            raise Exception("Can't read file %s" % self._config_file)

        try:
            config_data.index = config_data.CODE
            config_data.drop("CODE", 1, inplace=True)

        except BaseException:
            raise Exception("Badly configured file %s" % (self._config_file))

        return config_data

    def get_ibcode_for_instrument(self, instrument_code):

        config = self.get_instrument_config(instrument_code)
        return config.IBCODE

    def get_ibmarket_for_instrument(self, instrument_code):

        config = self.get_instrument_config(instrument_code)
        return config.MARKET

    def get_first_contract_date(self, instrument_code):

        config = self.get_instrument_config(instrument_code)
        start_date = config.FIRST_CONTRACT

        return "%d" % start_date

    def get_ib_dividing_factor(self, instrument_code):

        config = self.get_instrument_config(instrument_code)
        factor = config.FACTOR

        return float(factor)


USE_DEFAULT = object()


class _ibFuturesContract(futuresContract):
    """
    An individual futures contract, with additional IB methods
    """

    def __init__(self, futures_contract, ib_instrument_data=USE_DEFAULT):
        """
        We always create a IB contract from an existing, normal, contract

        :param futures_contract: of type FuturesContract
        """

        super().__init__(futures_contract.instrument, futures_contract.contract_date)

        if ib_instrument_data is USE_DEFAULT:
            ib_instrument_data = ibFuturesConfiguration()

        self._ib_instrument_data = ib_instrument_data

    def ib_identifier(self):
        """
        Returns the IB identifier for a given contract

        :return: str
        """

        ib_year = str(self.contract_date.year())
        ib_month = self.contract_date.letter_month()

        try:
            ib_date_id = ib_month + ib_year

            market = self.get_ibmarket_for_instrument()
            codename = self.get_ibcode_for_instrument()

            ibdef = "%s/%s%s" % (market, codename, ib_date_id)

            return ibdef
        except BaseException:
            raise ValueError(
                "Can't turn %s %s into a IB Contract"
                % (self.instrument_code, self.contract_date)
            )

    def get_ibcode_for_instrument(self):

        return self._ib_instrument_data.get_ibcode_for_instrument(
            self.instrument_code
        )

    def get_ibmarket_for_instrument(self):

        return self._ib_instrument_data.get_ibmarket_for_instrument(
            self.instrument_code
        )

    def get_start_date(self):

        return self._ib_instrument_data.get_start_date(
            self.instrument_code)

    def get_dividing_factor(self):

        return self._ib_instrument_data.get_ib_dividing_factor(
            self.instrument_code
        )


class ibFuturesContractPriceData(futuresContractPriceData):
    """
    Class to specifically get individual futures price data for IB
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
        return self._get_prices_for_contract_object_no_checking(
            contract_object)

    def _get_prices_for_contract_object_no_checking(
            self, futures_contract_object):
        """

        :param futures_contract_object: futuresContract
        :return: futuresContractPrices
        """
        print("########::::", futures_contract_object.instrument_code, futures_contract_object.date[:-2], futures_contract_object.__dict__)
        self.log.label(
            instrument_code=futures_contract_object.instrument_code,
            contract_date=futures_contract_object.date,
        )

        try:
            ib_contract = _ibFuturesContract(futures_contract_object)
            print(ib_contract.get_ibmarket_for_instrument())
        except BaseException:
            self.log.warning(
                "Can't parse contract object to find the IB identifier"
            )
            return futuresContractPrices.create_empty()

        try:
            contract = Future(futures_contract_object.instrument_code, futures_contract_object.date[:-2], ib_contract.get_ibmarket_for_instrument())
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
            self.log.warn(
                "Can't get IB data for %s error %s"
                % (ib_contract.ib_identifier(), exception)
            )
            return futuresContractPrices.create_empty()

        try:
            data = ibFuturesContractPrices(contract_data)
        except BaseException:
            self.log.error(
                "IB API error: data fields are not as expected %s"
                % ",".join(list(contract_data.columns))
            )
            return futuresContractPrices.create_empty()

        # apply multiplier
        factor = ib_contract.get_dividing_factor()
        data = data / factor

        return data


class ibFuturesContractPrices(futuresContractPrices):
    """
    Parses IB format into our format

    Does any transformations needed to price etc
    """

    def __init__(self, contract_data):

        try:
            contract_data.columns = ['OPEN', 'HIGH', 'LOW', 'FINAL', 'VOLUME']
            contract_data=contract_data[['OPEN', 'FINAL', 'HIGH', 'LOW', 'VOLUME']]

            new_data = contract_data.copy()
            print(new_data.tail())
            # new_data = pd.DataFrame(
            #     dict(
            #         OPEN=pd.Series(contract_data.open),
            #         FINAL=pd.Series(contract_data.close),
            #         HIGH=pd.Series(contract_data.high),
            #         LOW=pd.Series(contract_data.low),
            #     )
            # )
        except AttributeError:
            try:
                new_data = pd.DataFrame(
                    dict(
                        OPEN=contract_data.OPEN,
                        FINAL=contract_data.FINAL,
                        HIGH=contract_data.HIGH,
                        LOW=contract_data.LOW,
                    )
                )
            except AttributeError:
                try:
                    new_data = pd.DataFrame(
                        dict(
                            OPEN=contract_data.Open,
                            FINAL=contract_data.Settle,
                            HIGH=contract_data.High,
                            LOW=contract_data.Low,
                        )
                    )
                except BaseException:
                    raise Exception(
                        "IB API error: data fields %s are not as expected"
                        % ",".join(list(contract_data.columns))
                    )

        super().__init__(new_data)
