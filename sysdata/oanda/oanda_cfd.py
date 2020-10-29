"""
Get data from oanda for CFDs

"""
from sysdata.futures.contracts import futuresContract
from sysdata.futures.futures_per_contract_prices import futuresContractPriceData, futuresContractPrices
from syscore.fileutils import get_filename_for_package
from sysdata.ib.ib_utils import load_private_key

import json
import oandapyV20
from oandapyV20 import API
import oandapyV20.endpoints.instruments as v20instruments
import oandapyV20.endpoints.accounts as accounts

import pandas as pd

oanda_token, oanda_id = load_private_key()

class oandaFuturesContractPriceData(futuresContractPriceData):
    """
    Class to specifically get individual futures price data for oanda
    """

    def __init__(self):

        super().__init__()

        self.name = "simData connection for individual futures contracts prices, Oanda"
        self.client = oandapyV20.API(access_token=oanda_token)

    def __repr__(self):
        return self.name

    def get_prices_for_contract_object(self, contract_object):
        """
        We do this because we have no way of checking if OANDA has something without actually trying to get it
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
            v = v20instruments.InstrumentsCandles(instrument=futures_contract_object,
                                                  params={"granularity": 'D', "from": '2000-01-01', "count": 5000}) # count = [default=500, maximum=5000]
            dict = self.client.request(v)
            df = pd.DataFrame.from_records(dict['candles'])
            print(futures_contract_object)
            print(df.head(3))
            print(df.tail(3))
            print("3.1")
            # print(df['mid'].apply(pd.Series))
            # print(df)
            df = pd.concat([df.drop(['mid'], axis=1), df['mid'].apply(pd.Series)], axis=1)
            print("3.2")

            # df['DATETIME'] = pd.to_datetime(df.time)
            # df = df.set_index('DATETIME')
            # cols = df.columns.tolist()
            # print(cols)
            # cols = cols[-4:] + cols[:-4]
            # df = df[cols]
            # ['complete', 'volume', 'time', 'o', 'h', 'l', 'c']
            # df.columns = ['DATETIME', 'Open', 'High', 'Low', 'price', 'Complete', 'Volume']
            # df.drop(['complete', 'volume'], axis=1, inplace=True)
            df.drop(['complete', 'volume', 'o', 'h', 'l'], axis=1, inplace=True)
            # df.columns = ['DATETIME', 'Open', 'High', 'Low', 'Close']
            # df.columns = ['DATETIME', 'OPEN', 'FINAL', 'HIGH', 'LOW'] # bad order !!!!
            df.columns = ['DATETIME', 'price']
            df['DATETIME'] = df['DATETIME'].str[:10]+' '+df['DATETIME'].str[11:19]
            df = df.set_index('DATETIME')
            print(df.tail())
            print("4")

            # start = df.index[-1].replace(tzinfo=None)
            # beginat=str(start.strftime("%Y-%m-%d"))
            # print("5")

        except Exception as exception:
            self.log.warn("Can't get OANDA data for %s error %s" % (futures_contract_object, exception))
            return futuresContractPrices.create_empty()

        # try:
            # data = oandaFuturesContractPrices(df)

        # except:
        #     self.log.error(
        #         "Oanda API error: data fields are not as expected %s" % ",".join(list(df.columns)))
        #     return futuresContractPrices.create_empty()

        # # apply multiplier
        # factor = oanda_contract.get_dividing_factor()
        # data = data / factor

        return df

USE_DEFAULT = object()

class oandaFuturesContractPrices(futuresContractPrices):
    """
    Parses Barchart format into our format

    Does any transformations needed to price etc
    """

    def __init__(self, contract_data):
        print(contract_data.tail(3))
        try:
            print("trying try")
            # contract_data.columns = ['OPEN', 'FINAL', 'HIGH', 'LOW']

            contract_data.columns = ['OPEN', 'HIGH', 'LOW', 'FINAL']
            contract_data=contract_data[['OPEN', 'FINAL', 'HIGH', 'LOW']]

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
                        "Oanda API error: data fields %s are not as expected" % ",".join(list(contract_data.columns)))

        super().__init__(new_data)
