"""
For a given list of undated instruments defined by Oanda start dates:

read price data from oanda, and then write to artic
Write list of undated instruments to mongodb database
"""
from sysdata.oanda.oanda_cfd import oandaFuturesContractPriceData

from sysdata.arctic.arctic_adjusted_prices import arcticFuturesAdjustedPricesData
from sysdata.csv.csv_adjusted_prices import csvFuturesAdjustedPricesData

from sysdata.csv.csv_instrument_config import csvFuturesInstrumentData
INSTRUMENT_CONFIG_PATH = "sysdata.oanda"
INSTRUMENT_CSVDATA_PATH = "data.oanda"

def get_and_write_prices_for_contract_list_from_oanda_to_arctic(instrument_code):
    oanda_prices_data = oandaFuturesContractPriceData()
    arctic_adjusted_prices = arcticFuturesAdjustedPricesData()
    csv_adjusted_prices = csvFuturesAdjustedPricesData(INSTRUMENT_CSVDATA_PATH)

    adjusted_prices = oanda_prices_data.get_prices_for_contract_object(instrument_code)

    if adjusted_prices.empty:
        print("Problem reading price data this contract - skipping")
    else:
        print("Read ok, trying to write to arctic")
        try:
            # arctic_adjusted_prices.add_adjusted_prices(instrument_code, adjusted_prices, ignore_duplication=True)
            csv_adjusted_prices.add_adjusted_prices(instrument_code, adjusted_prices, ignore_duplication=True)
        except:
            raise Exception("Some kind of issue with arctic - stopping so you can fix it")


if __name__ == '__main__':

    data_in = csvFuturesInstrumentData(datapath=INSTRUMENT_CONFIG_PATH)
    instrument_list = data_in.get_list_of_instruments()

    for instrument_code in instrument_list:
        get_and_write_prices_for_contract_list_from_oanda_to_arctic(instrument_code)
        # check
        print("Added %s " % instrument_code)
