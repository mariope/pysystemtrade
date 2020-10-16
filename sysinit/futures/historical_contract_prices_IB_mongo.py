"""
For a given list of futures contracts defined by IB start dates:

read price data from IB, and then write to artic
Write list of futures contracts to mongodb database
"""

from sysdata.ib.ib_futures import (
    ibFuturesConfiguration,
    ibFuturesContractPriceData,
)
from sysdata.futures.contracts import listOfFuturesContracts
from sysdata.futures.instruments import futuresInstrument
from sysdata.mongodb.mongo_futures_instruments import mongoFuturesInstrumentData
from sysdata.mongodb.mongo_roll_data import mongoRollParametersData
from sysdata.arctic.arctic_futures_per_contract_prices import (
    arcticFuturesContractPriceData,
)

from sysdata.csv.csv_instrument_config import csvFuturesInstrumentData
INSTRUMENT_CONFIG_PATH = "sysdata.ib"

def get_roll_parameters_from_mongo(instrument_code):

    mongo_roll_parameters = mongoRollParametersData()

    roll_parameters = mongo_roll_parameters.get_roll_parameters(
        instrument_code)
    if roll_parameters.empty():
        raise Exception(
            "Instrument %s missing from %s" %
            (instrument_code, mongo_roll_parameters))

    return roll_parameters


def get_first_contract_date_from_ib(instrument_code):
    config = ibFuturesConfiguration()
    return config.get_first_contract_date(instrument_code)


def create_list_of_contracts(instrument_code):
    instrument_object = futuresInstrument(instrument_code)
    print(instrument_code)
    roll_parameters = get_roll_parameters_from_mongo(instrument_code)
    first_contract_date = get_first_contract_date_from_ib(instrument_code)

    print("roll_parameters:", roll_parameters)
    print("first_contract_date:", first_contract_date)

    list_of_contracts = listOfFuturesContracts.historical_price_contracts(
        instrument_object, roll_parameters, first_contract_date
    )

    return list_of_contracts


def get_and_write_prices_for_contract_list_from_ib_to_arctic(
        list_of_contracts):
    ib_prices_data = ibFuturesContractPriceData()
    arctic_prices_data = arcticFuturesContractPriceData()

    for contract_object in list_of_contracts:
        print("Processing %s" % contract_object.ident())
        ib_price = ib_prices_data.get_prices_for_contract_object(
            contract_object
        )
        print("###### ib_price ########")
        print(ib_price.tail())
        print("###### end ib_price ########")
        if ib_price.empty:
            print("Problem reading price data this contract - skipping")
        else:
            print("Read ok, trying to write to arctic")
            try:
                arctic_prices_data.write_prices_for_contract_object(
                    contract_object, ib_price
                )
            except BaseException:
                raise Exception(
                    "Some kind of issue with arctic - stopping so you can fix it"
                )


if __name__ == "__main__":
    # instrument_list = ["MES"]

    data_in = csvFuturesInstrumentData(datapath=INSTRUMENT_CONFIG_PATH)
    instrument_list = data_in.get_list_of_instruments()

    for instrument_code in instrument_list:
        print("Trying...", instrument_code)
        list_of_contracts = create_list_of_contracts(instrument_code)
        print(list_of_contracts)

        print("Generated %d contracts" % len(list_of_contracts))

        get_and_write_prices_for_contract_list_from_ib_to_arctic(
            list_of_contracts)
