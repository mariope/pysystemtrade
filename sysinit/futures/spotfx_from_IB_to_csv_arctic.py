"""
Get FX prices from arctic and write to csv

WARNING WILL OVERWRITE EXISTING!
"""

"""
Get FX prices from investing.com files, and from csv, merge and write to Arctic and/or optionally overwrite csv files
"""
from sysdata.arctic.arctic_spotfx_prices import arcticFxPricesData
from sysdata.csv.csv_spot_fx import csvFxPricesData
import pandas as pd

ADD_TO_ARCTIC = True
ADD_TO_CSV = True

# You may need to change this!
# There must be ONLY fx prices here, with filenames "GBPUSD.csv" etc
IB_DATA_PATH = "data.ib.fx_prices_csv"

if __name__ == "__main__":
    # You can adapt this for different providers by changing these parameters
    investingDotCom_csv_fx_prices = csvFxPricesData(
        datapath=IB_DATA_PATH,
        price_column="Price",
        date_column="Date",
        date_format="%b %d, %Y",
    )
    if ADD_TO_ARCTIC:
        arctic_fx_prices = arcticFxPricesData()
    my_csv_fx_prices = csvFxPricesData()

    list_of_ccy_codes = investingDotCom_csv_fx_prices.get_list_of_fxcodes()

    for currency_code in list_of_ccy_codes:

        print(currency_code)

        fx_prices_investingDotCom = investingDotCom_csv_fx_prices.get_fx_prices(
            currency_code)
        fx_prices_my_csv = my_csv_fx_prices.get_fx_prices(currency_code)
        print(
            "%d rows for my csv files, %d rows for investing.com"
            % (len(fx_prices_my_csv), len(fx_prices_investingDotCom))
        )
        # Merge;
        last_date_in_my_csv = fx_prices_my_csv.index[-1]
        fx_prices_investingDotCom = fx_prices_investingDotCom[last_date_in_my_csv:]
        fx_prices = pd.concat([fx_prices_my_csv, fx_prices_investingDotCom])
        fx_prices = fx_prices.loc[~fx_prices.index.duplicated(keep="first")]

        print("%d rows to write for %s" % (len(fx_prices), currency_code))

        if ADD_TO_CSV:
            my_csv_fx_prices.add_fx_prices(
                currency_code, fx_prices, ignore_duplication=True
            )

        if ADD_TO_ARCTIC:
            arctic_fx_prices.add_fx_prices(
                currency_code, fx_prices, ignore_duplication=True
            )





################################################################################


from sysdata.arctic.arctic_spotfx_prices import arcticFxPricesData
from sysdata.csv.csv_spot_fx import csvFxPricesData

if __name__ == "__main__":
    arctic_fx_prices = arcticFxPricesData()
    csv_fx_prices = csvFxPricesData(datapath = 'data.ib.fx_prices_csv')

    list_of_ccy_codes = csv_fx_prices.get_list_of_fxcodes()

    for currency_code in list_of_ccy_codes:
        fx_prices = arctic_fx_prices.get_fx_prices(currency_code)
        print(fx_prices)

        csv_fx_prices.add_fx_prices(
            currency_code, fx_prices, ignore_duplication=True)
