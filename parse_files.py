# parse the raw data, from a combination of the flat files and the API

from file_functions import list_dir,config
from api_functions import read_drink
from sql_functions import upsert_df,engine,bars_dtype,drinks_dtype,glasses_dtype,transactions_dtype
import pandas as pd
import re
import json
import os

# to store processed data to load to the DB, one per table
pd_bars = pd.DataFrame()
pd_drinks = pd.DataFrame()
pd_glasses = pd.DataFrame()
pd_transactions = pd.DataFrame()

drink_glass_map = {} # store the drink:glass pairs (for the drinks table)

# item map for bar_data raw file
bar_item_map = {
    "GLASS_TYPE":"GLASS_NAME",
    "STOCK":"STOCK",
    "BAR":"BAR_NAME",
}

# item map for transaction files from each individual bar - does not apply to london file
trans_item_map = {
    "TS":"TRANS_TIME",
    "TIME":"TRANS_TIME",
    "ITAL":"DRINK_NAME",
    "DRINK":"DRINK_NAME",
    "KÖLTSÉG":"VALUE",
    "AMOUNT":"VALUE",
}

# timezones for each transaction file - these are used to get the correct timestamp values
timezone_map = {
    "BUDAPEST":" +1:00",
    "LONDON":" +0:00",
    "NY":" -4:00",
}

# correct BAR_NAME values for each transaction file - match from the filename
barname_map = {
    "BUDAPEST":"BUDAPEST",
    "LONDON":"LONDON",
    "NY":"NEW_YORK",
}

# overrides needed for GLASS_NAME - some of the values we get are spelled wrong
glassname_map = {
    "COPER_MUG":"COPPER_MUG",
}

# clean a dataframe
def format_df(df,item_map=None):
    df.dropna(axis="columns",how="all",inplace=True) # drop empty columns
    # if we are remapping the raw field names (london file doesn't have field names)
    if item_map:
        df.columns = df.iloc[0] # set the columns as the first row (the field names)
        df.drop(df.index[0],inplace=True) # drop the first row
        df.columns = df.columns.str.upper() # set columns to uppercase
        df.rename(columns = item_map,inplace=True) # map to correct fields
    df = df.apply(lambda x: x.astype(str).str.upper()) # convert all values to uppercase
    return df

# standardise date/time formats
def format_timestamp(df):
    re_timestamp = re.search("^(\d{2})-(\d{2})-(\d{4}) (\d{2}:\d{2})$",str(df.TRANS_TIME))
    if re_timestamp:
        formatted = str(re_timestamp.group(3))+"-"+str(re_timestamp.group(1))+"-"+str(re_timestamp.group(2)) \
            +" "+str(re_timestamp.group(4))+":00"
    else:
        formatted = df.TRANS_TIME
    return str(formatted) + str(df.TIMEZONE)

# some stock values have junk at the end, we only want the numbers
# cast as int to ensure correct summing behaviour
def format_stock(cell):
    re_stock = re.search("^(\d+)",str(cell))
    if re_stock:
        return int(re_stock.group(1))
    else:
        return int(cell)

def format_glassname(cell):
    if cell in glassname_map:
        return glassname_map[cell]
    else:
        return cell

queue = config.get("Default","QUEUE_DIR") # where the raw files are coming from
debug = config.get("Default","DEBUG") # turn this on to prevent raw file deletion/DB updating

# main process
if __name__ == "__main__":
    # process bar files to populate the glasses and bars tables
    bar_files = list_dir(queue,".+\.csv$")
    for file in bar_files:
        # read and clean up data
        filepath = queue+file
        print(f"Processing {filepath}...")
        raw_bars = pd.read_csv(filepath,engine="python",header=None)
        format_bars = format_df(raw_bars,bar_item_map)
        format_bars["STOCK"] = format_bars["STOCK"].apply(format_stock)
        for field in ["BAR_NAME","GLASS_NAME"]:
            format_bars[field] = format_bars[field].str.replace(" ","_")
        format_bars["GLASS_NAME"] = format_bars["GLASS_NAME"].apply(format_glassname)

        # in the bars table, we want to store the glass stocks as a dict (JSON in the DB)
        # e.g. for London we might have "{'BALLOON_GLASS': 34, 'BEER_GLASS': 41, 'BEER_MUG': 42, ...}"
        bar_stocks = {}
        # loop through each unique bar
        for bar,bar_data in format_bars.groupby("BAR_NAME"):
            glass_stocks = {}
            # loop through each unique glass at that bar
            for glass,stock_data in bar_data.groupby("GLASS_NAME"):
                # store the sum of stock for that glass in glass_stocks
                glass_stocks[glass] = int(stock_data["STOCK"].sum())
            # glass_stocks is then used as the value for bar_stocks
            # so we have a dict of dicts
            bar_stocks[bar] = glass_stocks
        # the keys and values are then used as the columns in the bars table
        pd_bars["BAR_NAME"] = bar_stocks.keys()
        pd_bars["BAR_STOCK"] = bar_stocks.values()

        # sum up the stock count for each glass name, across all bars - this is used in the glasses table
        pd_glasses = format_bars.groupby("GLASS_NAME").STOCK.sum().reset_index()

        # remove file from queue
        if not debug:
            os.remove(filepath)
    ## END OF BAR FILES LOOP


    # process transaction files
    # this populates the drinks and transactions tables
    trans_files = list_dir(queue,"^(ny|budapest|london_transactions)\.csv\.gz$")
    for file in trans_files:
        # read and clean up data
        filepath = queue+file
        print(f"Processing {filepath}...")

        # get city from filename, london files are differently formatted
        re_city = re.search("^(\w+)\.csv\.gz$",file).group(1).upper()
        if re_city == "LONDON_TRANSACTIONS":
            city = "LONDON"
            raw_trans = pd.read_csv(filepath,engine="python",header=None,sep="\t",names=["TRANS_TIME","DRINK_NAME","VALUE"])
            format_trans = format_df(raw_trans)
        else:
            city = re_city
            raw_trans = pd.read_csv(filepath,engine="python",header=None)
            format_trans = format_df(raw_trans,trans_item_map)

        # use the city name to add timezone info and populate BAR_NAME
        # if the city has not yet been mapped (or we don't find one), we can't process the file
        if city in timezone_map:
            format_trans["TIMEZONE"] = timezone_map[city]
            format_trans["BAR_NAME"] = barname_map[city]
        else:
            print(f"City not found for {filepath}. Removing file from queue.")
            os.remove(filepath)
            continue
        format_trans["TRANS_TIME"] = format_trans[["TRANS_TIME","TIMEZONE"]].apply(format_timestamp,axis=1)

        format_trans["DRINK_NAME"] = format_trans["DRINK_NAME"].str.replace(" ","_")
        drink_names = format_trans["DRINK_NAME"].unique()

        # add to the transactions data so far
        # concat is needed as we source from various transaction files
        pd_transactions = pd.concat([pd_transactions,format_trans[["BAR_NAME","DRINK_NAME","VALUE","TRANS_TIME"]]],ignore_index=True)

        # loop through each unique drink, we need to get the glass name for each one from the API
        for drink in drink_names:
            drink_json = read_drink(drink) # call the API using the drink name, to return JSON data on the drink
            drink_python = json.loads(drink_json) # convert to python
            drink_glass_map[drink] = str(drink_python["drinks"][0]["strGlass"]).upper().replace(" ","_") # get the glass name for the drink

        # remove file from queue
        if not debug:
            os.remove(filepath)
    ## END OF TRANS FILES LOOP


    # populate drinks table from the keys and values in drink_glass_map
    pd_drinks["DRINK_NAME"] = drink_glass_map.keys()
    pd_drinks["GLASS_NAME"] = drink_glass_map.values()

    pd_transactions["TRANS_ID"] = pd_transactions.index # set TRANS_ID for the transactions table

    # set indexes for each dataframe - these are passed as the table indexes to upsert_df()
    pd_bars.set_index("BAR_NAME",inplace=True)
    pd_drinks.set_index("DRINK_NAME",inplace=True)
    pd_glasses.set_index("GLASS_NAME",inplace=True)
    pd_transactions.set_index("TRANS_ID",inplace=True)

    # upsert the data for each table to the DB
    if not debug:
        with engine.connect() as con:
            upsert_df(df=pd_bars,table_name="bars",engine=con,dtype=bars_dtype)
            upsert_df(df=pd_glasses,table_name="glasses",engine=con,dtype=glasses_dtype)
            upsert_df(df=pd_drinks,table_name="drinks",engine=con,dtype=drinks_dtype)
            upsert_df(df=pd_transactions,table_name="transactions",engine=con,dtype=transactions_dtype)
            con.commit()