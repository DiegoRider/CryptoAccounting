import pandas as pd
import numpy as np
from pycoingecko import CoinGeckoAPI
cg = CoinGeckoAPI()

pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

def match_price(row, prices_df):
    if row["TokenSymbol"] == "EUR":
        return 1
    if row.cg_id is not None:
        return prices_df[prices_df["DateString"] == row["DateString"]][row.cg_id].values[0]
    else:
        return None
    
def get_tx_fee(row):

    if row.get("TxnFee(ETH)",0) > 0:
        assert row.TokenName == "Ethereum" 
        return row["TxnFee(ETH)"] * row["TokenPriceEuro"]
    else:
        return None

def is_my_wallet(address, platform):
    if platform in ["ethereum", "arbitrum"]:
        return (address == "my_wallet")
    else:
        raise NotImplementedError

def is_in(row):
    return is_my_wallet(row.To, row.Platform)    

def is_out(row):
    return is_my_wallet(row.From, row.Platform)  

def is_nft(row):
    return np.isnan(row.Amount) and row["ExportType"] == "erc721"

def is_priced(row):
    return (row.ValueEuro > 0)

def is_unpriced(row):
    return (row.Amount > 0) & (row.cg_id is None)

def is_priced_token_in(row):
    return is_priced(row) & is_in(row)

def is_priced_token_out(row):
    return is_priced(row) & is_out(row)

def is_unpriced_token_in(row):
    return is_unpriced(row) & is_in(row)

def is_unpriced_token_out(row):
    return is_unpriced(row) & is_out(row)

def is_nft_in(row):
    return is_nft(row) & is_in(row)

def is_nft_out(row):
    return is_nft(row) & is_out(row)

def show(tx_df, column, value):
    return tx_df[tx_df[column] == value]

def show_lambda(tx_df, filter):
    return tx_df[tx_df.apply(lambda row: filter(row), axis=1)]

def show_tx_with(tx_df, column, value):
    txs = tx_df[tx_df[column] == value]["Hash"].unique()
    return tx_df[tx_df["Hash"].isin(txs)]

def get_platform(tx_rows):
    platforms = tx_rows["Platform"].unique()
    assert len(platforms) == 1
    return platforms[0]

def get_method(tx_rows):
    
    methods = tx_rows["Method"]
    methods = methods[methods.notnull()]

    if get_platform(tx_rows)=="bitstamp":
        methods = methods.unique()
    else:
        methods = methods.values
    
    if len(methods) == 0:
        return None
    elif len(methods) == 1:
        method = methods[0]
        return method
    else:
        print(methods)
        raise Exception(f"multiple methods for transaction")
        
def get_contract_id(tx_rows):

    if get_platform(tx_rows) == "bitstamp":
        return "bitstamp"
    
    contract_ids = tx_rows[tx_rows.Method.notnull()]["To"]
    
    if len(contract_ids) == 0:
        return None
    elif len(contract_ids) == 1:
        return contract_ids.values[0]
    else:
        print(contract_ids)
        raise Exception(f"multiple possible contract_ids for transaction")

def get_token_id2current_price(prices_df, tokens, time_end):
    current_prices = prices_df[prices_df["DateString"] == get_today_string(time_end)].iloc[0].reset_index()
    current_prices.columns = ["cg_id", "current_price"]
    current_prices = current_prices[~current_prices["cg_id"].isin(["timestamp", "date", "DateString"]) ]
    tokens_df = tokens.merge(current_prices, on="cg_id", how="left")
    tokens_df["token_id"] = tokens_df.apply(get_token_id, axis=1)
    token_id2current_price = {row.token_id: row.current_price for i, row in tokens_df[tokens_df["current_price"].notnull()].iterrows()}
    token_id2current_price["EUR"] = 1
    return token_id2current_price

def merge_tx_df_with_prices(tx_df, tokens, prices_df):

    tx_df = tx_df.merge(tokens, on=["TokenName", "TokenSymbol"], how="left")
    tx_df.sort_values("TimeStamp", inplace=True)
    tx_df.reset_index(drop="True", inplace=True)
    tx_df["DateString"] = tx_df.TimeStamp.apply(lambda x:pd.to_datetime(x, unit="s").strftime("%Y-%m-%d"))
    tx_df["TokenPriceEuro"] = tx_df.apply(lambda row: match_price(row, prices_df), axis=1)
    tx_df["ValueEuro"] = tx_df.apply(lambda x: x["TokenPriceEuro"]*x["Amount"], axis=1)
    tx_df["TxnFee(Euro)"] = tx_df.apply(get_tx_fee, axis=1)
    return tx_df

def get_token_id(row):
    return row.TokenSymbol         

def dict_union_sum(d1, d2):
            return {k: d1.get(k, 0) + d2.get(k, 0) for k in set(d1) | set(d2)} 

def capitalize(string):
    return string[0].upper() + string[1:] if len(string) > 0 else string

def get_today_string(time):
    return time.strftime("%Y-%m-%d")
