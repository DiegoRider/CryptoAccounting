import os
import pandas as pd
from etherscan import Etherscan
import requests
import time
from datetime import datetime
from eth_utils.abi import function_abi_to_4byte_selector
from web3_input_decoder.utils import (
    hex_to_bytes,
)
import pickle
import json
import urllib.request
from pycoingecko import CoinGeckoAPI
import numpy as np
from .utils import get_today_string, capitalize

ARBISCAN_TOKEN = "to_set"
ETHERSCAN_TOKEN = "to_set"

def get_or_load_etherscan_dfs(time_start, time_end, data_dir, eth_address, explorer="etherscan"):
    today_string = get_today_string(time_end)

    eth = None

    etherscan_types = ("normal", "erc20", "internal", "erc721")
    etherscan_dfs = {}

    if not os.path.exists(data_dir + today_string):
        os.mkdir(data_dir + today_string)

    if not os.path.exists(data_dir + today_string + "/" + explorer):
        os.mkdir(data_dir + today_string + "/" + explorer)

    block_start = None
    block_end = None

    for etherscan_type in etherscan_types:
        filepath = data_dir + today_string + f"/{explorer}/{etherscan_type}_{eth_address}_df.pickle"
        if not os.path.exists(filepath):
            print(f"fetching {etherscan_type} transactions from {explorer}")
            time.sleep(0.5)

            if explorer == "etherscan":
                if eth is None:
                    eth = Etherscan(ETHERSCAN_TOKEN)
                if block_start is None:
                    block_start = eth.get_block_number_by_timestamp(timestamp=int(time_start.timestamp()), closest='after')     
                if block_end is None:
                    block_end = eth.get_block_number_by_timestamp(timestamp=int(time_end.timestamp()), closest='after')

                if etherscan_type == "normal":
                    output = eth.get_normal_txs_by_address(address=eth_address, startblock=block_start, endblock=block_end, sort="asc") 
                elif etherscan_type == "erc20":
                    output = eth.get_erc20_token_transfer_events_by_address(address=eth_address, startblock=block_start, endblock=block_end, sort="asc") 
                elif etherscan_type == "internal":
                    output = eth.get_internal_txs_by_address(address=eth_address, startblock=block_start, endblock=block_end, sort="asc") 
                elif etherscan_type == "erc721":
                    output = eth.get_erc721_token_transfer_events_by_address(address=eth_address, startblock=block_start, endblock=block_end, sort="asc") 

            elif explorer == "arbiscan":
                if block_start is None:
                    block_start = get_arbitrum_block_from_timestamp(int(time_start.timestamp()))   
                if block_end is None:
                    block_end = get_arbitrum_block_from_timestamp(int(time_end.timestamp()))   

                if etherscan_type == "normal":
                    request= f"https://api.arbiscan.io/api?module=account&action=txlist&address={eth_address}&startblock={block_start}&endblock={block_end}&sort=asc&apikey={ARBISCAN_TOKEN}"
                elif etherscan_type == "erc20":
                    request= f"https://api.arbiscan.io/api?module=account&action=tokentx&address={eth_address}&startblock={block_start}&endblock={block_end}&sort=asc&apikey={ARBISCAN_TOKEN}"
                elif etherscan_type == "internal":
                    request= f"https://api.arbiscan.io/api?module=account&action=txlistinternal&address={eth_address}&startblock={block_start}&endblock={block_end}&sort=asc&apikey={ARBISCAN_TOKEN}"
                elif etherscan_type == "erc721":
                    request= f"https://api.arbiscan.io/api?module=account&action=tokennfttx&address={eth_address}&startblock={block_start}&endblock={block_end}&sort=asc&apikey={ARBISCAN_TOKEN}"

                r = requests.get(request)
                if r.status_code == 200:
                    output = r.json()["result"] 
                else:
                    raise Exception(f"request error {request}")

            else:
                raise Exception(f"unknown explorer type")

            output_df = pd.DataFrame.from_dict(output)
            output_df.to_pickle(filepath)

            etherscan_dfs[etherscan_type] = output_df
                
        else:
            etherscan_dfs[etherscan_type] = pd.read_pickle(filepath)

    return etherscan_dfs



def get_arbitrum_block_from_timestamp(timestamp):
    request=f"https://api.arbiscan.io/api?module=block&action=getblocknobytime&timestamp={timestamp}&closest=before&apikey={ARBISCAN_TOKEN}"
    r = requests.get(request)
    if r.status_code == 200:
        return r.json()["result"]
    else:
        print("ERROR")



# Some contracts redirect to another contract through "implementation" value. But sometimes etherscan can't read the value on the contract. 
# manual_proxies allows to manually definie to which contract an address redirects.
manual_proxies = {
    "0xc581b735a1688071a1746c968e0798d642ede491":"0xe6a2c1642455ce65d07abb417a461c6e1bed47a1",
}


def get_ethereum_contract_method(contract_address, tx_input, platform="ethereum"):

    if tx_input == '0x' or contract_address == "0x000000000000000000000000000000000000006E".lower():
        return 'transfer'

    contract_address = manual_proxies.get(contract_address, contract_address)

    abi = get_contract_abi(contract_address, platform)
    if abi is None:
        return "read_abi_error"
    implementation = None

    selector_to_type_def = {}
    for type_def in abi:
        if type_def["type"] == "function":
            selector = function_abi_to_4byte_selector(type_def)
            selector_to_type_def[selector] = type_def

            if type_def["name"] == "implementation":
                implementation = selector.hex()

    tx_input = hex_to_bytes(tx_input)
    selector, args = tx_input[:4], tx_input[4:]

    if selector not in selector_to_type_def:
        if implementation is not None:

            filepath = f"./data/contracts_{platform}/proxy_eth_call_{contract_address}_{implementation}.pickle"
            if platform == "ethereum":
                request = f"https://api.etherscan.io/api?module=proxy&action=eth_call&to={contract_address}&data=0x{implementation}&apikey={ETHERSCAN_TOKEN}"
            elif platform == "arbitrum":
                request = f"https://api.arbiscan.io/api?module=proxy&action=eth_call&to={contract_address}&data=0x{implementation}&apikey={ARBISCAN_TOKEN}"

            result = cached_request(filepath, request)
            if result is not None:
                impl_address = "0x" + result[-40:]
                return get_ethereum_contract_method(impl_address, tx_input)
            else:
                return "read_implementation_error"
        else:
            return "not_found_error"

    method_name = selector_to_type_def[selector]['name']
    return method_name

def get_contract_abi(contract_address, platform="ethereum"):

    folder_path = "./data/contracts_" + platform

    if not os.path.exists(folder_path):
            os.mkdir(folder_path)

    filepath = folder_path + f"/abi_" + contract_address + ".pickle"        
    if platform == "ethereum":
        request = f"https://api.etherscan.io/api?module=contract&action=getabi&address={contract_address}&apikey={ETHERSCAN_TOKEN}"
    else:
        request = f"https://api.arbiscan.io/api?module=contract&action=getabi&address={contract_address}&apikey={ARBISCAN_TOKEN}"

    result = cached_request(filepath, request)
    if result and result != "Invalid Address format":
        abi = json.loads(result)
        return abi
    else:
        return None

def cached_request(filepath, request, sleep=0.25):

    if os.path.exists(filepath):
        return pickle.load(open(filepath, 'rb'))

    else:

        response = urllib.request.urlopen(request)
        time.sleep(sleep)

        if response.status == 200:
            output = json.load(response)
            if "result" in output.keys():
                output = output["result"]
                pickle.dump(output, open(filepath, 'wb'))
                return output

def combine_etherscan_dfs(etherscan_dfs, eth_address, platform="ethereum"):

    for key in etherscan_dfs.keys():
        df = etherscan_dfs[key]
        df.rename(columns={x:capitalize(x) for x in df.columns}, inplace=True)

        df["Platform"] = platform
        df["ExportType"] = key
        
        if key == "normal":
            df["TxnFee(ETH)"] = df.apply(lambda x: int(x.GasPrice) * int(x.GasUsed) / 1e18, axis=1)

            df["Method"] = None
            for i, row in df.iterrows():
                method = get_ethereum_contract_method(row.To, row.Input, platform)
                df.at[i, "Method"] = method

            errors = df[df["Method"].apply(lambda x: "error" in x)]
            if len(errors)>0:
                print(f"method errors:" + errors["To"].unique())

        if key == "erc20":
            df["Amount"] = df.apply(lambda row: int(row.Value) / 10**(int(row.TokenDecimal)), axis=1)

        if key in ["normal", "internal"] :
            df["Amount"] = df["Value"].apply(lambda x: int(x) / 1e18)
            df["TokenName"] = "Ethereum"
            df["TokenSymbol"] = "ETH"

        etherscan_dfs[key] = df
        
    name_replacements = {
    "stETH": "Lido Staked Ether"
    }
    # todo fetch automatically contract names
    address_dict = {
    eth_address.lower(): "my_wallet",
    "0xd18140b4b819b895a3dba5442f959fa44994af50": "CVX Locker",
    "0x3fe65692bfcd0e6cf84cb1e7d24108e434a7587e": "cvxCRV Locker",
    "0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f": "arbitrum_bridge_l1",
    "0x72a19342e8f1838460ebfccef09f6585e32db86e": "vlCVX",
    "0x9e3382ca57f4404ac7bf435475eae37e87d1c453": "Eden Network: Proxy",
    "0xf403c135812408bfbe8713b5a23a04b3d48aae31": "Convex Finance: Booster",
    "0xc36442b4a4522e871399cd717abdd847ab11fe88": "Uniswap V3: Positions NFT",
    "0x9008d19f58aabd9ed0d60971565aa8510560ab41": "CoW Protocol: GPv2Settlement",
    "0x000000000000000000000000000000000000006e": "arbitrum_bridge_l2",
    "0x2c9c1e9b4bdf6bf9cb59c77e0e8c0892ce3a9d5f": "Dopex: ETH SSOV"
    }   
    unused_columns = ["Nonce", "BlockHash", "TransactionIndex", "Gas", "GasPrice", "Input", "ContractAddress", "CumulativeGasUsed", "GasUsed", "Confirmations", "Value", "TokenDecimal", "TraceId", "IsError", "Txreceipt_status", "Type", "ErrCode", "TokenID", "BlockNumber"]

    tx_df = pd.concat(list(etherscan_dfs.values()), ignore_index=True)
    tx_df["TokenName"] = tx_df["TokenName"].apply(lambda x: name_replacements[x] if x in name_replacements.keys() else x)
    tx_df = tx_df[tx_df["TokenSymbol"] != "CNV"]
    tx_df["Amount"] = tx_df["Amount"].apply(lambda x: float(x.replace(",","")) if type(x)==str else x )
    tx_df.drop(columns=tx_df.columns.intersection(unused_columns), inplace=True)
    tx_df.sort_values("TimeStamp", inplace=True)
    tx_df.reset_index(inplace=True, drop=True)

    if platform == "arbitrum": # bridge deposits are marked as "to" instead of "from" (?)
        indices = tx_df[tx_df["To"] == "0x000000000000000000000000000000000000006e"].index
        for i in indices:
            tx_df.at[i, "To"] = tx_df.loc[i]["From"]
            tx_df.at[i, "From"] = "0x000000000000000000000000000000000000006e"

    tx_df["From"] = tx_df["From"].apply(lambda x: address_dict.get(x, x))
    tx_df["To"] = tx_df["To"].apply(lambda x: address_dict.get(x, x))   
    return tx_df


def fetch_historical_prices(cg_ids, date_start, date_end):

    #fetch historical prices for each token
    cg_outputs = {}
    for cg_id in cg_ids:
        cg_outputs[cg_id] = cached_prices(cg_id, date_start, date_end)

    # combine prices into 1 dataframe
    prices_df = None
    for token_id in cg_ids:
        token_df = pd.DataFrame()
        token_df["timestamp"] = np.array(cg_outputs[token_id]["prices"])[:-1,0]
        token_df[token_id] = np.array(cg_outputs[token_id]["prices"])[:-1,1]
        if prices_df is None:
            prices_df = token_df
        else:
            prices_df = prices_df.merge(token_df, on="timestamp", how="outer")

    prices_df["date"] = prices_df["timestamp"].apply(lambda x: pd.to_datetime(x, unit="ms"))
    prices_df["DateString"] = prices_df["date"].apply(lambda x: x.strftime("%Y-%m-%d"))
    return prices_df

def cached_prices(cg_id, date_start, date_end):

    today_string = get_today_string(date_end)

    folder_path = f"./data/token_prices"
    if not os.path.isdir(folder_path):
        os.mkdir(folder_path)

    folder_path = f"./data/token_prices/{today_string}"
    if not os.path.isdir(folder_path):
        os.mkdir(folder_path)

    filepath = f"{folder_path}/{cg_id}.pickle"
    
    if os.path.exists(filepath):
        return pickle.load(open(filepath, 'rb'))

    else:
        print(f"querying historical prices for {cg_id}")
        cg = CoinGeckoAPI()
        ytd_days = (datetime.now() - date_start).days + 1
        response = cg.get_coin_market_chart_by_id(cg_id, "eur", ytd_days, interval="daily")
        
        # todo remove results after date_end ?
        pickle.dump(response, open(filepath, 'wb'))
        time.sleep(1)
        return response

def match_tokens_to_coingecko(tokens):
    tokens = tokens.copy()
    
    filepath = f"./data/cg_tokens.pickle"
    if os.path.exists(filepath):
        cg_coin_list = pickle.load(open(filepath, 'rb'))
    else:
        cg = CoinGeckoAPI()
        cg_coin_list = cg.get_coins_list()
    cg_coin_df = pd.DataFrame.from_dict(cg_coin_list)

    not_found = []
    
    tokens["cg_id"] = None
    for i, row in tokens.iterrows():
        name = row["TokenName"]
        symbol = row["TokenSymbol"]

        if symbol == "SLP":
            token_id = None
        else:
            matches = cg_coin_df[cg_coin_df["symbol"].apply(lambda x: x.lower()) == symbol.lower()]
            matches = matches[["Wormhole" not in name for name in matches["name"]]]

            if len(matches) == 0:            
                token_id = None
            elif len(matches) == 1:
                token_id = matches["id"].values[0]
            else:
                name_matches = matches[[cg_name.lower() in name.lower() for cg_name in matches["name"]]]
                if len(name_matches) == 1:
                    token_id = name_matches["id"].values[0]
                else:
                    print("MULTIPLE POSSIBLE MATCHES")
                    print(name, symbol)
                    print(matches)
                    token_id = None
            if token_id == None:
                not_found.append((name, symbol))
        
        row["cg_id"] = token_id

    print("Tokens not found on CoinGecko: " + "; ".join([symbol + ":" + name for name, symbol in not_found]))
    tokens.sort_values("TokenSymbol", inplace=True)
    return tokens