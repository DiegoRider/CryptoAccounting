import numpy as np
import pandas as pd
from sources.classes import RowType, TxType, Portfolio, TxData, BaseToken, LiquidDepositToken
from sources.utils import is_in, is_out, is_nft, is_nft_in, is_nft_out, get_token_id, dict_union_sum

INITIAL_DEPOSIT_WALLET = "to_set (0x...)"

def classify_rows(tx_rows, txdata, portfolio):
    return {i: classify_row(row, txdata, portfolio) for i, row in tx_rows.iterrows()}

def classify_row(row, txdata, portfolio):

    error_type = None
    
    if row.Amount == 0.00:
        
        return RowType.NO_TRANSFER
    
    elif is_nft(row):
        
        if is_nft_in(row):
            return RowType.TRANSFER_NFT_IN
        elif is_nft_out(row):
            return RowType.TRANSFER_NFT_OUT
        else:
            error_type = "nft error"
    
    elif txdata.tx_type == TxType.TRANSFER_IN:
        
        assert is_in(row)
        if INITIAL_DEPOSIT_WALLET == row.From.lower():
            return RowType.INITIAL_DEPOSIT
        elif any([x in row.From.lower() for x in ["bridge"]]):
            return RowType.TRANSFER_INTERNAL_IN
        else:
            return RowType.TRANSFER_PAYMENT_IN
        
    elif txdata.tx_type == TxType.TRANSFER_OUT:
        
        assert is_out(row)
        if any([x in row.To.lower() for x in ["bridge"]]):
            return RowType.TRANSFER_INTERNAL_OUT
        else:
            return RowType.TRANSFER_PAYMENT_OUT
        
    elif txdata.tx_type == TxType.CONTRACT_DEPOSIT:
        
        assert is_out(row)
        if "bridge" in row.To.lower():
            return RowType.TRANSFER_INTERNAL_OUT
        else:
            return RowType.CONTRACT_DEPOSIT_OUT
        
    elif txdata.tx_type == TxType.CONTRACT_WITHDRAW:
        
        assert is_in(row)
        if txdata.method and "reward" in txdata.method.lower():
            return RowType.TRANSFER_PAYMENT_IN
        else:
            if txdata.contract_id is None:
                print("contract id is None for " + row.Hash)

            if row.From not in portfolio.deposits.keys():
                return RowType.TRANSFER_PAYMENT_IN
            
            if portfolio.deposits[row.From].token_deposit_amount(get_token_id(row)) > 0: # if token received is token deposited
                return RowType.CONTRACT_WITHDRAW_IN
            else:
                return RowType.TRANSFER_PAYMENT_IN
        
    elif txdata.tx_type == TxType.SWAP:
        assert row["ValueEuro"] > 0
        if is_in(row):
            return RowType.SWAP_IN
        elif is_out(row):
            return RowType.SWAP_OUT
        else:
            error_type = "swap error"
    elif txdata.tx_type == TxType.LIQUID_DEPOSIT:
        if is_in(row):
            return RowType.LIQUID_DEPOSIT_IN
        elif is_out(row):
            return RowType.LIQUID_DEPOSIT_OUT
        else:
            error_type = "liquid deposit error"
        
    elif txdata.tx_type == TxType.LIQUID_WITHDRAW:
        if is_in(row):
            return RowType.LIQUID_WITHDRAW_IN
        elif is_out(row):
            return RowType.LIQUID_WITHDRAW_OUT
        else:
            error_type = "liquid withdraw error"
        
    elif txdata.tx_type == TxType.ERROR:
        error_type = "transaction type error"
    
    print("row error", error_type, row.Hash)
    return RowType.ERROR

def compute_portfolio_and_gains(tx_df):
    portfolio = Portfolio()
    tx_df = tx_df.copy()

    txs = tx_df["Hash"].unique()
    tx_df["RowCategory"] = None
    tx_df["TxCategory"] = None
    tx_df["Cost"] = 0
    tx_df["Gain/Loss"] = 0
    tx_df["TxnFee(Cost)"] = 0
    tx_df["TxnFee(Gain/Loss)"] = 0

    for txhash in txs:
            
        tx_rows = tx_df[tx_df["Hash"] == txhash]

        tx_data = TxData(tx_rows, portfolio)
        
        row_types = classify_rows(tx_rows, tx_data, portfolio)
        
        for i, category in row_types.items():
            tx_df.at[i, "RowCategory"] = category.name
            tx_df.at[i, "TxCategory"] = tx_data.tx_type.name
            
        category2rows = {}
        for k, v in row_types.items():
            category2rows[v] = category2rows.get(v, []) + [tx_rows.loc[k]]
            
        for i, row in tx_rows.iterrows():
            fee = None
            if is_out(row) and row["TxnFee(ETH)"] > 0:
                fee = portfolio.remove_token("ETH", row["TxnFee(ETH)"])
            
            if fee:
                tx_df.at[i, "TxnFee(Cost)"] = fee.cost()
                tx_df.at[i, "TxnFee(Gain/Loss)"] = row["TxnFee(Euro)"] - fee.cost()

        for row in category2rows.get(RowType.INITIAL_DEPOSIT, []):
            portfolio.add_buy(get_token_id(row), row.Amount, row.TokenPriceEuro)                
            
        for row in category2rows.get(RowType.TRANSFER_PAYMENT_IN, []):
            if np.isnan(row.TokenPriceEuro):
                print(f"received unpriced token {row.TokenSymbol}: {row.Amount}")
                portfolio.add_buy(get_token_id(row), row.Amount, 0)
            else:
                portfolio.add_buy(get_token_id(row), row.Amount, row.TokenPriceEuro)
            tx_df.at[row.name, "Gain/Loss"] = row.ValueEuro
            
        for row in category2rows.get(RowType.TRANSFER_PAYMENT_OUT, []):
            token = portfolio.remove_token(get_token_id(row), row.Amount)
            tx_df.at[row.name, "Cost"] = token.cost()
            tx_df.at[row.name, "Gain/Loss"] = row.ValueEuro - token.cost()
            
        for row in category2rows.get(RowType.CONTRACT_DEPOSIT_OUT, []):
            portfolio.deposit(tx_data.contract_id, get_token_id(row), row.Amount)
                
        for row in category2rows.get(RowType.CONTRACT_WITHDRAW_IN, []):
            
            contract_id = row.From
            token_amount_deposited = portfolio.deposits[contract_id].token_deposit_amount(get_token_id(row))
            to_withdraw = min(token_amount_deposited, row.Amount)
            extra_amount = max(row.Amount - token_amount_deposited, 0)
            
            removed = portfolio.remove_from_contract(contract_id, get_token_id(row), to_withdraw)
            portfolio.add_token(removed)
            
            if extra_amount > 0:
                print(f"withdraw more than deposited {tx_data.tx_id} {get_token_id(row)}")
                portfolio.add_buy(get_token_id(row), extra_amount, row.TokenPriceEuro)
                tx_df.at[row.name, "Gain/Loss"] = extra_amount * row.TokenPriceEuro
                
        if tx_data.tx_type == TxType.SWAP:
            
            in_rows = category2rows.get(RowType.SWAP_IN, [])
            out_rows = category2rows.get(RowType.SWAP_OUT, [])
            assert(len(in_rows) > 0)
            assert(len(out_rows) > 0)
            
            for row in out_rows:
                removed = portfolio.remove_token(get_token_id(row), row.Amount)
                tx_df.at[row.name, "Cost"] = removed.cost()
                tx_df.at[row.name, "Gain/Loss"] = - removed.cost()
            
            for row in in_rows:
                assert(row.TokenPriceEuro > 0)
                portfolio.add_buy(get_token_id(row), row.Amount, row.TokenPriceEuro)
                tx_df.at[row.name, "Gain/Loss"] = row.ValueEuro
                
        elif tx_data.tx_type == TxType.LIQUID_DEPOSIT:
            
            in_rows = category2rows.get(RowType.LIQUID_DEPOSIT_IN, [])
            out_rows = category2rows.get(RowType.LIQUID_DEPOSIT_OUT, [])
            
            assert(len(in_rows) == 1)
            assert(len(out_rows) > 0)
            
            in_row = in_rows[0]
            
            deposits = {}
            for row in out_rows:
                removed = portfolio.remove_token(get_token_id(row), row.Amount)
                assert removed.token_id not in deposits.keys()
                deposits[removed.token_id] = removed
            
            portfolio.liquid_deposit(get_token_id(in_row), in_row.Amount, deposits)
            
        elif tx_data.tx_type == TxType.LIQUID_WITHDRAW:
            
            in_rows = category2rows.get(RowType.LIQUID_WITHDRAW_IN, [])
            out_rows = category2rows.get(RowType.LIQUID_WITHDRAW_OUT, [])
            
            assert(len(in_rows) > 0)
            assert(len(out_rows) == 1)
            
            out_row = out_rows[0]
            removed = portfolio.remove_token(get_token_id(out_row), out_row.Amount)
            
            for row in in_rows:
                token_amount_deposited = removed.underlying_token_amount(get_token_id(row))
                if  token_amount_deposited > 0:
                    
                    to_withdraw = min(token_amount_deposited, row.Amount)
                    extra_amount = max(row.Amount - token_amount_deposited, 0)
                    
                    unwrapped = removed.withdraw(get_token_id(row), to_withdraw)
                    portfolio.add_token(unwrapped)
                    
                    if extra_amount > 0:
                        assert(row.TokenPriceEuro > 0)
                        portfolio.add_buy(get_token_id(row), extra_amount, row.TokenPriceEuro)
                        tx_df.at[row.name, "Gain/Loss"] = extra_amount * row.TokenPriceEuro
                else:
                    if np.isnan(row.TokenPriceEuro):
                        print(f"received unpriced token {row.TokenSymbol}: {row.Amount}")
                        portfolio.add_buy(get_token_id(row), row.Amount, 0)
                    else:
                        assert(row.TokenPriceEuro > 0)
                        portfolio.add_buy(get_token_id(row), row.Amount, row.TokenPriceEuro)
                        tx_df.at[row.name, "Gain/Loss"] = row.ValueEuro
                    
            tx_df.at[out_row.name, "Cost"] = removed.cost()
            tx_df.at[out_row.name, "Gain/Loss"] = - removed.cost()

    return portfolio, tx_df

def approx_holdings(portfolio):
    
    approx_holdings = {}

    for token in portfolio.spot.values():
        approx_holdings = add_base_tokens(token, approx_holdings)

    for deposit_contract in portfolio.deposits.values():
        for token in deposit_contract.deposits.values():
            approx_holdings = add_base_tokens(token, approx_holdings)

    return approx_holdings

def add_base_tokens(token, d):
    if type(token) == BaseToken:
        return dict_union_sum(d, {token.token_id: token.amount()})
    elif type(token) == LiquidDepositToken:
        for deposit in token.deposits.values():
            d = add_base_tokens(deposit, d)
        return d
    else:
        print(token)
        raise Exception

