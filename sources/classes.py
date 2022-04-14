from enum import IntEnum, auto
import numpy as np
from sources.utils import get_platform, is_priced_token_in, is_priced_token_out, is_unpriced_token_in, is_unpriced_token_out
from sources.utils import get_method, get_contract_id, get_token_id, dict_union_sum

EPS = 1e-10

class Buy:
    def __init__(self, token_id: str, count:float, cost_basis:float):
        assert cost_basis >= 0
        assert count > 0
        self.token_id = token_id
        self.count = count
        self.cost_basis = cost_basis
        
    def __repr__(self):
        return self.__str__()
    
    def __str__(self):
        return f"token: {self.token_id} amount: {self.count} cost_basis: {self.cost_basis}"

class Token:
    def __init__(self, token_id: str):
        if token_id is None:
            raise Exception("token_id is None")
        self.token_id = token_id
        
    def amount(self):
        # return amount
        raise Exception("not implemented")       
    
    def add_token(self, other_token):
        # combine with other token of same type
        raise Exception("not implemented")
        
    def remove(self, amount):
        # return same class with amount in it
        raise Exception("not implemented")

    def cost(self):
        # combine with other token of same type
        raise Exception("not implemented")
        
    def cost_basis(self):
        # return total cost basis for self
        return self.cost()/self.amount()

    def is_empty(self):
        return self.amount() < EPS
    
    def __repr__(self):
        return self.__str__()
    
    def __str__(self):
        return f"{self.token_id}: {self.amount()}"
        
    
class BaseToken(Token):
    
    def __init__(self, token_id: str, buy:Buy=None):
        super().__init__(token_id)
        self.buys = []
        if buy:
            self.buys.append(buy)
        
    def add_buy(self, count:float, cost_basis:float):
        self.buys.append(Buy(self.token_id, count, cost_basis))
        
    def add_token(self, other_token):
        assert type(other_token) == BaseToken
        assert self.token_id == other_token.token_id
        self.buys = self.buys + other_token.buys        
        
    def amount(self):
        return sum([buy.count for buy in self.buys])
    
    def cost(self):
        return sum([buy.count * buy.cost_basis for buy in self.buys])
    
    def remove(self, amount):
        amount_to_remove = amount
        removed_token = BaseToken(self.token_id)
        
        while amount_to_remove > EPS:
            # LIFO method
            if len(self.buys) == 0:
                print(f"empty buys, {self}, left to remove: {amount_to_remove}")
            buy = self.buys.pop()

            to_remove_from_buy = min(amount_to_remove, buy.count)
            amount_to_remove = amount_to_remove - to_remove_from_buy
            new_amount = buy.count - to_remove_from_buy
            
            removed_token.add_buy(to_remove_from_buy, buy.cost_basis)
            
            if new_amount > EPS:
                self.add_buy(new_amount,  buy.cost_basis)
                
        assert(np.abs(removed_token.amount() - amount) < EPS)
        return removed_token
    
    def remove_ratio(self, remove_ratio):
        assert(remove_ratio <= 1)
        assert(remove_ratio >= 0)
        return self.remove(self.amount() * remove_ratio)
        
        
class LiquidDepositToken(Token):
    def __init__(self, token_id: str, deposit_tokens: dict[str, Token], amount):
        super().__init__(token_id)
        assert type(deposit_tokens) == dict
        self.deposits = deposit_tokens
        self.count = amount
    
    def amount(self):
        return self.count
    
    def underlying_token_amount(self, token_id):
        if token_id in self.deposits.keys():
            return self.deposits[token_id].amount()
        else:
            return 0
        
    def cost(self):
        return sum([token.cost() for token in self.deposits.values()])
    
    def add_token(self, other_token):
        assert type(other_token) == LiquidDepositToken
        assert self.token_id == other_token.token_id
        
        for key in other_token.deposits.keys():
            if key in self.deposits.keys():
                self.deposits[key].add_token(other_token.deposits[key])
            else:
                self.deposits[key] = other_token.deposits[key]
                
        self.count = self.count + other_token.count 
        
    def remove_ratio(self, ratio):
        removed_amount = self.count * ratio
        self.count = self.count - removed_amount
        
        return LiquidDepositToken(self.token_id, 
                                  {deposit.token_id: deposit.remove_ratio(ratio) for deposit in self.deposits.values()}, 
                                  removed_amount)

    def leaf_base_token_amounts(self):
        base_tokens = {}
        for token in self.deposits.values():
            if type(token) == BaseToken:
                base_tokens[token.token_id] = base_tokens.get(token.token_id, 0) + token.amount()
            else:
                base_tokens = dict_union_sum(base_tokens, token.leaf_base_token_amounts())
        return base_tokens
            
    def remove(self, amount):
        ratio = amount / self.count
        return self.remove_ratio(ratio)
    
    def withdraw(self, token_id, amount):
        return self.deposits[token_id].remove(amount)

    def __str__(self):
        return f"{self.token_id}: {self.amount()} (deposits: " + " ".join([x.__str__() for x in self.deposits.values()]) + ")"
    
    
class DepositContract:
    def __init__(self, contract_id):
        self.contract_id = contract_id
        self.deposits = {}
        
    def deposit(self, token):
        if token.token_id not in self.deposits.keys():
            self.deposits[token.token_id] = token
        else:
            self.deposits[token.token_id].add_token(token)
            
    def token_deposit_amount(self, token_id):
        
        if token_id in self.deposits.keys():
            return self.deposits[token_id].amount()
        else:
            return 0
            
    def withdraw(self, token_id, amount):
        removed =  self.deposits[token_id].remove(amount)
        if self.deposits[token_id].amount() < EPS:
            self.deposits.pop(token_id, None)
        return removed
            
    def cost(self):
        return sum([token.cost() for token in self.deposits.values()])

    def is_empty(self):
        if len(self.deposits) == 0:
            return True
        else:
            #assert sum([x.amount() for x in self.deposits.values()]) > EPS
            return False

    def leaf_base_token_amounts(self):
        base_tokens = {}
        for token in self.deposits.values():
            if type(token) == BaseToken:
                base_tokens[token.token_id] = base_tokens.get(token.token_id, 0) + token.amount()
            else:
                base_tokens = dict_union_sum(base_tokens, token.leaf_base_token_amounts())
        return base_tokens
        
    def __repr__(self):
        return self.__str__()
    
    def __str__(self):
        string = "contract id: " + self.contract_id + "\n"
        for key in self.deposits.keys():
            string += f"{self.deposits[key]} \n"
        return string
    
class Portfolio:
    def __init__(self):
        self.spot = {}
        self.deposits = {}
        
    def add_buy(self, token_id: str, amount: float, cost_basis: float):
        if token_id not in self.spot.keys():
            self.spot[token_id] = BaseToken(token_id)
            
        self.spot[token_id].add_buy(amount, cost_basis)
        
    def add_token(self, token: Token):
        
        if token.token_id not in self.spot.keys():
            self.spot[token.token_id] = token
        else:
            self.spot[token.token_id].add_token(token)
            
    def deposit(self, contract_id: str, token_id: str, amount: float):
        
        if contract_id not in self.deposits.keys():
            self.deposits[contract_id] = DepositContract(contract_id)
        
        token_to_deposit = self.remove_token(token_id, amount)
        self.deposits[contract_id].deposit(token_to_deposit)
        
    def liquid_deposit(self, liquid_token_id: str, liquid_token_amount: float, deposits: dict[str, Token]):
        liquid_token = LiquidDepositToken(liquid_token_id, deposits, liquid_token_amount)
        
        if liquid_token_id in self.spot.keys():
            self.spot[liquid_token_id].add_token(liquid_token)
        else:
            self.spot[liquid_token_id] = liquid_token
            
    def remove_from_contract(self, contract_id: str, token_id, amount: float):
        removed = self.deposits[contract_id].withdraw(token_id, amount)
        if self.deposits[contract_id].is_empty():
            self.deposits.pop(contract_id, None)
        return removed
    
    def remove_token(self, token_id: str, amount: float):
        removed =  self.spot[token_id].remove(amount)
        if self.spot[token_id].is_empty():
            self.spot.pop(token_id, None)
        return removed
            
    def cost(self):
        return sum([token.cost() for token in self.spot.values()]) + sum([deposit.cost() for deposit in self.deposits.values()])
        
    def __repr__(self):
        return self.__str__()
    
    def __str__(self):
        string = "Spot - priced tokens \n"
        for key in self.spot.keys():
            if type(self.spot[key]) == BaseToken:
                string += f"{self.spot[key]} \n"
        string += "\n"

        string += "Spot - deposit tokens \n"
        for key in self.spot.keys():
            if type(self.spot[key]) == LiquidDepositToken:
                string += f"{self.spot[key]} \n"
        string += "\n"

        string += "In contracts: \n"
        for key in self.deposits.keys():
            string += f"{self.deposits[key]} \n"
        return string
    
class TxType(IntEnum):
    FEE_ONLY = auto()
    
    TRANSFER_IN = auto()
    TRANSFER_OUT = auto()

    CONTRACT_DEPOSIT = auto()
    CONTRACT_WITHDRAW = auto()
    
    SWAP = auto()
    
    LIQUID_DEPOSIT = auto()
    LIQUID_WITHDRAW = auto()
    
    ERROR = auto()
    
class RowType(IntEnum):
    NO_TRANSFER = auto()
    
    INITIAL_DEPOSIT = auto()
    
    TRANSFER_PAYMENT_IN = auto()
    TRANSFER_PAYMENT_OUT = auto()
    
    TRANSFER_INTERNAL_IN = auto()
    TRANSFER_INTERNAL_OUT = auto()
    
    TRANSFER_NFT_IN = auto()
    TRANSFER_NFT_OUT = auto()

    CONTRACT_DEPOSIT_OUT = auto()
    CONTRACT_WITHDRAW_IN = auto()
    
    SWAP_IN = auto()
    SWAP_OUT = auto()
    
    LIQUID_DEPOSIT_IN = auto()
    LIQUID_DEPOSIT_OUT = auto()
    
    LIQUID_WITHDRAW_IN = auto()
    LIQUID_WITHDRAW_OUT = auto()
    
    ERROR = auto()

class TxData:
    
    def __init__(self, tx_rows, portfolio, verbose = False):
        self.tx_rows = tx_rows
        self.method = get_method(tx_rows)
        self.contract_id = get_contract_id(tx_rows)
        self.platform = get_platform(tx_rows)
        
        self.num_priced_tokens_in    = sum(tx_rows.apply(is_priced_token_in, axis = 1))
        self.num_priced_tokens_out   = sum(tx_rows.apply(is_priced_token_out, axis = 1))
        self.num_unpriced_tokens_in  = sum(tx_rows.apply(is_unpriced_token_in, axis = 1))
        self.num_unpriced_tokens_out = sum(tx_rows.apply(is_unpriced_token_out, axis = 1))
        
        self.num_in = self.num_priced_tokens_in + self.num_unpriced_tokens_in
        self.num_out = self.num_priced_tokens_out + self.num_unpriced_tokens_out
        
        self.tx_type = None
        
        txids = tx_rows["Hash"].unique()
        assert len(txids) == 1
        self.tx_id = txids[0]
        
        if (verbose):
            print(f"tx_id: {self.tx_id}")
            print(f"method: {self.method}")
            print(f"contract_id: {self.contract_id}")
            print(f"num_in: {self.num_in}")
            print(f"num_out: {self.num_out}")
            print(f"num_priced_tokens_in: {self.num_priced_tokens_in}")
            print(f"num_priced_tokens_out: {self.num_priced_tokens_out}")
            print(f"num_unpriced_tokens_in: {self.num_unpriced_tokens_in}")
            print(f"num_unpriced_tokens_out: {self.num_unpriced_tokens_out}")
        
        else: 
            if self.num_in == 0 and self.num_out == 0:
                
                self.tx_type = TxType.FEE_ONLY
                
            elif self.method is None and self.num_in > 0 and self.num_out == 0:
                
                self.tx_type = TxType.TRANSFER_IN
                    
            elif self.num_in == 0 and self.num_out > 0:
                if self.method.lower() == "transfer":
                    self.tx_type = TxType.TRANSFER_OUT
                else:
                    self.tx_type = TxType.CONTRACT_DEPOSIT
                    
            elif self.num_in > 0 and self.num_out == 0:
                
                if self.method.lower() == "transfer":
                    self.tx_type = TxType.TRANSFER_IN
                else:
                    self.tx_type = TxType.CONTRACT_WITHDRAW
                
            elif self.num_priced_tokens_in == 1 and self.num_priced_tokens_out > 0:
                
                if self.num_priced_tokens_out > 1:
                    raise Exception("swap for multiple priced tokens")
                
                self.tx_type = TxType.SWAP
                
            
            elif self.num_out == 1 and self.num_unpriced_tokens_out == 1 and (self.num_in > 1 or self.num_priced_tokens_in > 0):
                
                self.tx_type = TxType.LIQUID_WITHDRAW
                
            elif self.num_out == 1 and self.num_unpriced_tokens_out == 1 and self.num_in == 1 and self.num_unpriced_tokens_in == 1:
                
                in_token_id = get_token_id(tx_rows[tx_rows.apply(is_unpriced_token_in, axis = 1)].iloc[0])
                out_token_id = get_token_id(tx_rows[tx_rows.apply(is_unpriced_token_out, axis = 1)].iloc[0])
                out_deposits = portfolio.spot[out_token_id].deposits
                
                if in_token_id in out_deposits.keys() and out_deposits[in_token_id].amount() > 0:
                    self.tx_type = TxType.LIQUID_WITHDRAW 
                else:
                    self.tx_type = TxType.LIQUID_DEPOSIT 
                    
            elif self.num_in == 1 and self.num_unpriced_tokens_in == 1 and self.num_out > 0:
                
                self.tx_type = TxType.LIQUID_DEPOSIT
                
            else:
                self.tx_type = TxType.ERROR
            
        if (verbose):
            print(f"tx_type: {self.tx_type}")



