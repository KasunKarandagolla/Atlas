"""Logical Bybit V1 wire contracts only; no transport."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from atlas.domain.enums import Side
from atlas.domain.money import canonical_decimal_str
from atlas.domain.trade_plan import TradePlan


@dataclass(frozen=True)
class EntryWireContract:
    instrument:str; side:str; quantity:Decimal; price:Decimal; stop_loss:Decimal; order_type:str='LIMIT'; time_in_force:str='IOC'; reduce_only:bool=False; position_idx:int=0; sl_trigger_by:str='MarkPrice'; sl_order_type:str='Market'; tpsl_mode:str='Full'
    def __post_init__(self):
        if self.order_type!='LIMIT' or self.time_in_force!='IOC' or self.reduce_only or self.position_idx!=0 or self.sl_trigger_by!='MarkPrice' or self.sl_order_type!='Market' or self.tpsl_mode!='Full':raise ValueError('entry contract violates frozen V1')
        if self.side not in ('Buy','Sell') or min(self.quantity,self.price,self.stop_loss)<=0:raise ValueError('invalid entry values')
    def to_bybit_params(self)->dict[str,Any]:return {'symbol':self.instrument,'side':self.side,'orderType':'LIMIT','timeInForce':'IOC','qty':canonical_decimal_str(self.quantity),'price':canonical_decimal_str(self.price),'reduceOnly':False,'positionIdx':0,'stopLoss':canonical_decimal_str(self.stop_loss),'slTriggerBy':'MarkPrice','slOrderType':'Market','tpslMode':'Full'}
@dataclass(frozen=True)
class ExitWireContract:
    instrument:str; side:str; quantity:Decimal; order_type:str='LIMIT'; time_in_force:str='IOC'; price:Decimal|None=None; reduce_only:bool=True; position_idx:int=0
    def __post_init__(self):
        if self.side not in ('Buy','Sell') or self.quantity<=0 or not self.reduce_only or self.position_idx!=0:raise ValueError('invalid exit contract')
        if self.order_type=='LIMIT' and (self.price is None or self.price<=0):raise ValueError('LIMIT requires price')
        if self.order_type=='MARKET' and self.price is not None:raise ValueError('MARKET must not contain fake price')
        if self.order_type not in ('LIMIT','MARKET'):raise ValueError('unsupported order type')
    def to_bybit_params(self)->dict[str,Any]:
        d={'symbol':self.instrument,'side':self.side,'orderType':self.order_type,'timeInForce':self.time_in_force,'qty':canonical_decimal_str(self.quantity),'reduceOnly':True,'positionIdx':0}
        if self.price is not None:d['price']=canonical_decimal_str(self.price)
        return d
def build_entry_wire_contract(plan:TradePlan,collar_price:Decimal,stop_price:Decimal,quantity:Decimal)->EntryWireContract:return EntryWireContract(f'{plan.instrument}-LINEAR.BYBIT','Buy' if plan.side==Side.LONG else 'Sell',quantity,collar_price,stop_price)
def build_exit_wire_contract(plan:TradePlan,exit_price:Decimal,quantity:Decimal)->ExitWireContract:return ExitWireContract(f'{plan.instrument}-LINEAR.BYBIT','Sell' if plan.side==Side.LONG else 'Buy',quantity,'LIMIT','IOC',exit_price)
def build_market_exit_wire_contract(plan:TradePlan,quantity:Decimal)->ExitWireContract:return ExitWireContract(f'{plan.instrument}-LINEAR.BYBIT','Sell' if plan.side==Side.LONG else 'Buy',quantity,'MARKET','IOC',None)
