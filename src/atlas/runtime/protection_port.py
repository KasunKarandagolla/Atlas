"""Narrow Bybit protection port. No ordinary order transport belongs here."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from atlas.domain.execution import EconomicEvent, ProtectionObservation


@dataclass(frozen=True)
class ProtectionPortConfig:
    environment:str='testnet'; venue:str='BYBIT'; product:str='linear'; position_mode:str='one_way'; account_mode:str='isolated'


class BybitProtectionPort(Protocol):
    def read_protection(self,account:str,instrument:str,position_idx:int=0)->ProtectionObservation:...
    def ensure_full_stop(self,position_epoch:int,expected_signed_qty,stop_price,trigger_basis:str):...
    def read_economic_events(self,cursor:str|None,overlap_start:int)->tuple[EconomicEvent,...]:...
class BybitProtectionPortUnavailable:
    def _x(self):raise RuntimeError('TEST GATE: authenticated Bybit protection port unavailable')
    def read_protection(self,*a,**k):return self._x()
    def ensure_full_stop(self,*a,**k):return self._x()
    def read_economic_events(self,*a,**k):return self._x()

def create_protection_port(config:ProtectionPortConfig)->BybitProtectionPort:
    if config.environment not in ('development','testnet') or config.venue!='BYBIT' or config.product!='linear':
        raise ValueError('unsupported protection port profile')
    return BybitProtectionPortUnavailable()
