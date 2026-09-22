"""Causal information contract shared by Phase 3 records."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .enums import AvailabilityClass
from .time import ensure_utc_ns


@dataclass(frozen=True)
class InformationContract:
    contract_version:str; source_id:str; data_type:str; units:str=''; source_event_at_ns:int|None=None; source_published_at_ns:int|None=None; received_at_ns:int|None=None; available_at_ns:int|None=None; processed_at_ns:int|None=None; source_revision:str|None=None; availability_class:AvailabilityClass=AvailabilityClass.UNKNOWN; availability_lower_ns:int|None=None; availability_upper_ns:int|None=None; replay_available_at_ns:int|None=None; availability_method:str|None=None; evidence_ref:str|None=None; content_hash:str|None=None; dependency_ids:tuple[str,...]=(); pipeline_version:str='1.0'; quality:str=''; missingness:str=''
    def __post_init__(self):
        for n in ('contract_version','source_id','data_type','pipeline_version'):
            if not isinstance(getattr(self,n),str) or not getattr(self,n).strip(): raise ValueError(f'{n} must be non-blank')
        if not isinstance(self.availability_class,AvailabilityClass): raise ValueError('availability_class invalid')
        for n in ('source_event_at_ns','source_published_at_ns','received_at_ns','available_at_ns','processed_at_ns','availability_lower_ns','availability_upper_ns','replay_available_at_ns'):
            v=getattr(self,n)
            if v is not None: ensure_utc_ns(v,field=n)
        object.__setattr__(self,'dependency_ids',tuple(self.dependency_ids)); self._validate()
    def _validate(self):
        s=self
        if s.source_event_at_ns is not None and s.source_published_at_ns is not None and s.source_published_at_ns<s.source_event_at_ns: raise ValueError('source_published_at cannot precede source_event_at')
        if s.source_published_at_ns is not None and s.received_at_ns is not None and s.received_at_ns<s.source_published_at_ns: raise ValueError('received_at cannot precede source_published_at')
        if s.received_at_ns is not None and s.available_at_ns is not None and s.available_at_ns<s.received_at_ns: raise ValueError('available_at cannot precede received_at')
        if s.processed_at_ns is not None and s.available_at_ns is not None and s.available_at_ns<s.processed_at_ns: raise ValueError('available_at cannot precede processed_at')
        if s.availability_lower_ns is not None and s.availability_upper_ns is not None and s.availability_upper_ns<s.availability_lower_ns: raise ValueError('availability interval invalid')
        if s.availability_class==AvailabilityClass.ACTUAL_OBSERVED:
            if s.received_at_ns is None or s.available_at_ns is None: raise ValueError('ACTUAL_OBSERVED requires received_at and available_at')
            if s.replay_available_at_ns is not None: raise ValueError('ACTUAL_OBSERVED must not carry replay_available_at')
        elif s.availability_class==AvailabilityClass.RECONSTRUCTED_PUBLIC:
            if s.received_at_ns is None or s.replay_available_at_ns is None or not (s.availability_method and s.availability_method.strip()): raise ValueError('RECONSTRUCTED_PUBLIC requires actual receipt, replay time and method')
            if s.received_at_ns<=s.replay_available_at_ns: raise ValueError('must not fabricate historical received_at')
        elif s.availability_class==AvailabilityClass.REVISED_NO_VINTAGE and s.replay_available_at_ns is not None: raise ValueError('REVISED_NO_VINTAGE must not carry replay time')
    def validate_dependencies(self,availability_by_id:Mapping[str,int])->None:
        if not self.dependency_ids:return
        missing=[d for d in self.dependency_ids if d not in availability_by_id]
        if missing: raise ValueError(f'missing dependency availabilities: {missing}')
        latest=max(availability_by_id[d] for d in self.dependency_ids); anchor=self.available_at_ns if self.available_at_ns is not None else self.processed_at_ns
        if anchor is None or anchor<latest: raise ValueError('cannot be available before required dependencies')
    def to_dict(self)->dict[str,Any]: return {f:(getattr(self,f).value if f=='availability_class' else list(getattr(self,f)) if f=='dependency_ids' else getattr(self,f)) for f in self.__dataclass_fields__}
    def to_canonical_json(self)->str: return json.dumps(self.to_dict(),sort_keys=True,separators=(',',':'))
    def compute_content_hash(self)->str:
        d=self.to_dict(); d['content_hash']=None; return hashlib.sha256(json.dumps(d,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def actual_observed(*,source_id:str,data_type:str,units:str='',source_event_at_ns:int|None=None,source_published_at_ns:int|None=None,received_at_ns:int,available_at_ns:int,processed_at_ns:int|None=None,pipeline_version:str='1.0',evidence_ref:str|None=None,dependency_ids:tuple[str,...]=(),quality:str='',missingness:str='')->InformationContract:
    """Compatibility constructor for the pre-Phase-3 observed-record API."""
    return InformationContract('1.0',source_id,data_type,units,source_event_at_ns,source_published_at_ns,received_at_ns,available_at_ns,processed_at_ns,None,AvailabilityClass.ACTUAL_OBSERVED,None,None,None,None,evidence_ref,None,dependency_ids,pipeline_version,quality,missingness)


def reconstructed_public(*,source_id:str,data_type:str,units:str='',source_event_at_ns:int|None=None,received_at_ns:int,replay_available_at_ns:int,availability_method:str,availability_lower_ns:int|None=None,availability_upper_ns:int|None=None,pipeline_version:str='1.0',evidence_ref:str|None=None,dependency_ids:tuple[str,...]=(),quality:str='',missingness:str='')->InformationContract:
    """Compatibility constructor preserving actual receipt separately from replay time."""
    return InformationContract('1.0',source_id,data_type,units,source_event_at_ns,None,received_at_ns,None,None,None,AvailabilityClass.RECONSTRUCTED_PUBLIC,availability_lower_ns,availability_upper_ns,replay_available_at_ns,availability_method,evidence_ref,None,dependency_ids,pipeline_version,quality,missingness)
