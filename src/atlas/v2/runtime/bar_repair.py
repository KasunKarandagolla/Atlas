"""Bounded confirmed-bar gap repair; does not claim exact trade completeness."""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from .._serialization import json_value, sha256_json
from ..chronology import sample
from ..data.bars import BarIntervalV2
from ..data.history import reconstruct_native_bars_from_index_page
from ..instruments import InstrumentKeyV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository


def repair_bar_interval(repository: OpsRepository, archive_root: Path, *, key: InstrumentKeyV2,
                        interval: BarIntervalV2, source_id: str, cutoff_ns: int,
                        clock_ns: Callable[[],int]) -> tuple[bool,str | None]:
    """Verify at most 128 rows after the last healthy boundary, with durable progress."""
    healthy = repository.latest_healthy_source_before(source_id,before_ns=cutoff_ns)
    if healthy is None or healthy.available_at_ns>cutoff_ns:
        return False,None
    start = healthy.observed_at_ns
    head = repository.public_bar_repair_head(key,interval.value)
    cursor = start//interval.duration_ns*interval.duration_ns
    previous_ref = None
    if head is not None and head["recovery_started_at_ns"]==start:
        if head["available_at_ns"]>cutoff_ns:
            return False,head["certificate_ref"]
        previous_ref = head["certificate_ref"]
        previous = repository.get_artifact(previous_ref)
        body = json_value(previous.metadata["repair"]) if previous is not None else {}
        if (previous is None or previous.artifact_type!="PublicBarGapRepairPageV1"
                or previous.content_hash!=sha256_json(body)
                or previous.artifact_ref!=previous.content_hash
                or body.get("key")!=key.to_dict()
                or body.get("interval")!=interval.value
                or body.get("source_id")!=source_id
                or body.get("recovery_started_at_ns")!=start
                or body.get("verified_close_at_ns")!=head["verified_close_at_ns"]
                or previous.available_at_ns!=head["available_at_ns"]):
            raise ValueError("bar-repair checkpoint certificate identity is invalid")
        cursor = head["verified_close_at_ns"]
    target = cutoff_ns//interval.duration_ns*interval.duration_ns
    if cursor>=target:
        return True,previous_ref
    started = sample(clock_ns,floor_ns=cutoff_ns)
    reason = "BAR_GAP_REPAIR_PENDING"
    refs: tuple[str,...] = ()
    last = cursor
    try:
        entries,_source_cursor,more = repository.active_history_source_page(
            key,interval.value,after_close_at_ns=cursor,cutoff_ns=cutoff_ns)
        indexed = reconstruct_native_bars_from_index_page(repository,archive_root,key=key,
            interval=interval,index_entries=entries,max_origins=128)
        selected = {item.bar.close_at_ns:item for item in indexed}
        for close in sorted(selected):
            item = selected[close]
            if close!=last+interval.duration_ns or item.bar.raw.source_id!=source_id:
                reason = "CONFIRMED_BAR_GAP_UNREPAIRED"
                break
            last = close
        else:
            reason = ("CONFIRMED_BAR_GAP_REPAIRED" if last>=target else
                      "BAR_GAP_REPAIR_BACKLOG" if more else "CONFIRMED_BAR_GAP_UNREPAIRED")
        refs = tuple(item.observation_index_ref for close,item in sorted(selected.items()) if close<=last)
    except (ValueError,OSError):
        reason = "BAR_GAP_REPAIR_SOURCE_UNSUPPORTED"
    available = sample(clock_ns,floor_ns=started)
    body = {"version":"PublicBarGapRepairPageV1","key":key.to_dict(),"interval":interval.value,
        "source_id":source_id,"recovery_started_at_ns":start,"information_cutoff_ns":cutoff_ns,
        "computation_started_ns":started,"available_at_ns":available,"previous_certificate_ref":previous_ref,
        "previous_verified_close_at_ns":cursor,"verified_close_at_ns":last,"target_close_at_ns":target,
        "input_refs":refs,"reason_code":reason,"max_source_rows_per_cycle":128,"authority":"ZERO",
        "exact_trade_completeness":"NOT ESTIMABLE"}
    ref = sha256_json(body)
    with repository.atomic_composition():
        repository.register_artifact(ArtifactIndexEntryV2(ref,"PublicBarGapRepairPageV1",ref,
            available,available,{"repair":body}))
        if last>cursor:
            repository.save_public_bar_repair_head(key,interval.value,started_ns=start,close_ns=last,
                certificate_ref=ref,available_at_ns=available)
    return last>=target,ref
