from __future__ import annotations

from support.scanner_fixture import EPOCH_NS, SLOT_NS

from atlas.scanner import BlindSpotObservation, BlindSpotStatus, RankBand, blindspot_metrics


def _observation(slot: int, instrument: str, value: float, *, top: bool, exploration: bool,
                 probability: float = 0.5, warm: bool = True, deadline: bool = True) -> BlindSpotObservation:
    return BlindSpotObservation(slot, instrument, RankBand.TOP_3 if top else RankBand.B1, top, exploration,
                                probability, value, warm, deadline)


def test_blindspots_use_ipw_only_for_known_exploration_probability():
    observations = []
    for index in range(6):
        slot = EPOCH_NS + index * SLOT_NS
        observations.append(_observation(slot, "BTCUSDT", 2.0, top=True, exploration=False, probability=0.0))
        observations.append(_observation(slot, "XRPUSDT", 0.1, top=False, exploration=True, probability=0.5))
    metrics = blindspot_metrics(observations, tolerance=0.20)
    assert metrics.status in {BlindSpotStatus.PASS, BlindSpotStatus.ATTENTION}
    assert metrics.missed_value_share is not None and metrics.missed_value_share_upper is not None
    assert metrics.selection_coverage is not None
    assert metrics.selection_lift is not None
    assert metrics.exploration_probability_support == 6


def test_blindspots_are_inconclusive_without_support():
    observation = _observation(EPOCH_NS, "BTCUSDT", 2.0, top=True, exploration=False, probability=0.0)
    metrics = blindspot_metrics((observation,))
    assert metrics.status is BlindSpotStatus.INCONCLUSIVE
    assert "INSUFFICIENT_SLOT_SUPPORT" in metrics.reasons


def test_warmup_and_deadline_loss_are_present_in_metrics():
    observations = [
        _observation(EPOCH_NS, "BTCUSDT", 1.0, top=True, exploration=False, probability=0.0),
        _observation(EPOCH_NS, "SOLUSDT", -0.1, top=False, exploration=True, probability=1.0,
                     warm=False, deadline=False),
    ]
    metrics = blindspot_metrics(observations)
    assert metrics.warmup_exclusion_rate == 0.5
    assert metrics.deadline_loss_rate == 0.5
