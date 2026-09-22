from __future__ import annotations

from support.scanner_fixture import EPOCH_NS, SLOT_NS

from atlas.scanner import BlindSpotObservation, BlindSpotStatus, RankBand, blindspot_metrics


def _observation(slot: int, instrument: str, value: float, *, top: bool, exploration: bool,
                 probability: float = 0.5, warm: bool = True, deadline: bool = True,
                 warmup_required: bool = True, deadline_applicable: bool = True) -> BlindSpotObservation:
    return BlindSpotObservation(slot, instrument, RankBand.TOP_3 if top else RankBand.B1, top, exploration,
                                probability, value, warm, deadline, warmup_required, deadline_applicable)


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


def test_not_selected_instruments_are_excluded_from_warmup_and_deadline_denominators():
    observations = [
        _observation(EPOCH_NS, "BTCUSDT", 1.0, top=True, exploration=False, probability=0.0),
        _observation(EPOCH_NS, "DOGEUSDT", -0.1, top=False, exploration=False, probability=0.0,
                     warm=False, deadline=False, warmup_required=False, deadline_applicable=False),
        _observation(EPOCH_NS, "AVAXUSDT", -0.2, top=False, exploration=False, probability=0.0,
                     warm=False, deadline=False, warmup_required=False, deadline_applicable=False),
    ]
    metrics = blindspot_metrics(observations)
    assert metrics.warmup_exclusion_rate == 0.0
    assert metrics.deadline_loss_rate == 0.0

    only_not_selected = tuple(item for item in observations if not item.warmup_required)
    metrics = blindspot_metrics(only_not_selected)
    assert metrics.warmup_exclusion_rate is None
    assert metrics.deadline_loss_rate is None


def test_deep_missing_warmup_and_capital_or_exploration_requirements_count():
    observations = [
        _observation(EPOCH_NS, "ETHUSDT", 1.0, top=True, exploration=False, probability=0.0,
                     warm=True, deadline=True),
        _observation(EPOCH_NS, "SOLUSDT", -0.1, top=False, exploration=False, probability=0.0,
                     warm=False, deadline=False),
        _observation(EPOCH_NS, "XRPUSDT", -0.2, top=False, exploration=True, probability=0.25,
                     warm=False, deadline=False),
    ]
    metrics = blindspot_metrics(observations)
    assert metrics.warmup_exclusion_rate == 2 / 3
    assert metrics.deadline_loss_rate == 2 / 3
