"""Typed model ABI declarations; this package contains no model runtime."""

from .protocol import (
    ForecastArtifactV2,
    ModelManifestV2,
    ModelRequestV2,
    PromotionStatusV2,
)

__all__ = [
    "ForecastArtifactV2",
    "ModelManifestV2",
    "ModelRequestV2",
    "PromotionStatusV2",
]
