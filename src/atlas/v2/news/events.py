"""Versioned S7 collection, event safety, alerts and directional shadow.

All network access is public and injectable. Raw response bytes are durably
indexed before deterministic parsing. The module exposes no credentials or
capital/order controls.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from email.utils import parsedate_to_datetime
from enum import StrEnum
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from atlas.v2._serialization import artifact_wire, seal_envelope, sha256_json, timestamp
from atlas.v2.contracts import ArtifactEnvelope, OpportunityWatchV2, V2Side, WatchStateV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.raw import AvailabilityClassV2
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.strategies.s1_trend import EventGate, EventState

NEWS_EVENT_SCHEMA_V2 = "NEWS_EVENT_V2"
EVENT_SAFETY_GATE_VERSION = "EVENT_SAFETY_GATE_V2_1"
CALENDAR_REQUIRED_BEFORE_NS = 15 * 60 * 1_000_000_000
CALENDAR_REQUIRED_AFTER_NS = 30 * 60 * 1_000_000_000
EVENT_REACTION_VERSION = "S7_DIRECTIONAL_REACTION_V1"
ALERT_VERSION = "EVENT_ALERT_V2_1"
PRODUCER_VERSION = "S7_EVENT_PIPELINE_V2_1"
RAW_BODY_MAX_BYTES = 5_000_000
CALENDAR_MAX_AGE_NS = 24 * 60 * 60 * 1_000_000_000
ABNORMALITY_MAX_AGE_NS = 60 * 60 * 1_000_000_000
REACTION_SOURCE_HEALTH_MAX_AGE_NS = 60_000_000_000
REACTION_SPREAD_MAX_AGE_NS = 1_000_000_000
REACTION_BAR_NS = BarIntervalV2.M5.duration_ns


class NewsSourceClassV2(StrEnum):
    OFFICIAL_MACRO = "OFFICIAL_MACRO"
    OFFICIAL_VENUE_PROJECT_SECURITY = "OFFICIAL_VENUE_PROJECT_SECURITY"
    REPUTABLE_NEWS = "REPUTABLE_NEWS"
    DISCOVERY_AGGREGATOR = "DISCOVERY_AGGREGATOR"


class SourceQualificationV2(StrEnum):
    UNVERIFIED = "UNVERIFIED"
    VERIFIED = "VERIFIED"


class AssetMappingStateV2(StrEnum):
    UNAMBIGUOUS = "UNAMBIGUOUS"
    UNMAPPED = "UNMAPPED"
    AMBIGUOUS = "AMBIGUOUS"


class EventGateStateV2(StrEnum):
    CLEAR = "CLEAR"
    BLOCKED = "BLOCKED"
    UNKNOWN = "UNKNOWN"


class AbnormalityStateV2(StrEnum):
    NORMAL = "NORMAL"
    ABNORMAL = "ABNORMAL"
    UNKNOWN = "UNKNOWN"


class IncidentStateV2(StrEnum):
    OPEN = "OPEN"
    VERIFIED_RESOLVED = "VERIFIED_RESOLVED"
    HUMAN_REVIEW_RESOLVED = "HUMAN_REVIEW_RESOLVED"


@dataclass(frozen=True)
class NewsSourceConfigV2:
    source_id: str
    source_class: NewsSourceClassV2
    feed_url: str
    allowed_hosts: tuple[str, ...]
    parser: str = "RSS_ATOM_OR_JSON_V1"
    qualification: SourceQualificationV2 = SourceQualificationV2.UNVERIFIED

    def __post_init__(self) -> None:
        if not self.source_id or not self.allowed_hosts:
            raise ValueError("news source identity and allowlisted host are required")
        object.__setattr__(self, "source_class", NewsSourceClassV2(self.source_class))
        object.__setattr__(self, "qualification", SourceQualificationV2(self.qualification))
        hosts = tuple(sorted({item.lower() for item in self.allowed_hosts}))
        object.__setattr__(self, "allowed_hosts", hosts)
        parsed = urlsplit(self.feed_url)
        if parsed.scheme != "https" or parsed.hostname is None or parsed.hostname.lower() not in hosts:
            raise ValueError("news feed URL must use HTTPS on its explicit host allowlist")

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "source_id": self.source_id, "source_class": self.source_class.value,
                "feed_url": self.feed_url, "allowed_hosts": list(self.allowed_hosts),
                "parser": self.parser, "qualification": self.qualification.value}


DEFAULT_NEWS_SOURCES_V2 = (
    NewsSourceConfigV2("FEDERAL_RESERVE", NewsSourceClassV2.OFFICIAL_MACRO,
                       "https://www.federalreserve.gov/feeds/press_all.xml", ("www.federalreserve.gov",)),
    NewsSourceConfigV2("BLS", NewsSourceClassV2.OFFICIAL_MACRO,
                       "https://www.bls.gov/feed/news_release/rss.xml", ("www.bls.gov",)),
    NewsSourceConfigV2("SEC", NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
                       "https://www.sec.gov/news/pressreleases.rss", ("www.sec.gov",)),
    NewsSourceConfigV2("BYBIT_OFFICIAL", NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
                       "https://announcements.bybit.com/en-US/", ("announcements.bybit.com",)),
    NewsSourceConfigV2("BYBIT_STATUS", NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
                       "https://api.bybit.com/v5/system/status", ("api.bybit.com",)),
    NewsSourceConfigV2("BINANCE_OFFICIAL", NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
                       "https://www.binance.com/en/support/announcement", ("www.binance.com",)),
    NewsSourceConfigV2("BINANCE_STATUS", NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
                       "https://api.binance.com/sapi/v1/system/status", ("api.binance.com",)),
    NewsSourceConfigV2("GDELT_DISCOVERY", NewsSourceClassV2.DISCOVERY_AGGREGATOR,
                       "https://api.gdeltproject.org/api/v2/doc/doc?query=cryptocurrency&mode=ArtList&format=json",
                       ("api.gdeltproject.org",)),
    NewsSourceConfigV2("COIN_METRICS_COMMUNITY", NewsSourceClassV2.REPUTABLE_NEWS,
                       "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics",
                       ("community-api.coinmetrics.io",)),
)


def official_project_security_source_v2(source_id: str, feed_url: str) -> NewsSourceConfigV2:
    """Create an explicitly allowlisted official project/security source."""
    host = urlsplit(feed_url).hostname
    if host is None:
        raise ValueError("official project/security source URL must include a host")
    return NewsSourceConfigV2(
        source_id, NewsSourceClassV2.OFFICIAL_VENUE_PROJECT_SECURITY,
        feed_url, (host,), qualification=SourceQualificationV2.UNVERIFIED,
    )


def canonical_url(url: str) -> str:
    parsed = urlsplit(url.strip())
    if parsed.scheme not in ("http", "https") or parsed.hostname is None:
        raise ValueError("news event URL must be an absolute HTTP(S) URL")
    host = parsed.hostname.lower()
    port = parsed.port
    netloc = host if port in (None, 80, 443) else f"{host}:{port}"
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    query = [(key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)
             if not key.lower().startswith("utm_") and key.lower() not in {"gclid", "fbclid", "mc_cid", "mc_eid"}]
    return urlunsplit((parsed.scheme.lower(), netloc, path, urlencode(sorted(query)), ""))


def _published_ns(value: str | None) -> int | None:
    if not value:
        return None
    parsed: datetime | None = None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            return None
    if parsed.tzinfo is None:
        return None
    return int(parsed.astimezone(UTC).timestamp() * 1_000_000_000)


@dataclass(frozen=True)
class FetchedDocumentV2:
    requested_url: str
    final_url: str
    status_code: int
    body: bytes
    received_at_ns: int
    headers: Mapping[str, str]

    def __post_init__(self) -> None:
        timestamp(self.received_at_ns, field="news.received_at_ns")
        if type(self.status_code) is not int or not 100 <= self.status_code <= 599:
            raise ValueError("invalid HTTP status code")
        if not isinstance(self.body, bytes) or len(self.body) > RAW_BODY_MAX_BYTES:
            raise ValueError("news raw response exceeds byte bound or is not bytes")


class PublicNewsTransportV2:
    """Minimal allowlist-bound no-credential HTTPS fetcher."""

    def __init__(self, *, clock_ns: Callable[[], int], timeout_s: float = 10.0) -> None:
        self.clock_ns = clock_ns
        self.timeout_s = timeout_s

    def fetch(self, source: NewsSourceConfigV2) -> FetchedDocumentV2:
        request = urllib.request.Request(source.feed_url, headers={"User-Agent": "ATLAS-research/2.1", "Accept": "application/rss+xml, application/atom+xml, application/json, */*"})
        with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
            final_url = response.geturl()
            host = urlsplit(final_url).hostname
            if host is None or host.lower() not in source.allowed_hosts:
                raise ValueError("news transport redirected outside the source host allowlist")
            body = response.read(RAW_BODY_MAX_BYTES + 1)
            return FetchedDocumentV2(source.feed_url, final_url, response.status, body,
                                     self.clock_ns(), dict(response.headers.items()))


@dataclass(frozen=True)
class ParsedNewsItemV2:
    title: str
    url: str
    published_claim: str | None
    text: str


def parse_feed_bytes_v2(body: bytes, *, base_url: str) -> tuple[ParsedNewsItemV2, ...]:
    """Deterministic RSS/Atom and small JSON-list parser; no LLM dependency."""
    stripped = body.lstrip()
    if stripped.startswith((b"{", b"[")):
        payload = json.loads(body.decode("utf-8"))
        if isinstance(payload, Mapping):
            raw_items = payload.get("articles", payload.get("results", payload.get("data", [])))
            result_block = payload.get("result")
            if isinstance(result_block, Mapping) and isinstance(result_block.get("list"), list):
                raw_items = result_block["list"]
            if not raw_items and "status" in payload:
                status = payload.get("status")
                if status not in (0, "0", "normal", "NORMAL"):
                    message = str(payload.get("msg", payload.get("message", f"status={status}")))
                    return (ParsedNewsItemV2("Official venue system status", base_url, None, message),)
        else:
            raw_items = payload
        if not isinstance(raw_items, list):
            return ()
        json_items: list[ParsedNewsItemV2] = []
        for item in raw_items:
            if not isinstance(item, Mapping):
                continue
            title = str(item.get("title", item.get("name", ""))).strip()
            link = str(item.get("url", item.get("link", base_url))).strip()
            text = str(item.get("description", item.get("summary", item.get("text", "")))).strip()
            published = item.get("publishedAt", item.get("published_at", item.get("datePublished")))
            if not published and item.get("begin") is not None:
                try:
                    published = datetime.fromtimestamp(int(item["begin"]) / 1000, UTC).isoformat()
                except (TypeError, ValueError, OverflowError, OSError):
                    published = None
            state = item.get("state")
            begin, end = item.get("begin"), item.get("end")
            if state is not None:
                text = " ".join(part for part in (text, f"system status {state}",
                                                     f"begin {begin}" if begin is not None else "",
                                                     f"end {end}" if end is not None else "") if part)
            if link == base_url and item.get("id"):
                parsed_base = urlsplit(base_url)
                query = dict(parse_qsl(parsed_base.query, keep_blank_values=True))
                query["id"] = str(item["id"])
                link = urlunsplit((parsed_base.scheme, parsed_base.netloc, parsed_base.path,
                                   urlencode(sorted(query.items())), parsed_base.fragment))
            if title:
                json_items.append(ParsedNewsItemV2(title, urljoin(base_url, link),
                                                   str(published) if published else None, text))
        return tuple(json_items)
    root = ET.fromstring(body)
    result: list[ParsedNewsItemV2] = []
    for item in root.iter():
        local_tag = item.tag.rsplit("}", 1)[-1].lower() if isinstance(item.tag, str) else ""
        if local_tag not in {"item", "entry"}:
            continue
        children: dict[str, list[ET.Element]] = {}
        for child in item:
            name = child.tag.rsplit("}", 1)[-1].lower() if isinstance(child.tag, str) else ""
            children.setdefault(name, []).append(child)
        def value(*names: str, source_children: Mapping[str, list[ET.Element]] = children) -> str | None:
            for name in names:
                candidates = source_children.get(name, [])
                if candidates:
                    candidate = candidates[0]
                    return candidate.attrib.get("href") or "".join(candidate.itertext()).strip()
            return None
        title = value("title") or ""
        link = value("link", "id") or base_url
        published = value("published", "pubdate", "updated", "date")
        text = value("summary", "description", "content:encoded", "content") or ""
        if title:
            result.append(ParsedNewsItemV2(title, urljoin(base_url, link), published, text))
    return tuple(result)


def _semantic_text(title: str, text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", f"{title} {text}".lower()))


def classify_event_v2(title: str, text: str) -> tuple[str, str, Decimal]:
    content = _semantic_text(title, text)
    rules = (
        (("consumer price index", "cpi release", "inflation report"), "US_CPI", "HIGH", Decimal("0.95")),
        (("nonfarm payroll", "employment situation", "payrolls"), "US_PAYROLL", "HIGH", Decimal("0.95")),
        (("fomc", "federal funds rate", "rate decision"), "FOMC_RATE_DECISION", "HIGH", Decimal("0.95")),
        (("security incident", "exploit", "hack", "vulnerability", "breach"), "SECURITY_INCIDENT", "HIGH", Decimal("0.90")),
        (("service disruption", "system status", "maintenance", "trading suspended"), "VENUE_INCIDENT", "HIGH", Decimal("0.85")),
    )
    for tokens, event_type, severity, confidence in rules:
        if any(token in content for token in tokens):
            return event_type, severity, confidence
    if "announcement" in content or "launch" in content or "listing" in content:
        return "OFFICIAL_ANNOUNCEMENT", "MEDIUM", Decimal("0.65")
    return "GENERAL_CONTEXT", "LOW", Decimal("0.50")


def map_assets_v2(text: str, aliases: Mapping[str, str]) -> tuple[AssetMappingStateV2, tuple[str, ...]]:
    matches: set[str] = set()
    folded = text.casefold()
    for alias, canonical_asset_id in aliases.items():
        if not alias or not canonical_asset_id:
            continue
        pattern = rf"(?<![a-z0-9]){re.escape(alias.casefold())}(?![a-z0-9])"
        if re.search(pattern, folded):
            matches.add(canonical_asset_id)
    if not matches:
        return AssetMappingStateV2.UNMAPPED, ()
    ordered = tuple(sorted(matches))
    return (AssetMappingStateV2.UNAMBIGUOUS, ordered) if len(ordered) == 1 else (AssetMappingStateV2.AMBIGUOUS, ())


@dataclass(frozen=True)
class NewsEventV2:
    event_id: str
    source_id: str
    source_class: NewsSourceClassV2
    source_url: str
    content_hash: str
    claimed_published_at_ns: int | None
    received_at_ns: int
    extraction_completed_ns: int
    affected_asset_ids: tuple[str, ...]
    event_type: str
    severity: str
    confidence: Decimal
    supporting_spans: tuple[str, ...]
    duplicate_group: str
    raw_ref: str
    receipt_ref: str
    source_health_ref: str
    authentication_state: str = "UNKNOWN"
    authentication_ref: str | None = None
    asset_mapping_state: AssetMappingStateV2 = AssetMappingStateV2.UNMAPPED
    supersedes_id: str | None = None
    expires_at_ns: int | None = None
    schema_version: int = 2
    source_health_state: str = "UNKNOWN"

    def __post_init__(self) -> None:
        if self.schema_version != 2:
            raise ValueError("unsupported NewsEventV2 schema")
        object.__setattr__(self, "source_class", NewsSourceClassV2(self.source_class))
        object.__setattr__(self, "asset_mapping_state", AssetMappingStateV2(self.asset_mapping_state))
        for name in ("received_at_ns", "extraction_completed_ns"):
            timestamp(getattr(self, name), field=f"NewsEventV2.{name}")
        if self.claimed_published_at_ns is not None:
            timestamp(self.claimed_published_at_ns, field="NewsEventV2.claimed_published_at_ns")
        if self.expires_at_ns is not None:
            timestamp(self.expires_at_ns, field="NewsEventV2.expires_at_ns")
        if self.extraction_completed_ns < self.received_at_ns:
            raise ValueError("event availability cannot precede actual receipt and extraction")
        object.__setattr__(self, "confidence", Decimal(self.confidence))
        if not self.confidence.is_finite() or not 0 <= self.confidence <= 1:
            raise ValueError("event confidence must be within [0,1]")
        assets = tuple(sorted(set(self.affected_asset_ids)))
        if assets != self.affected_asset_ids:
            raise ValueError("affected assets must be unique canonical IDs in stable order")
        if self.asset_mapping_state != AssetMappingStateV2.UNAMBIGUOUS and assets:
            raise ValueError("ambiguous/unmapped asset evidence cannot claim affected assets")
        if self.authentication_state not in ("UNKNOWN", "AUTHENTICATED", "EXPLICITLY_RESOLVED"):
            raise ValueError("invalid source authentication state")
        if self.source_health_state not in {item.value for item in PublicSourceStateV2} | {"UNKNOWN"}:
            raise ValueError("invalid NewsEventV2 source health state")
        if self.authentication_state in ("AUTHENTICATED", "EXPLICITLY_RESOLVED") and self.authentication_ref is None:
            raise ValueError("authenticated/resolved news requires explicit source evidence")
        for name in ("event_id", "source_id", "source_url", "event_type", "severity", "duplicate_group"):
            if not getattr(self, name):
                raise ValueError(f"NewsEventV2 {name} is required")
        for name, value in (("content_hash", self.content_hash), ("raw_ref", self.raw_ref),
                            ("receipt_ref", self.receipt_ref), ("source_health_ref", self.source_health_ref),
                            ("duplicate_group", self.duplicate_group)):
            if len(value) != 64:
                raise ValueError(f"NewsEventV2 {name} must be a SHA-256 ref")
        if self.authentication_ref is not None and len(self.authentication_ref) != 64:
            raise ValueError("authentication_ref must be SHA-256")
        if self.supersedes_id is not None and len(self.supersedes_id) != 64:
            raise ValueError("supersedes_id must be SHA-256")
        spans = tuple(span for span in self.supporting_spans if span)
        object.__setattr__(self, "supporting_spans", spans)

    @property
    def available_at_ns(self) -> int:
        return max(self.received_at_ns, self.extraction_completed_ns)

    @property
    def semantic_hash(self) -> str:
        return sha256_json(self._body())

    def _body(self) -> dict[str, object]:
        return {"schema_version": self.schema_version, "event_id": self.event_id,
                "source_id": self.source_id, "source_class": self.source_class.value,
                "source_url": self.source_url, "content_hash": self.content_hash,
                "claimed_published_at_ns": self.claimed_published_at_ns,
                "received_at_ns": self.received_at_ns, "extraction_completed_ns": self.extraction_completed_ns,
                "available_at_ns": self.available_at_ns, "affected_asset_ids": list(self.affected_asset_ids),
                "event_type": self.event_type, "severity": self.severity,
                "confidence": str(self.confidence), "supporting_spans": list(self.supporting_spans),
                "duplicate_group": self.duplicate_group, "raw_ref": self.raw_ref,
                "receipt_ref": self.receipt_ref, "source_health_ref": self.source_health_ref,
                "source_health_state": self.source_health_state,
                "authentication_state": self.authentication_state, "authentication_ref": self.authentication_ref,
                "asset_mapping_state": self.asset_mapping_state.value,
                "supersedes_id": self.supersedes_id, "expires_at_ns": self.expires_at_ns}

    def to_dict(self) -> dict[str, object]:
        return self._body()


@dataclass(frozen=True)
class EventAlertV2:
    alert_id: str
    event_ref: str
    duplicate_group: str
    created_at_ns: int
    available_at_ns: int
    relevance: str
    delivery_status: str = "UNVERIFIED"
    schema_version: int = 2

    def __post_init__(self) -> None:
        timestamp(self.created_at_ns, field="EventAlertV2.created_at_ns")
        timestamp(self.available_at_ns, field="EventAlertV2.available_at_ns")
        if self.available_at_ns < self.created_at_ns:
            raise ValueError("alert availability cannot precede creation")
        if self.delivery_status != "UNVERIFIED":
            raise ValueError("external alert delivery was not exercised")
        if self.schema_version != 2 or not self.relevance:
            raise ValueError("invalid EventAlertV2 schema or relevance")
        if any(len(ref) != 64 for ref in (self.alert_id, self.event_ref, self.duplicate_group)):
            raise ValueError("EventAlertV2 refs must be SHA-256")

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": self.schema_version, "alert_id": self.alert_id,
                "event_ref": self.event_ref, "duplicate_group": self.duplicate_group,
                "created_at_ns": self.created_at_ns, "available_at_ns": self.available_at_ns,
                "relevance": self.relevance, "delivery_status": self.delivery_status,
                "delivery_qualification": "UNVERIFIED"}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class NewsCollectionResultV2:
    raw_ref: str
    event_refs: tuple[str, ...]
    alert_refs: tuple[str, ...]
    duplicate_count: int
    raw_hash: str


@dataclass(frozen=True)
class _StoredNewsRow:
    title: str
    text: str
    canonical_url: str
    claimed_published_at_ns: int | None
    content_hash: str


class NewsCollectionPipelineV2:
    def __init__(
        self, repository: OpsRepository, *, clock_ns: Callable[[], int],
        transport: object, sources: Sequence[NewsSourceConfigV2] = DEFAULT_NEWS_SOURCES_V2,
        parser: Callable[[bytes, str], Sequence[ParsedNewsItemV2]] = lambda body, url: parse_feed_bytes_v2(body, base_url=url),
        asset_aliases: Mapping[str, str] | None = None,
        source_health: Callable[[str, int], PublicSourceHealthV2 | None] | None = None,
        authentication: Callable[[NewsSourceConfigV2, int], tuple[str, str | None]] | None = None,
    ) -> None:
        self.repository = repository
        self.clock_ns = clock_ns
        self.transport = transport
        self.sources = {source.source_id: source for source in sources}
        if len(self.sources) != len(tuple(sources)):
            raise ValueError("news source IDs must be unique")
        self.parser = parser
        self.asset_aliases = dict(asset_aliases or {})
        self.source_health = source_health
        self.authentication = authentication or (lambda _source, _at: ("UNKNOWN", None))

    def _fetch(self, source: NewsSourceConfigV2) -> FetchedDocumentV2:
        fetch = getattr(self.transport, "fetch", None)
        response = fetch(source) if callable(fetch) else self.transport(source)  # type: ignore[operator]
        if not isinstance(response, FetchedDocumentV2):
            raise TypeError("injectable news transport must return FetchedDocumentV2")
        if response.status_code < 200 or response.status_code >= 300:
            raise ValueError("news source returned a non-success HTTP status")
        final = urlsplit(response.final_url)
        if final.scheme != "https" or final.hostname is None or final.hostname.lower() not in source.allowed_hosts:
            raise ValueError("news response escaped its configured HTTPS host allowlist")
        return response

    def _archive_raw(self, source: NewsSourceConfigV2, response: FetchedDocumentV2) -> tuple[str, str]:
        digest = hashlib.sha256(response.body).hexdigest()
        document_url = canonical_url(response.final_url)
        raw_ref = sha256_json({"artifact_type": "NewsRawPayloadV2", "source_id": source.source_id,
                               "source_url": document_url, "raw_payload_hash": digest})
        existing = self.repository.get_artifact(raw_ref)
        if existing is None:
            self.repository.register_artifact(ArtifactIndexEntryV2(
                raw_ref, "NewsRawPayloadV2", digest, response.received_at_ns, response.received_at_ns,
                {"schema_version": 2, "source_id": source.source_id, "source_class": source.source_class.value,
                 "source_url": document_url, "raw_payload_hash": digest,
                 "raw_bytes_base64": base64.b64encode(response.body).decode("ascii"),
                 "qualification": source.qualification.value},
            ))
        elif existing.content_hash != digest:
            raise ValueError("raw event archive identity conflicts with stored bytes")
        receipt = {"schema_version": 1, "raw_ref": raw_ref, "source_id": source.source_id,
                   "source_url": document_url, "received_at_ns": response.received_at_ns}
        receipt_ref = sha256_json(receipt)
        self.repository.register_artifact(ArtifactIndexEntryV2(
            receipt_ref, "NewsReceiptV2", receipt_ref, response.received_at_ns, response.received_at_ns, receipt,
        ))
        return raw_ref, receipt_ref

    def _events(self, source_id: str) -> tuple[NewsEventV2, ...]:
        result = []
        for entry in self.repository.artifact_entries("NewsEventV2"):
            body = entry.metadata.get("event")
            if isinstance(body, Mapping) and body.get("source_id") == source_id:
                result.append(_event_from_dict(body))
        return tuple(result)

    def collect(self, source_id: str) -> NewsCollectionResultV2:
        source = self.sources.get(source_id)
        if source is None:
            raise KeyError(f"unconfigured news source: {source_id}")
        response = self._fetch(source)
        raw_ref, receipt_ref = self._archive_raw(source, response)
        # The immutable raw payload and receipt are committed before parser invocation.
        items = tuple(self.parser(response.body, response.final_url))
        extracted_at = self.clock_ns()
        if extracted_at < response.received_at_ns:
            raise ValueError("news extraction completion cannot precede actual receipt")
        health = self.source_health(source_id, extracted_at) if self.source_health else None
        if health is None or health.available_at_ns > extracted_at:
            health_state = "UNKNOWN"
            unavailable = {"schema_version": 1, "source_id": source_id, "state": "UNKNOWN",
                           "available_at_ns": extracted_at, "reason": "SOURCE_HEALTH_UNAVAILABLE_AT_EXTRACTION"}
            health_ref = sha256_json(unavailable)
            self.repository.register_artifact(ArtifactIndexEntryV2(
                health_ref, "NewsSourceHealthUnavailableV2", health_ref, extracted_at, extracted_at,
                {"health": unavailable},
            ))
        else:
            health_ref = health.content_hash
            health_state = health.state.value
            self.repository.register_artifact(ArtifactIndexEntryV2(
                health_ref, "PublicSourceHealthV2", health_ref, health.available_at_ns,
                health.available_at_ns, {"health": health.to_dict()},
            ))
        auth_state, auth_ref = self.authentication(source, extracted_at)
        if auth_state in ("AUTHENTICATED", "EXPLICITLY_RESOLVED") and (
                auth_ref is None or not _source_evidence_available(self.repository, auth_ref, extracted_at)):
            auth_state, auth_ref = "UNKNOWN", None
        stored = self._events(source_id)
        alerts = self.repository.artifact_entries("EventAlertV2")
        duplicate_groups_alerted = {entry.metadata.get("duplicate_group") for entry in alerts}
        event_refs: list[str] = []
        alert_refs: list[str] = []
        duplicates = 0
        for item in items:
            item_url = canonical_url(urljoin(response.final_url, item.url))
            published = _published_ns(item.published_claim)
            item_text = " ".join(item.text.split())
            title = " ".join(item.title.split())
            item_bytes = json.dumps({"title": title, "text": item_text, "url": item_url},
                                    sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            item_hash = hashlib.sha256(item_bytes).hexdigest()
            prior = [event for event in stored if event.source_url == item_url]
            # Authentication can become explicitly known after first receipt.
            # Treat that state transition as a new immutable event revision while
            # keeping repeated identical content/authentication idempotent.
            exact = next((event for event in prior
                          if event.content_hash == item_hash
                          and event.authentication_state == auth_state), None)
            if exact is not None:
                event_refs.append(exact.event_id)
                duplicates += 1
                continue
            event_type, severity, confidence = classify_event_v2(title, item_text)
            mapping_state, asset_ids = map_assets_v2(f"{title} {item_text}", self.asset_aliases)
            semantic_text = _semantic_text(title, item_text)
            duplicate_group = sha256_json({"event_type": event_type, "semantic_text": semantic_text})
            supersedes = max(prior, key=lambda event: (event.available_at_ns, event.event_id)).event_id if prior else None
            identity = {"schema_version": 2, "source_id": source_id,
                        "source_url": item_url, "content_hash": item_hash}
            if supersedes is not None:
                identity.update({"supersedes_id": supersedes,
                                 "authentication_state": auth_state,
                                 "authentication_ref": auth_ref})
            event_id = sha256_json(identity)
            event = NewsEventV2(
                event_id, source_id, source.source_class, item_url, item_hash, published,
                response.received_at_ns, extracted_at, asset_ids, event_type, severity, confidence,
                tuple(span for span in (title, item_text[:400]) if span), duplicate_group,
                raw_ref, receipt_ref, health_ref, auth_state, auth_ref, mapping_state, supersedes,
                source_health_state=health_state,
            )
            self.repository.register_artifact(ArtifactIndexEntryV2(
                event.event_id, "NewsEventV2", event.semantic_hash, response.received_at_ns,
                event.available_at_ns, {"event": event.to_dict()},
            ))
            stored = (*stored, event)
            event_refs.append(event.event_id)
            related: list[NewsEventV2] = []
            for indexed in self.repository.artifact_entries("NewsEventV2"):
                indexed_body = indexed.metadata.get("event")
                if isinstance(indexed_body, Mapping) and indexed_body.get("duplicate_group") == duplicate_group:
                    related.append(_event_from_dict(indexed_body))
            if len({other.source_id for other in related}) > 1:
                conflict = {"schema_version": 1, "duplicate_group": duplicate_group,
                            "event_refs": sorted(other.event_id for other in related),
                            "source_ids": sorted({other.source_id for other in related}),
                            "status": "SOURCE_CONFLICT_RETAINED"}
                conflict_ref = sha256_json(conflict)
                self.repository.register_artifact(ArtifactIndexEntryV2(
                    conflict_ref, "NewsSourceConflictV2", conflict_ref, response.received_at_ns,
                    event.available_at_ns, conflict,
                ))
            relevant = event.severity in ("HIGH", "CRITICAL") or event.event_type in (
                "US_CPI", "US_PAYROLL", "FOMC_RATE_DECISION", "VENUE_INCIDENT", "SECURITY_INCIDENT")
            if (relevant and event.authentication_state in ("AUTHENTICATED", "EXPLICITLY_RESOLVED")
                    and event.duplicate_group not in duplicate_groups_alerted):
                alert_id = sha256_json({"event_alert": ALERT_VERSION, "duplicate_group": event.duplicate_group})
                alert = EventAlertV2(alert_id, event.event_id, event.duplicate_group,
                                     event.available_at_ns, event.available_at_ns, "AUTHENTICATED_RELEVANT_EVENT")
                self.repository.register_artifact(ArtifactIndexEntryV2(
                    alert.alert_id, "EventAlertV2", alert.content_hash, event.available_at_ns,
                    alert.available_at_ns, {"alert": alert.to_dict(), "duplicate_group": event.duplicate_group},
                ))
                alert_refs.append(alert.alert_id)
                duplicate_groups_alerted.add(event.duplicate_group)
        return NewsCollectionResultV2(raw_ref, tuple(event_refs), tuple(alert_refs), duplicates,
                                      hashlib.sha256(response.body).hexdigest())


def _event_from_dict(body: Mapping[str, object]) -> NewsEventV2:
    published = body.get("claimed_published_at_ns")
    assets = body.get("affected_asset_ids", ())
    spans = body.get("supporting_spans", ())
    auth_ref = body.get("authentication_ref")
    supersedes = body.get("supersedes_id")
    expires = body.get("expires_at_ns")
    if not isinstance(assets, (tuple, list)) or not isinstance(spans, (tuple, list)):
        raise ValueError("stored news event assets/spans must be sequences")
    return NewsEventV2(
        str(body["event_id"]), str(body["source_id"]), NewsSourceClassV2(str(body["source_class"])),
        str(body["source_url"]), str(body["content_hash"]),
        int(published) if isinstance(published, int) else None,
        int(str(body["received_at_ns"])), int(str(body["extraction_completed_ns"])),
        tuple(str(value) for value in assets), str(body["event_type"]),
        str(body["severity"]), Decimal(str(body["confidence"])),
        tuple(str(value) for value in spans), str(body["duplicate_group"]),
        str(body["raw_ref"]), str(body["receipt_ref"]), str(body["source_health_ref"]),
        str(body.get("authentication_state", "UNKNOWN")), str(auth_ref) if auth_ref is not None else None,
        AssetMappingStateV2(str(body.get("asset_mapping_state", "UNMAPPED"))),
        str(supersedes) if supersedes is not None else None,
        int(expires) if isinstance(expires, int) else None, int(str(body.get("schema_version", 2))),
        str(body.get("source_health_state", "UNKNOWN")),
    )


@dataclass(frozen=True)
class ScheduledEventV2:
    event_id: str
    classification: str
    scheduled_at_ns: int
    schedule_revision: str
    source_id: str
    source_event_at_ns: int
    received_at_ns: int
    available_at_ns: int
    evidence_ref: str

    def __post_init__(self) -> None:
        if self.classification not in ("US_CPI", "US_PAYROLL", "FOMC_RATE_DECISION"):
            raise ValueError("unsupported S7 scheduled-event classification")
        for name in ("scheduled_at_ns", "source_event_at_ns", "received_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=f"ScheduledEventV2.{name}")
        if self.available_at_ns < self.received_at_ns:
            raise ValueError("calendar event cannot be available before receipt")
        if self.received_at_ns < self.source_event_at_ns:
            raise ValueError("calendar receipt cannot precede source event time")
        if not self.event_id or not self.schedule_revision or not self.source_id or len(self.evidence_ref) != 64:
            raise ValueError("scheduled event requires revisioned source evidence")

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "event_id": self.event_id, "classification": self.classification,
                "scheduled_at_ns": self.scheduled_at_ns, "schedule_revision": self.schedule_revision,
                "source_id": self.source_id, "source_event_at_ns": self.source_event_at_ns,
                "received_at_ns": self.received_at_ns, "available_at_ns": self.available_at_ns,
                "evidence_ref": self.evidence_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class CalendarCoverageV2:
    source_id: str
    covered_from_ns: int
    covered_through_ns: int
    observed_at_ns: int
    received_at_ns: int
    available_at_ns: int
    complete: bool
    revision: str
    evidence_ref: str
    source_qualification: str = "UNVERIFIED"

    def __post_init__(self) -> None:
        for name in ("covered_from_ns", "covered_through_ns", "observed_at_ns", "received_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=f"CalendarCoverageV2.{name}")
        if (self.covered_through_ns < self.covered_from_ns
                or self.received_at_ns < self.observed_at_ns
                or self.available_at_ns < self.received_at_ns):
            raise ValueError("invalid calendar coverage interval/availability")
        if (type(self.complete) is not bool or not self.source_id or not self.revision
                or len(self.evidence_ref) != 64
                or self.source_qualification not in ("UNVERIFIED", "VERIFIED")):
            raise ValueError("calendar coverage must explicitly state completeness and revision")

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "source_id": self.source_id, "covered_from_ns": self.covered_from_ns,
                "covered_through_ns": self.covered_through_ns, "observed_at_ns": self.observed_at_ns,
                "received_at_ns": self.received_at_ns, "available_at_ns": self.available_at_ns,
                "complete": self.complete, "revision": self.revision, "evidence_ref": self.evidence_ref,
                "source_qualification": self.source_qualification}


@dataclass(frozen=True)
class AbnormalityEvidenceV2:
    state: AbnormalityStateV2
    observed_at_ns: int
    available_at_ns: int
    evidence_ref: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", AbnormalityStateV2(self.state))
        timestamp(self.observed_at_ns, field="abnormality.observed_at_ns")
        timestamp(self.available_at_ns, field="abnormality.available_at_ns")
        if self.available_at_ns < self.observed_at_ns or len(self.evidence_ref) != 64:
            raise ValueError("abnormality evidence is not causal/content-addressed")

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "state": self.state.value, "observed_at_ns": self.observed_at_ns,
                "available_at_ns": self.available_at_ns, "evidence_ref": self.evidence_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class OperationalIncidentV2:
    incident_id: str
    venue: str
    asset_id: str | None
    state: IncidentStateV2
    revision: int
    observed_at_ns: int
    available_at_ns: int
    evidence_ref: str
    supersedes_id: str | None = None
    resolved_by_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", IncidentStateV2(self.state))
        timestamp(self.observed_at_ns, field="incident.observed_at_ns")
        timestamp(self.available_at_ns, field="incident.available_at_ns")
        if self.available_at_ns < self.observed_at_ns or self.revision < 1:
            raise ValueError("incident chronology/revision is invalid")
        if not self.incident_id or not self.venue or len(self.evidence_ref) != 64:
            raise ValueError("incident identity/evidence required")
        if self.supersedes_id is not None and not self.supersedes_id:
            raise ValueError("incident supersession identity is invalid")
        if self.state in (IncidentStateV2.VERIFIED_RESOLVED, IncidentStateV2.HUMAN_REVIEW_RESOLVED) and not self.resolved_by_ref:
            raise ValueError("incident resolution requires explicit available evidence")
        if self.supersedes_id is not None and len(self.supersedes_id) != 64:
            raise ValueError("incident supersession reference must be SHA-256")
        if self.resolved_by_ref is not None and len(self.resolved_by_ref) != 64:
            raise ValueError("incident resolution reference must be SHA-256")

    def applies_to(self, key: InstrumentKeyV2) -> bool:
        return self.venue == key.venue.value and (self.asset_id is None or self.asset_id == key.base_asset_id)

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, "incident_id": self.incident_id, "venue": self.venue,
                "asset_id": self.asset_id, "state": self.state.value, "revision": self.revision,
                "observed_at_ns": self.observed_at_ns, "available_at_ns": self.available_at_ns,
                "evidence_ref": self.evidence_ref, "supersedes_id": self.supersedes_id,
                "resolved_by_ref": self.resolved_by_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


@dataclass(frozen=True)
class EventSafetyGateV2:
    envelope: ArtifactEnvelope
    cutoff_ns: int
    state: EventGateStateV2
    blocked: bool
    reasons: tuple[str, ...]
    scheduled_event_ref: str | None
    schedule_revision: str | None
    calendar_source_id: str | None
    abnormality_state: AbnormalityStateV2
    incident_refs: tuple[str, ...]
    gate_version: str = EVENT_SAFETY_GATE_VERSION

    ARTIFACT_TYPE = "EventSafetyGateV2"

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", EventGateStateV2(self.state))
        object.__setattr__(self, "abnormality_state", AbnormalityStateV2(self.abnormality_state))
        timestamp(self.cutoff_ns, field="EventSafetyGateV2.cutoff_ns")
        if self.blocked != (self.state != EventGateStateV2.CLEAR):
            raise ValueError("S7 event gate blocks unless state is explicitly CLEAR")
        if self.gate_version != EVENT_SAFETY_GATE_VERSION:
            raise ValueError("unsupported event safety gate version")
        object.__setattr__(self, "reasons", tuple(sorted(set(self.reasons))))
        object.__setattr__(self, "incident_refs", tuple(sorted(set(self.incident_refs))))
        object.__setattr__(self, "envelope", seal_envelope(self.envelope, self._body(),
                                                            artifact_type=self.ARTIFACT_TYPE))

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def _body(self) -> dict[str, object]:
        return {"schema_version": 2, "cutoff_ns": self.cutoff_ns,
                "state": self.state.value, "blocked": self.blocked, "reasons": list(self.reasons),
                "scheduled_event_ref": self.scheduled_event_ref, "schedule_revision": self.schedule_revision,
                "calendar_source_id": self.calendar_source_id,
                "abnormality_state": self.abnormality_state.value, "incident_refs": list(self.incident_refs),
                "gate_version": self.gate_version, "availability_view": "ACTUAL_SYSTEM"}

    def to_dict(self) -> dict[str, object]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)

    def to_s1_event_gate(self) -> EventGate:
        state = {EventGateStateV2.CLEAR: EventState.CLEAR,
                 EventGateStateV2.BLOCKED: EventState.BLOCKED,
                 EventGateStateV2.UNKNOWN: EventState.UNKNOWN}[self.state]
        return EventGate(state, self.envelope.available_at_ns, self.content_hash, self.gate_version)


def _gate_artifact(*, cutoff_ns: int, state: EventGateStateV2, reasons: Iterable[str], refs: Iterable[str],
                   event_ref: str | None, schedule_revision: str | None, source_id: str | None,
                   abnormality: AbnormalityStateV2, incidents: Iterable[str]) -> EventSafetyGateV2:
    ordered_refs = tuple(sorted({ref for ref in refs if ref}))
    why = tuple(sorted(set(reasons)))
    incident_refs = tuple(sorted(set(incidents)))
    body = {"version": EVENT_SAFETY_GATE_VERSION, "cutoff_ns": cutoff_ns, "state": state.value,
            "reasons": list(why), "refs": list(ordered_refs), "scheduled_event_ref": event_ref,
            "schedule_revision": schedule_revision, "calendar_source_id": source_id,
            "abnormality_state": abnormality.value, "incident_refs": list(incident_refs)}
    artifact_id = sha256_json(body)
    envelope = ArtifactEnvelope(1, artifact_id, cutoff_ns, cutoff_ns, PRODUCER_VERSION, ordered_refs)
    return EventSafetyGateV2(envelope, cutoff_ns, state, state != EventGateStateV2.CLEAR, why,
                             event_ref, schedule_revision, source_id, abnormality, incident_refs)


def _source_evidence_available(repository: OpsRepository, ref: str, cutoff_ns: int) -> bool:
    try:
        entry = repository.get_artifact(ref)
    except ValueError:
        return False
    return bool(entry is not None and entry.content_hash == ref and entry.available_at_ns <= cutoff_ns)


def _news_event_available(repository: OpsRepository, event: NewsEventV2, cutoff_ns: int) -> bool:
    """Require an archived immutable event, receipt, and raw payload at the cutoff."""
    try:
        entry = repository.get_artifact(event.event_id)
        raw = repository.get_artifact(event.raw_ref)
        receipt = repository.get_artifact(event.receipt_ref)
    except ValueError:
        return False
    if raw is None or not isinstance(raw.metadata.get("raw_bytes_base64"), str):
        return False
    try:
        raw_bytes = base64.b64decode(raw.metadata["raw_bytes_base64"], validate=True)
    except (ValueError, TypeError):
        return False
    raw_digest = hashlib.sha256(raw_bytes).hexdigest()
    raw_url = raw.metadata.get("source_url")
    expected_raw_ref = sha256_json({"artifact_type": "NewsRawPayloadV2", "source_id": event.source_id,
                                    "source_url": raw_url, "raw_payload_hash": raw_digest})
    return bool(
        entry is not None and entry.artifact_type == "NewsEventV2"
        and entry.content_hash == event.semantic_hash and entry.available_at_ns == event.available_at_ns
        and entry.available_at_ns <= cutoff_ns
        and isinstance(entry.metadata.get("event"), Mapping)
        and sha256_json(entry.metadata["event"]) == event.semantic_hash
        and raw is not None and raw.artifact_type == "NewsRawPayloadV2"
        and raw.content_hash == raw_digest and raw.metadata.get("raw_payload_hash") == raw_digest
        and raw.artifact_ref == expected_raw_ref
        and raw.available_at_ns <= event.received_at_ns
        and raw.metadata.get("source_id") == event.source_id
        and receipt is not None and receipt.artifact_type == "NewsReceiptV2"
        and receipt.content_hash == event.receipt_ref and receipt.available_at_ns <= event.received_at_ns
        and receipt.metadata.get("raw_ref") == event.raw_ref
        and receipt.metadata.get("source_id") == event.source_id
    )


class EventSafetyGateBuilderV2:
    def __init__(self, repository: OpsRepository) -> None:
        self.repository = repository

    def evaluate(self, *, key: InstrumentKeyV2, cutoff_ns: int,
                 coverage: CalendarCoverageV2 | None, scheduled_events: Sequence[ScheduledEventV2],
                 abnormality: AbnormalityEvidenceV2 | None,
                 incidents: Sequence[OperationalIncidentV2]) -> EventSafetyGateV2:
        timestamp(cutoff_ns, field="event_gate.cutoff_ns")
        reasons: list[str] = []
        refs: list[str] = []
        event_match: ScheduledEventV2 | None = None
        if coverage is None or not coverage.complete or coverage.source_qualification != "VERIFIED" \
                or coverage.available_at_ns > cutoff_ns \
                or cutoff_ns - coverage.available_at_ns > CALENDAR_MAX_AGE_NS \
                or cutoff_ns - coverage.observed_at_ns > CALENDAR_MAX_AGE_NS \
                or coverage.covered_from_ns > cutoff_ns - CALENDAR_REQUIRED_BEFORE_NS \
                or coverage.covered_through_ns < cutoff_ns + CALENDAR_REQUIRED_AFTER_NS \
                or not _source_evidence_available(self.repository, coverage.evidence_ref,
                                                  coverage.available_at_ns):
            state = EventGateStateV2.UNKNOWN
            reasons.append("CALENDAR_COVERAGE_MISSING_STALE_OR_INCOMPLETE")
            if coverage is not None:
                if coverage.available_at_ns <= cutoff_ns:
                    _persist_gate_input(self.repository, coverage.content_hash, "CalendarCoverageV2",
                                        coverage.available_at_ns, coverage.to_dict())
                    refs.extend((coverage.evidence_ref, coverage.content_hash))
        else:
            _persist_gate_input(self.repository, coverage.content_hash, "CalendarCoverageV2",
                                coverage.available_at_ns, coverage.to_dict())
            refs.extend((coverage.evidence_ref, coverage.content_hash))
            eligible_events = sorted((event for event in scheduled_events
                                      if event.classification in ("US_CPI", "US_PAYROLL", "FOMC_RATE_DECISION")
                                     and event.available_at_ns <= cutoff_ns),
                                     key=lambda event: (event.scheduled_at_ns, event.event_id))
            schedule_evidence_missing = False
            for scheduled in eligible_events:
                _persist_gate_input(self.repository, scheduled.content_hash, "ScheduledEventV2",
                                    scheduled.available_at_ns, scheduled.to_dict())
                refs.extend((scheduled.content_hash, scheduled.evidence_ref))
                if not _source_evidence_available(self.repository, scheduled.evidence_ref,
                                                  scheduled.available_at_ns):
                    schedule_evidence_missing = True
                    reasons.append("SCHEDULE_EVENT_SOURCE_EVIDENCE_UNAVAILABLE")
            for event in eligible_events:
                start, end = event.scheduled_at_ns - 30 * 60 * 1_000_000_000, event.scheduled_at_ns + 15 * 60 * 1_000_000_000
                if start <= cutoff_ns < end:
                    event_match = event
                    break
            if event_match is not None:
                state = EventGateStateV2.BLOCKED
                reasons.append("SCHEDULED_MACRO_BLACKOUT")
                refs.extend((event_match.content_hash, event_match.evidence_ref))
            else:
                state = EventGateStateV2.UNKNOWN if schedule_evidence_missing else EventGateStateV2.CLEAR
        abnormal_state = AbnormalityStateV2.UNKNOWN
        if (abnormality is None or abnormality.available_at_ns > cutoff_ns
                or cutoff_ns - abnormality.observed_at_ns > ABNORMALITY_MAX_AGE_NS
                or not _source_evidence_available(self.repository, abnormality.evidence_ref,
                                                  abnormality.available_at_ns)):
            state = EventGateStateV2.UNKNOWN
            reasons.append("POST_EVENT_ABNORMALITY_UNKNOWN")
        else:
            abnormal_state = abnormality.state
            _persist_gate_input(self.repository, abnormality.content_hash, "AbnormalityEvidenceV2",
                                abnormality.available_at_ns, abnormality.to_dict())
            refs.extend((abnormality.content_hash, abnormality.evidence_ref))
            if abnormality.state == AbnormalityStateV2.ABNORMAL:
                state = EventGateStateV2.BLOCKED
                reasons.append("POST_EVENT_ABNORMALITY")
            elif abnormality.state == AbnormalityStateV2.UNKNOWN:
                state = EventGateStateV2.BLOCKED
                reasons.append("POST_EVENT_ABNORMALITY_UNKNOWN")
        unresolved_refs: list[str] = []
        latest_by_id: dict[str, OperationalIncidentV2] = {}
        for incident in incidents:
            if incident.available_at_ns > cutoff_ns or not incident.applies_to(key):
                continue
            _persist_gate_input(self.repository, incident.content_hash, "OperationalIncidentV2",
                                incident.available_at_ns, incident.to_dict())
            refs.extend((incident.content_hash, incident.evidence_ref))
            previous = latest_by_id.get(incident.incident_id)
            if previous is None or incident.revision > previous.revision:
                latest_by_id[incident.incident_id] = incident
        for incident in latest_by_id.values():
            if incident.state == IncidentStateV2.OPEN:
                unresolved_refs.append(incident.evidence_ref)
            else:
                prior = [item for item in incidents if item.incident_id == incident.incident_id
                         and item.available_at_ns <= cutoff_ns and item.revision == incident.revision - 1]
                review_resolution = (incident.state == IncidentStateV2.HUMAN_REVIEW_RESOLVED
                                     and incident.resolved_by_ref is not None)
                verified_revision = (bool(prior) and incident.supersedes_id == prior[0].evidence_ref
                                     and incident.resolved_by_ref is not None
                                     and _source_evidence_available(self.repository, incident.resolved_by_ref,
                                                                    incident.available_at_ns)
                                     and _source_evidence_available(self.repository, incident.evidence_ref,
                                                                    incident.available_at_ns))
                if (not (review_resolution or verified_revision)
                        or incident.available_at_ns > cutoff_ns
                        or review_resolution and not _source_evidence_available(
                            self.repository, incident.resolved_by_ref or "", incident.available_at_ns)):
                    unresolved_refs.append(incident.evidence_ref)
        if unresolved_refs:
            state = EventGateStateV2.BLOCKED
            reasons.append("UNRESOLVED_VENUE_OR_ASSET_INCIDENT")
        gate = _gate_artifact(cutoff_ns=cutoff_ns, state=state, reasons=reasons, refs=refs,
                              event_ref=event_match.content_hash if event_match else None,
                              schedule_revision=event_match.schedule_revision if event_match else (coverage.revision if coverage else None),
                              source_id=event_match.source_id if event_match else (coverage.source_id if coverage else None),
                              abnormality=abnormal_state, incidents=unresolved_refs)
        self.repository.register_artifact(ArtifactIndexEntryV2(
            gate.content_hash, EventSafetyGateV2.ARTIFACT_TYPE, gate.content_hash, cutoff_ns, cutoff_ns,
            {"gate": gate.to_dict(), "source_event_refs": list(gate.envelope.input_refs)},
        ))
        return gate


def _persist_gate_input(repository: OpsRepository, ref: str, kind: str, available_at_ns: int,
                        value: Mapping[str, object]) -> None:
    repository.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, available_at_ns,
                                                        available_at_ns, {"evidence": dict(value)}))


@dataclass(frozen=True)
class ReactionSpreadEvidenceV2:
    key: InstrumentKeyV2
    acceptable: bool
    observed_at_ns: int
    available_at_ns: int
    evidence_ref: str
    availability_class: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM

    def __post_init__(self) -> None:
        timestamp(self.observed_at_ns, field="spread.observed_at_ns")
        timestamp(self.available_at_ns, field="spread.available_at_ns")
        object.__setattr__(self, "availability_class", AvailabilityClassV2(self.availability_class))
        if self.available_at_ns < self.observed_at_ns or len(self.evidence_ref) != 64:
            raise ValueError("S7 spread evidence must be causal and referenced")


@dataclass(frozen=True)
class PreEventBetaV2:
    key: InstrumentKeyV2
    btc_proxy: InstrumentKeyV2
    beta: float
    estimated_through_ns: int
    available_at_ns: int
    evidence_ref: str
    availability_class: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM

    def __post_init__(self) -> None:
        timestamp(self.estimated_through_ns, field="pre_event_beta.estimated_through_ns")
        timestamp(self.available_at_ns, field="pre_event_beta.available_at_ns")
        object.__setattr__(self, "availability_class", AvailabilityClassV2(self.availability_class))
        if not math.isfinite(self.beta) or self.available_at_ns < self.estimated_through_ns:
            raise ValueError("pre-event BTC beta must be finite and available after its training window")
        if len(self.evidence_ref) != 64:
            raise ValueError("pre-event beta requires exact evidence reference")


@dataclass(frozen=True)
class MarketReturn5MV2:
    key: InstrumentKeyV2
    close_at_ns: int
    available_at_ns: int
    log_return: float
    evidence_ref: str
    availability_class: AvailabilityClassV2 = AvailabilityClassV2.ACTUAL_SYSTEM

    def __post_init__(self) -> None:
        timestamp(self.close_at_ns, field="market_return.close_at_ns")
        timestamp(self.available_at_ns, field="market_return.available_at_ns")
        object.__setattr__(self, "availability_class", AvailabilityClassV2(self.availability_class))
        if self.available_at_ns < self.close_at_ns or not math.isfinite(self.log_return):
            raise ValueError("BTC 5M proxy return is unavailable or invalid")
        if len(self.evidence_ref) != 64:
            raise ValueError("BTC 5M proxy return requires exact evidence ref")


@dataclass(frozen=True)
class EventResolutionV2:
    event_ref: str
    available_at_ns: int
    evidence_ref: str

    def __post_init__(self) -> None:
        timestamp(self.available_at_ns, field="event_resolution.available_at_ns")
        if len(self.event_ref) != 64 or len(self.evidence_ref) != 64:
            raise ValueError("event resolution requires exact event and review refs")


@dataclass(frozen=True)
class EventReactionArtifactV2:
    envelope: ArtifactEnvelope
    event_ref: str
    key: InstrumentKeyV2
    cutoff_ns: int
    status: str
    reason: str
    abnormal_5m_return: float | None
    abnormal_volume: float | None
    spread_evidence_ref: str | None
    first_bar_ref: str | None
    second_bar_ref: str | None
    side: V2Side | None

    ARTIFACT_TYPE = "EventReactionArtifactV2"

    def __post_init__(self) -> None:
        timestamp(self.cutoff_ns, field="event_reaction.cutoff_ns")
        if self.status not in ("NOT_ESTIMABLE", "NO_CANDIDATE", "HYPOTHESIS", "TRIGGER_CONFIRMED"):
            raise ValueError("invalid directional event reaction state")
        for value in (self.abnormal_5m_return, self.abnormal_volume):
            if value is not None and not math.isfinite(value):
                raise ValueError("event reaction features must be finite")
        object.__setattr__(self, "envelope", seal_envelope(self.envelope, self._body(),
                                                            artifact_type=self.ARTIFACT_TYPE))

    @property
    def content_hash(self) -> str:
        return self.envelope.content_hash

    def _body(self) -> dict[str, object]:
        return {"schema_version": 1, "event_ref": self.event_ref,
                "key": self.key.to_dict(), "cutoff_ns": self.cutoff_ns,
                "status": self.status, "reason": self.reason,
                "abnormal_5m_return": self.abnormal_5m_return,
                "abnormal_volume": self.abnormal_volume,
                "spread_evidence_ref": self.spread_evidence_ref,
                "first_bar_ref": self.first_bar_ref, "second_bar_ref": self.second_bar_ref,
                "side": self.side.value if self.side else None,
                "availability_view": "ACTUAL_SYSTEM",
                "exact_action_status": "NOT_ESTIMABLE_EXACT_ACTION_CONTRACT",
                "missing_contract": "S7_directional_stop_and_horizon_semantics_not_frozen"}

    def to_dict(self) -> dict[str, object]:
        return artifact_wire(self.envelope, self._body(), artifact_type=self.ARTIFACT_TYPE)


def _nearest_rank(values: Sequence[Decimal], p: float) -> Decimal:
    if not values or not 0 < p <= 1:
        raise ValueError("nearest-rank percentile requires finite nonempty data")
    ordered = sorted(values)
    return ordered[math.ceil(p * len(ordered)) - 1]


def _causal_history_bars_v2(bars: Sequence[CausalBarV2], key: InstrumentKeyV2,
                            before_ns: int) -> tuple[CausalBarV2, ...]:
    """Latest actual-system final 5M bars strictly before the event reaction window."""
    by_open: dict[int, CausalBarV2] = {}
    for bar in bars:
        if (bar.interval != BarIntervalV2.M5 or not bar.final
                or bar.instrument_revision != key.contract_revision
                or bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
                or bar.close_at_ns > before_ns or bar.raw.available_at_ns > before_ns
                or not bar.volume.is_finite() or bar.volume < 0):
            continue
        prior = by_open.get(bar.open_at_ns)
        if prior is None or (bar.raw.available_at_ns, bar.raw.record_id) > (prior.raw.available_at_ns, prior.raw.record_id):
            by_open[bar.open_at_ns] = bar
    return tuple(sorted(by_open.values(), key=lambda bar: (bar.close_at_ns, bar.content_hash)))


class S7DirectionalShadowV2:
    """Separate event reaction research path; it never creates CandidateActionV2."""

    def __init__(self, repository: OpsRepository) -> None:
        self.repository = repository

    def evaluate(self, *, event: NewsEventV2, key: InstrumentKeyV2,
                 first_bar: CausalBarV2 | None, second_bar: CausalBarV2 | None,
                 history_bars: Sequence[CausalBarV2], beta_to_btc: PreEventBetaV2 | None,
                 btc_return_5m: MarketReturn5MV2 | None, spread: ReactionSpreadEvidenceV2 | None,
                 source_health: PublicSourceHealthV2 | None,
                 bar_source_health: PublicSourceHealthV2 | None, cutoff_ns: int,
                 resolution: EventResolutionV2 | None = None) -> EventReactionArtifactV2:
        timestamp(cutoff_ns, field="reaction.cutoff_ns")
        refs = [event.event_id]
        refs.extend(bar.content_hash for bar in (first_bar, second_bar) if bar is not None)
        if spread is not None:
            refs.append(spread.evidence_ref)
        if source_health is not None:
            refs.append(source_health.content_hash)
        if bar_source_health is not None:
            refs.append(bar_source_health.content_hash)
        if beta_to_btc is not None:
            refs.append(beta_to_btc.evidence_ref)
        if btc_return_5m is not None:
            refs.append(btc_return_5m.evidence_ref)
        if resolution is not None:
            refs.append(resolution.evidence_ref)
        causal_history = (_causal_history_bars_v2(history_bars, key, first_bar.open_at_ns)
                          if first_bar is not None else ())
        refs.extend(bar.content_hash for bar in causal_history)
        reason = ""
        status = "NOT_ESTIMABLE"
        abnormal = volume_ratio = None
        side: V2Side | None = None
        if event.available_at_ns > (first_bar.open_at_ns if first_bar else cutoff_ns):
            reason = "EVENT_RECEIVED_AFTER_TRIGGER_WINDOW"
        elif not _news_event_available(self.repository, event, first_bar.open_at_ns if first_bar else cutoff_ns):
            reason = "EVENT_ARTIFACT_OR_RECEIPT_EVIDENCE_UNAVAILABLE"
        elif not (
                event.authentication_state in ("AUTHENTICATED", "EXPLICITLY_RESOLVED")
                and event.authentication_ref is not None
                and _source_evidence_available(self.repository, event.authentication_ref, event.available_at_ns)
                and event.source_health_state == PublicSourceStateV2.HEALTHY_CURRENT.value
                and _source_evidence_available(self.repository, event.source_health_ref, event.available_at_ns)
        ) and not (
                resolution is not None and resolution.event_ref == event.event_id
                and resolution.available_at_ns <= (first_bar.open_at_ns if first_bar else cutoff_ns)
                and _source_evidence_available(
                    self.repository, resolution.evidence_ref, first_bar.open_at_ns if first_bar else cutoff_ns
                )
        ):
            reason = "EVENT_SOURCE_NOT_AUTHENTICATED_OR_RESOLVED"
        elif event.asset_mapping_state != AssetMappingStateV2.UNAMBIGUOUS or event.affected_asset_ids != (key.base_asset_id,):
            reason = "ASSET_MAPPING_AMBIGUOUS_OR_MISMATCHED"
        elif source_health is None or source_health.source_id != event.source_id \
                or source_health.state != PublicSourceStateV2.HEALTHY_CURRENT \
                or source_health.available_at_ns > cutoff_ns or source_health.observed_at_ns > cutoff_ns \
                or cutoff_ns - source_health.observed_at_ns > REACTION_SOURCE_HEALTH_MAX_AGE_NS \
                or not _source_evidence_available(self.repository, source_health.content_hash, cutoff_ns):
            reason = "EVENT_SOURCE_HEALTH_UNACCEPTABLE"
        elif (bar_source_health is None or first_bar is None
              or bar_source_health.source_id != first_bar.raw.source_id
              or bar_source_health.state != PublicSourceStateV2.HEALTHY_CURRENT
              or bar_source_health.available_at_ns > cutoff_ns
              or bar_source_health.observed_at_ns > cutoff_ns
              or cutoff_ns - bar_source_health.observed_at_ns > REACTION_SOURCE_HEALTH_MAX_AGE_NS
              or cutoff_ns - bar_source_health.available_at_ns > REACTION_SOURCE_HEALTH_MAX_AGE_NS
              or not _source_evidence_available(self.repository, bar_source_health.content_hash, cutoff_ns)
              or any(bar.raw.source_id != bar_source_health.source_id for bar in causal_history)):
            reason = "MARKET_DATA_SOURCE_HEALTH_UNACCEPTABLE"
        elif first_bar is None or first_bar.interval != BarIntervalV2.M5 or not first_bar.final \
                or first_bar.instrument_revision != key.contract_revision or first_bar.close_at_ns > cutoff_ns \
                or first_bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM \
                or first_bar.raw.available_at_ns > cutoff_ns:
            reason = "FIRST_5M_BAR_UNAVAILABLE_OR_UNCONFIRMED"
        elif (beta_to_btc is None or btc_return_5m is None or beta_to_btc.key != key
              or beta_to_btc.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
              or beta_to_btc.estimated_through_ns > event.received_at_ns
              or beta_to_btc.available_at_ns > event.available_at_ns
              or not _source_evidence_available(self.repository, beta_to_btc.evidence_ref, cutoff_ns)
              or btc_return_5m.key != beta_to_btc.btc_proxy
              or btc_return_5m.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
              or btc_return_5m.close_at_ns != first_bar.close_at_ns
              or btc_return_5m.available_at_ns > first_bar.close_at_ns
              or btc_return_5m.available_at_ns > cutoff_ns
              or not _source_evidence_available(self.repository, btc_return_5m.evidence_ref, cutoff_ns)):
            reason = "PRE_EVENT_BETA_OR_BTC_PROXY_UNAVAILABLE"
        else:
            asset_first_return = math.log(float(first_bar.close) / float(first_bar.open))
            abnormal = asset_first_return - beta_to_btc.beta * btc_return_5m.log_return
            if abnormal == 0 or not math.isfinite(abnormal):
                reason = "FIRST_ABNORMAL_REACTION_ZERO_OR_INVALID"
            elif not causal_history:
                reason = "HISTORICAL_VOLUME_90TH_PERCENTILE_UNAVAILABLE"
            elif second_bar is None:
                status, reason = "HYPOTHESIS", "FIRST_ABNORMAL_5M_REACTION_OBSERVED"
                side = V2Side.LONG if abnormal > 0 else V2Side.SHORT
            elif second_bar.interval != BarIntervalV2.M5 or not second_bar.final \
                    or second_bar.instrument_revision != key.contract_revision \
                    or second_bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM \
                    or second_bar.raw.source_id != bar_source_health.source_id \
                    or second_bar.close_at_ns - first_bar.close_at_ns != REACTION_BAR_NS \
                    or second_bar.close_at_ns > cutoff_ns or second_bar.raw.available_at_ns > cutoff_ns:
                reason = "SECOND_CLOSED_5M_BAR_UNAVAILABLE_OR_NONCONTIGUOUS"
            elif spread is None or spread.key != key or spread.available_at_ns > cutoff_ns \
                    or spread.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM \
                    or spread.observed_at_ns > cutoff_ns \
                    or not _source_evidence_available(self.repository, spread.evidence_ref, cutoff_ns) \
                    or cutoff_ns - spread.observed_at_ns > REACTION_SPREAD_MAX_AGE_NS or not spread.acceptable:
                reason = "SPREAD_ACCEPTABILITY_UNKNOWN_OR_UNACCEPTABLE"
            else:
                threshold = _nearest_rank(tuple(bar.volume for bar in causal_history), 0.90)
                volume_ratio = float(second_bar.volume / threshold) if threshold > 0 else None
                continuation = (second_bar.close > first_bar.close if abnormal > 0 else second_bar.close < first_bar.close)
                if second_bar.volume <= threshold or not continuation:
                    status, reason = "NO_CANDIDATE", "REACTION_CONTINUATION_OR_VOLUME_RULE_FAILED"
                else:
                    status, reason = "TRIGGER_CONFIRMED", "SECOND_5M_CONTINUED_WITH_VOLUME_AND_ACCEPTABLE_SPREAD"
                    side = V2Side.LONG if abnormal > 0 else V2Side.SHORT
        body = {"version": EVENT_REACTION_VERSION, "event_ref": event.event_id,
                "key": key.to_dict(), "cutoff_ns": cutoff_ns, "status": status,
                "reason": reason, "abnormal_5m_return": abnormal,
                "abnormal_volume": volume_ratio,
                "spread_evidence_ref": spread.evidence_ref if spread else None,
                "first_bar_ref": first_bar.content_hash if first_bar else None,
                "second_bar_ref": second_bar.content_hash if second_bar else None,
                "side": side.value if side else None, "availability_view": "ACTUAL_SYSTEM",
                "input_refs": sorted(set(refs))}
        artifact_id = sha256_json(body)
        reaction = EventReactionArtifactV2(
            ArtifactEnvelope(1, artifact_id, cutoff_ns, cutoff_ns, PRODUCER_VERSION, tuple(sorted(set(refs)))),
            event.event_id, key, cutoff_ns, status, reason, abnormal, volume_ratio,
            spread.evidence_ref if spread else None,
            first_bar.content_hash if first_bar else None, second_bar.content_hash if second_bar else None, side,
        )
        self.repository.register_artifact(ArtifactIndexEntryV2(
            reaction.content_hash, reaction.ARTIFACT_TYPE, reaction.content_hash,
            cutoff_ns, cutoff_ns, {"reaction": reaction.to_dict(), "safety_gate_ref": None,
                                    "candidate_action": None, "selector_influence": "ZERO"},
        ))
        if status in ("HYPOTHESIS", "TRIGGER_CONFIRMED"):
            watch_id = sha256_json({"event_ref": event.event_id, "key": key.to_dict(),
                                    "reaction": "S7_DIRECTIONAL_SHADOW", "version": EVENT_REACTION_VERSION})
            prior_watch = self.repository.get_watch(watch_id)
            if prior_watch is None:
                watch = OpportunityWatchV2(
                    watch_id, key, "S7_DIRECTIONAL_REACTION", EVENT_REACTION_VERSION,
                    sha256_json({"policy": EVENT_REACTION_VERSION}), WatchStateV2.DETECTED, 0,
                    cutoff_ns, cutoff_ns, reaction.content_hash,
                    tuple(sorted(set(refs + [reaction.content_hash]))), "BAR_CLOSE_5M",
                    cutoff_ns + REACTION_BAR_NS, cutoff_ns,
                )
                self.repository.create_watch(watch)
                self.repository.transition_watch(
                    watch_id, expected_state_version=0,
                    event_id=sha256_json({"watch": watch_id, "event": reaction.content_hash}),
                    event_at_ns=cutoff_ns, transition_at_ns=cutoff_ns,
                    target_state=WatchStateV2.WAITING_FOR_EVENT,
                    outbox_id=sha256_json({"watch": watch_id, "outbox": reaction.content_hash}),
                )
            if status == "TRIGGER_CONFIRMED":
                current_watch = self.repository.get_watch(watch_id)
                if current_watch is not None and current_watch.state == WatchStateV2.WAITING_FOR_EVENT:
                    ready = self.repository.transition_watch(
                        watch_id, expected_state_version=current_watch.state_version,
                        event_id=sha256_json({"watch": watch_id, "reaction": reaction.content_hash,
                                              "state": "READY_FOR_RECHECK"}),
                        event_at_ns=cutoff_ns, transition_at_ns=cutoff_ns,
                        target_state=WatchStateV2.READY_FOR_RECHECK,
                        outbox_id=sha256_json({"watch": watch_id, "trigger": reaction.content_hash,
                                               "outbox": "READY_FOR_RECHECK"}),
                    ).watch
                    self.repository.transition_watch(
                        watch_id, expected_state_version=ready.state_version,
                        event_id=sha256_json({"watch": watch_id, "reaction": reaction.content_hash,
                                              "state": "CONFIRMED"}),
                        event_at_ns=cutoff_ns, transition_at_ns=cutoff_ns,
                        target_state=WatchStateV2.CONFIRMED,
                        outbox_id=sha256_json({"watch": watch_id, "trigger": reaction.content_hash,
                                               "outbox": "CONFIRMED"}),
                    )
        return reaction
