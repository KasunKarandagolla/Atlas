"""Versioned OpenAI price schedule and worst-case spend reservation math."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from pathlib import Path

from atlas.v2._serialization import sha256_json

_MILLION = Decimal(1_000_000)


@dataclass(frozen=True)
class ProviderPriceScheduleV1:
    version: str
    provider: str
    requested_model_id: str
    endpoint: str
    effective_date: str
    pricing_source: str
    input_usd_per_million: Decimal
    cached_input_usd_per_million: Decimal
    cache_write_usd_per_million: Decimal
    output_usd_per_million: Decimal
    maximum_input_tokens: int
    maximum_output_tokens: int
    maximum_input_context_tokens: int
    maximum_model_calls_per_job: int
    maximum_read_tool_calls_per_job: int
    maximum_concurrent_research_jobs: int
    maximum_cost_per_call_usd: Decimal
    maximum_cost_per_job_usd: Decimal
    maximum_daily_cost_usd: Decimal

    @classmethod
    def load(cls, path: str | Path) -> ProviderPriceScheduleV1:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        expected = {"version", "provider", "requested_model_id", "endpoint", "effective_date", "pricing_source",
            "prices_usd_per_million_tokens", "maximum_input_tokens", "maximum_output_tokens",
            "maximum_input_context_tokens", "maximum_model_calls_per_job", "maximum_read_tool_calls_per_job",
            "maximum_concurrent_research_jobs", "maximum_cost_per_call_usd", "maximum_cost_per_job_usd",
            "maximum_daily_cost_usd"}
        if not isinstance(data, dict) or set(data) != expected:
            raise ValueError("provider price schedule has an invalid schema")
        rates = data["prices_usd_per_million_tokens"]
        if not isinstance(rates, dict) or set(rates) != {"input", "cached_input", "cache_write", "output"}:
            raise ValueError("provider price schedule rates are invalid")
        decimal = lambda value: Decimal(value)  # noqa: E731
        schedule = cls(data["version"], data["provider"], data["requested_model_id"], data["endpoint"],
            data["effective_date"], data["pricing_source"], decimal(rates["input"]),
            decimal(rates["cached_input"]), decimal(rates["cache_write"]), decimal(rates["output"]),
            data["maximum_input_tokens"], data["maximum_output_tokens"], data["maximum_input_context_tokens"],
            data["maximum_model_calls_per_job"], data["maximum_read_tool_calls_per_job"],
            data["maximum_concurrent_research_jobs"], decimal(data["maximum_cost_per_call_usd"]),
            decimal(data["maximum_cost_per_job_usd"]), decimal(data["maximum_daily_cost_usd"]))
        if (schedule.provider != "openai" or schedule.requested_model_id != "gpt-6-astra"
                or schedule.endpoint != "https://api.openai.com/v1/responses"):
            raise ValueError("initial price schedule provider/model/endpoint is not authorized")
        if (schedule.maximum_model_calls_per_job != 3 or schedule.maximum_read_tool_calls_per_job != 8
                or schedule.maximum_concurrent_research_jobs != 1):
            raise ValueError("initial fixed job and tool limits are invalid")
        if schedule.worst_case_call_usd() > schedule.maximum_cost_per_call_usd:
            raise ValueError("per-call cost cap does not cover worst-case uncached/cache-write input")
        if schedule.worst_case_call_usd() * schedule.maximum_model_calls_per_job > schedule.maximum_cost_per_job_usd:
            raise ValueError("job cost cap does not cover all allowed model calls")
        for value in rates.values():
            if decimal(value) < 0:
                raise ValueError("provider prices must be non-negative")
        return schedule

    @property
    def content_hash(self) -> str:
        body = {name: str(value) if isinstance(value, Decimal) else value for name, value in self.__dict__.items()}
        return sha256_json(body)

    def worst_case_call_usd(self, *, input_tokens: int | None = None, output_tokens: int | None = None) -> Decimal:
        inputs = self.maximum_input_tokens if input_tokens is None else input_tokens
        outputs = self.maximum_output_tokens if output_tokens is None else output_tokens
        if (type(inputs) is not int or type(outputs) is not int or inputs < 0 or outputs < 0
                or inputs > self.maximum_input_tokens or outputs > self.maximum_output_tokens
                or inputs > self.maximum_input_context_tokens):
            raise ValueError("inference token request is outside the versioned price schedule")
        # Input may be billed as cache write; that is more expensive than ordinary input.
        amount = (Decimal(inputs) * max(self.input_usd_per_million, self.cache_write_usd_per_million)
                  + Decimal(outputs) * self.output_usd_per_million) / _MILLION
        return amount.quantize(Decimal("0.000001"), rounding=ROUND_CEILING)

    def validate_request_budgets(self, *, calls: int, input_tokens: int, output_tokens: int,
                                 job_usd: Decimal, daily_usd: Decimal) -> Decimal:
        if (calls < 1 or calls > self.maximum_model_calls_per_job
                or input_tokens > self.maximum_input_tokens or output_tokens > self.maximum_output_tokens
                or job_usd <= 0 or job_usd > self.maximum_cost_per_job_usd
                or daily_usd <= 0 or daily_usd > self.maximum_daily_cost_usd):
            raise ValueError("request exceeds the current versioned inference budget")
        per_call = self.worst_case_call_usd(input_tokens=input_tokens, output_tokens=output_tokens)
        if per_call * calls > job_usd:
            raise ValueError("job ceiling cannot reserve every configured model call")
        return per_call


@dataclass(frozen=True)
class DeepSeekPriceScheduleV1:
    """Provider-neutral cache classes for the fixed DeepSeek V4.1 Flash alias."""

    version: str
    provider: str
    requested_model_id: str
    base_url: str
    endpoint_path: str
    effective_date: str
    pricing_source: str
    cache_hit_input_usd_per_million: Decimal
    cache_miss_input_usd_per_million: Decimal
    output_usd_per_million: Decimal
    maximum_input_tokens: int
    maximum_output_tokens: int
    maximum_input_context_tokens: int
    maximum_model_calls_per_job: int
    maximum_read_tool_calls_per_job: int
    maximum_concurrent_research_jobs: int
    maximum_cost_per_call_usd: Decimal
    maximum_cost_per_job_usd: Decimal
    maximum_daily_cost_usd: Decimal

    def __post_init__(self) -> None:
        fixed = (self.version == "DEEPSEEK_V41_FLASH_PEAK_2026_09_V1"
            and self.provider == "deepseek" and self.requested_model_id == "deepseek-flash"
            and self.base_url == "https://api.deepseek.com" and self.endpoint_path == "/responses"
            and self.effective_date == "2026-09-10")
        if not fixed:
            raise ValueError("DeepSeek pricing identity or endpoint is outside the reviewed schedule")
        if self.pricing_source != "https://api-docs.deepseek.com/quick_start/pricing/":
            raise ValueError("DeepSeek pricing source must be the official model-pricing page")
        if (self.cache_hit_input_usd_per_million != Decimal("0.006")
                or self.cache_miss_input_usd_per_million != Decimal("0.30")
                or self.output_usd_per_million != Decimal("1.20")):
            raise ValueError("DeepSeek prices must match the approved conservative peak rates")
        if (self.maximum_input_tokens != 12_000 or self.maximum_input_context_tokens != 12_000
                or self.maximum_output_tokens != 4_000 or self.maximum_model_calls_per_job != 3
                or self.maximum_read_tool_calls_per_job != 8 or self.maximum_concurrent_research_jobs != 1):
            raise ValueError("DeepSeek job/token limits differ from the approved amendment")
        if (self.maximum_cost_per_call_usd != Decimal("0.008400")
                or self.maximum_cost_per_job_usd != Decimal("0.025200")
                or self.maximum_daily_cost_usd != Decimal("5.25")):
            raise ValueError("DeepSeek spend ceilings do not cover the conservative token limits")

    @classmethod
    def load(cls, path: str | Path) -> DeepSeekPriceScheduleV1:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        expected = {"version", "provider", "requested_model_id", "base_url", "endpoint_path",
            "effective_date", "pricing_source", "prices_usd_per_million_tokens", "maximum_input_tokens",
            "maximum_output_tokens", "maximum_input_context_tokens", "maximum_model_calls_per_job",
            "maximum_read_tool_calls_per_job", "maximum_concurrent_research_jobs",
            "maximum_cost_per_call_usd", "maximum_cost_per_job_usd", "maximum_daily_cost_usd"}
        if not isinstance(data, dict) or set(data) != expected:
            raise ValueError("DeepSeek price schedule has an invalid schema")
        rates = data["prices_usd_per_million_tokens"]
        if not isinstance(rates, dict) or set(rates) != {"cache_hit_input", "cache_miss_input", "output"}:
            raise ValueError("DeepSeek price schedule rates are invalid")
        decimal = lambda value: Decimal(value)  # noqa: E731
        schedule = cls(data["version"], data["provider"], data["requested_model_id"], data["base_url"],
            data["endpoint_path"], data["effective_date"], data["pricing_source"],
            decimal(rates["cache_hit_input"]), decimal(rates["cache_miss_input"]), decimal(rates["output"]),
            data["maximum_input_tokens"], data["maximum_output_tokens"], data["maximum_input_context_tokens"],
            data["maximum_model_calls_per_job"], data["maximum_read_tool_calls_per_job"],
            data["maximum_concurrent_research_jobs"], decimal(data["maximum_cost_per_call_usd"]),
            decimal(data["maximum_cost_per_job_usd"]), decimal(data["maximum_daily_cost_usd"]))
        schedule.worst_case_call_usd()
        return schedule

    @property
    def endpoint(self) -> str:
        return self.base_url + self.endpoint_path

    @property
    def content_hash(self) -> str:
        body = {name: str(value) if isinstance(value, Decimal) else value for name, value in self.__dict__.items()}
        return sha256_json(body)

    def worst_case_call_usd(self, *, input_tokens: int | None = None,
                            output_tokens: int | None = None) -> Decimal:
        inputs = self.maximum_input_tokens if input_tokens is None else input_tokens
        outputs = self.maximum_output_tokens if output_tokens is None else output_tokens
        if (type(inputs) is not int or type(outputs) is not int or inputs < 0 or outputs < 0
                or inputs > self.maximum_input_tokens or outputs > self.maximum_output_tokens
                or inputs > self.maximum_input_context_tokens):
            raise ValueError("DeepSeek inference token request is outside the reviewed schedule")
        # ATLAS reserves cache-miss input pricing even when the provider later bills a hit.
        amount = (Decimal(inputs) * self.cache_miss_input_usd_per_million
                  + Decimal(outputs) * self.output_usd_per_million) / _MILLION
        return amount.quantize(Decimal("0.000001"), rounding=ROUND_CEILING)

    def validate_request_budgets(self, *, calls: int, input_tokens: int, output_tokens: int,
                                 job_usd: Decimal, daily_usd: Decimal) -> Decimal:
        if (type(calls) is not int or calls < 1 or calls > self.maximum_model_calls_per_job
                or input_tokens > self.maximum_input_tokens or output_tokens > self.maximum_output_tokens
                or job_usd <= 0 or job_usd > self.maximum_cost_per_job_usd
                or daily_usd <= 0 or daily_usd > self.maximum_daily_cost_usd):
            raise ValueError("request exceeds the DeepSeek versioned inference budget")
        per_call = self.worst_case_call_usd(input_tokens=input_tokens, output_tokens=output_tokens)
        if per_call * calls > job_usd:
            raise ValueError("job ceiling cannot reserve every configured DeepSeek model call")
        return per_call
