"""Long-lived safe runtime skeleton for atlas-crypto-live (Phase 1/2).

This process:
- Acquires writer ownership once and holds it for process lifetime
- Opens and retains the SQLite journal
- Enters RECOVERING and periodically reevaluates health/recovery prerequisites
- Periodically publishes sanitized status
- Remains new-risk-disabled while qualification is incomplete
- Handles SIGINT/SIGTERM / Ctrl-C gracefully
- Closes journal and releases local writer lock on clean shutdown

No order submission. No test orders. New risk remains disabled.
"""

from __future__ import annotations

import signal
import sys
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path

from atlas.domain.capability import CapabilityContract
from atlas.domain.time import now_ns
from atlas.persistence.sqlite import SQLiteJournal

from .connectivity import PrivateVerification, PublicVenueHealth
from .coordinator import BootResult, RiskPolicyDecision, boot
from .prerequisites import IdentityExpectation, ObservedAccountState
from .status import RuntimeStatus, publish_status
from .writer_lock import WriterLock, WriterOwnership


@dataclass(frozen=True)
class SafeRuntimeConfig:
    journal_path: str
    lock_path: str
    status_path: str
    capability_hash: str
    all_qualified: bool
    assisted_enabled: bool
    identity_expected: IdentityExpectation
    identity_observed: ObservedAccountState
    public_health: PublicVenueHealth
    private_verification: PrivateVerification
    max_public_staleness_ns: int
    clock_uncertainty_ns: int = 0
    tick_interval_ns: int = 10_000_000_000  # 10 seconds default
    capability_contract: CapabilityContract | None = None
    risk_policy: RiskPolicyDecision | None = None


class SafeRuntime:
    """Long-lived safe runtime process skeleton."""

    def __init__(self, config: SafeRuntimeConfig) -> None:
        self._config = config
        self._writer: WriterLock | None = None
        self._ownership: WriterOwnership | None = None
        self._journal: SQLiteJournal | None = None
        self._runtime_instance_id = uuid.uuid4().hex
        self._journal_path = Path(config.journal_path)
        self._status_path = Path(config.status_path)
        self._shutdown_event = threading.Event()
        self._tick_count = 0
        self._last_boot_result: BootResult | None = None
        self._lock = threading.Lock()

        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum: int, frame) -> None:
        self._shutdown_event.set()

    def start(self) -> BootResult:
        """Acquire writer, open journal, run initial boot sequence."""
        if self._writer is not None:
            raise RuntimeError("SafeRuntime already started")

        self._writer = WriterLock(self._config.lock_path)
        try:
            self._ownership = self._writer.acquire()
            self._journal = SQLiteJournal(self._journal_path)
            result = self._run_boot()
            self._last_boot_result = result
            self._publish_status(result)
            return result
        except Exception:
            # Startup is transactional from the process-boundary perspective:
            # release anything acquired before re-raising the real failure.
            if self._journal is not None:
                try:
                    self._journal.close()
                except Exception:
                    pass
                self._journal = None
            if self._writer is not None:
                try:
                    self._writer.release()
                except Exception:
                    pass
                self._writer = None
                self._ownership = None
            raise

    def _run_boot(self) -> BootResult:
        """Run a single boot/health evaluation cycle."""
        return boot(
            journal_path=self._config.journal_path,
            lock_path=self._config.lock_path,
            capability_hash=self._config.capability_hash,
            all_qualified=self._config.all_qualified,
            assisted_enabled=self._config.assisted_enabled,
            identity_expected=self._config.identity_expected,
            identity_observed=self._config.identity_observed,
            public_health=self._config.public_health,
            private_verification=self._config.private_verification,
            now_ns=now_ns(),
            max_public_staleness_ns=self._config.max_public_staleness_ns,
            clock_uncertainty_ns=self._config.clock_uncertainty_ns,
            writer=self._writer,
            journal=self._journal,
            capability_contract=self._config.capability_contract,
            risk_policy=self._config.risk_policy,
            runtime_instance_id=self._runtime_instance_id,
        )

    def _publish_status(self, result: BootResult) -> None:
        """Publish sanitized runtime status."""
        from atlas.domain.time import now_ns
        status = RuntimeStatus(
            runtime_state=result.state.value,
            writer_epoch=result.certificate.writer_epoch,
            journal_healthy=True,
            reconciliation_health=result.certificate.reconciliation_health.value,
            unresolved_intents=result.unresolved_intents,
            unknown_commands=result.unknown_commands,
            unresolved_commands=result.unresolved_commands,
            capability_hash=self._config.capability_hash,
            all_qualified=self._config.all_qualified,
            assisted_enabled=self._config.assisted_enabled,
            generated_at_ns=now_ns(),
            runtime_instance_id=result.certificate.recovery_run_id,
            writer_id=result.certificate.writer_id,
        )
        publish_status(self._status_path, status)

    def tick(self) -> BootResult:
        """Run one health reevaluation cycle. Must be called periodically."""
        if self._writer is None:
            raise RuntimeError("SafeRuntime not started")

        with self._lock:
            result = self._run_boot()
            self._last_boot_result = result
            self._tick_count += 1
            self._publish_status(result)
            return result

    def status(self) -> BootResult | None:
        """Return the last boot result (thread-safe)."""
        with self._lock:
            return self._last_boot_result

    def shutdown(self) -> None:
        """Clean shutdown: close journal, release writer lock."""
        self._shutdown_event.set()
        if self._journal is not None:
            try:
                self._journal.close()
            except Exception:
                pass
            self._journal = None
        if self._writer is not None:
            try:
                self._writer.release()
            except Exception:
                pass
            self._writer = None
            self._ownership = None

    @property
    def journal(self) -> SQLiteJournal | None:
        """The process-lifetime journal, retained for bounded-runtime tests."""
        return self._journal

    @property
    def runtime_instance_id(self) -> str:
        return self._runtime_instance_id

    def run_forever(self) -> None:
        """Run the main loop until shutdown signal received."""
        if self._writer is None:
            self.start()

        tick_interval_s = self._config.tick_interval_ns / 1_000_000_000

        while not self._shutdown_event.is_set():
            try:
                self.tick()
            except Exception as exc:
                # Log but continue - fail closed behavior
                print(f"SafeRuntime tick error: {exc}", file=sys.stderr)

            # Wait for next tick or shutdown
            self._shutdown_event.wait(timeout=tick_interval_s)

        self.shutdown()

    @property
    def writer_held(self) -> bool:
        """Check if writer lock is currently held."""
        return self._writer is not None and self._writer.ownership is not None

    @property
    def tick_count(self) -> int:
        """Return number of ticks completed."""
        return self._tick_count

    @property
    def new_risk_allowed(self) -> bool:
        """Check if new risk is currently allowed (always False in Phase 1/2)."""
        with self._lock:
            if self._last_boot_result is None:
                return False
            return self._last_boot_result.new_risk_allowed
