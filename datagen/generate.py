#!/usr/bin/env python3
"""
Synthetic Customer 360 Data Generator

Synthetic Customer 360 data generator with 7 realism features:
  1. Zipf-distributed customer IDs (power-law activity)
  2. Weighted interaction types (browse-heavy, realistic conversion rate)
  3. Conditional nulls (event-type-dependent columns)
  4. Dirty data corruption patterns (email, phone, city, state)
  5. Channel-device coherence (null device for offline channels)
  6. Log-normal transaction amounts (long-tailed distribution)
  7. Customer-consistent loyalty (same customer = same tier)

Generates Parquet files directly to S3 Bronze bucket for the medallion pipeline.
Memory-efficient design allows scaling from 1TB to 100TB+ without OOM issues.

Usage:
    python generate.py --target-tb 1
    python generate.py --target-tb 100 --workers 8
    python generate.py --target-tb 100 --node-id 0 --total-nodes 4  # Multi-node
"""

import argparse
import io
import json
import multiprocessing
import os
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from multiprocessing import Process
from multiprocessing import Queue as MPQueue
from typing import Protocol, runtime_checkable

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig
from tqdm import tqdm

# Disable tqdm's TMonitor thread. It runs as a non-daemon background
# thread that pings every 10s to re-render if a bar hasn't updated; on
# interpreter shutdown Python waits for it, and it doesn't observe
# SIGTERM/SIGINT reliably. A hung TMonitor was what turned a clean
# continuous-mode duration-expiry run into a 30s pod-grace-period
# SIGKILL (main printed "Complete!" then wedged; kubelet killed).
# monitor_interval=0 stops the thread from being created at all.
tqdm.monitor_interval = 0

# =============================================================================
# Continuous Mode Constants
# =============================================================================

NUM_GENERATORS = 8  # Number of generator processes
NUM_UPLOADERS = 2  # Number of uploader threads
QUEUE_DEPTH = 8  # Max items in upload queue (backpressure)

# Shutdown coordination for graceful SIGTERM/SIGINT handling.
#
# Two events are needed because the main process (Python threads) and the
# generator workers (multiprocessing.Process children) live in different
# memory spaces:
#   - SHUTDOWN_REQUESTED (threading.Event) -- polled by the main run_continuous
#     loop and by uploader threads.
#   - MP_SHUTDOWN_EVENT (multiprocessing.Event) -- shared with generator
#     child processes; they poll it out-of-band from file_queue.
#
# Both are created at module load, BEFORE any handler is installed and
# before any child fork. A prior design deferred mp_event creation into
# run_continuous, which opened a millisecond-scale race: SIGTERM landing
# between the initial main() install (mp_event=None) and the deferred
# creation set SHUTDOWN_REQUESTED only. Generators (which don't share the
# parent's threading memory) then never saw shutdown, drained the full
# Phase 1 pre-load, and blew past terminationGracePeriodSeconds. Now the
# event exists at import time so every install path passes the real
# object; children fork with the semaphore fd already inherited.
#
# Why not just use poison pills on file_queue? Phase 1 pre-loads file_queue
# with EVERY remaining file_id (can be thousands at large --target-tb).
# Pills appended by the SIGTERM handler sit at the END of that queue, so
# generators would have to work through all pending file_ids before seeing
# them -- shutdown takes minutes instead of seconds. The MP event
# bypasses the queue entirely: workers check it between file_queue.get()s
# and inside their put()/upload_queue.put() paths so they exit within one
# poll interval regardless of queue depth.
#
# Without this, K8s pod deletion would kill the process mid-upload,
# leaking S3 multipart uploads to FlashBlade (CLAUDE.md gotcha 2:
# multipart ghosts) and losing the in-flight batch's checkpoint update.
SHUTDOWN_REQUESTED = threading.Event()
MP_SHUTDOWN_EVENT = multiprocessing.Event()


def _install_shutdown_handlers() -> None:
    """Install SIGTERM/SIGINT handlers on the CURRENT process only.

    Both events (SHUTDOWN_REQUESTED and MP_SHUTDOWN_EVENT) are module-level
    singletons created at import time -- there's no race window between
    install and event creation. The handler flips both so main-process
    threads and forked child generators wake within one poll interval.

    Multiprocessing.Process children fork/spawn with this handler
    attached, but the generator worker resets to SIG_IGN as its first
    action so shutdown stays driven by the parent through
    MP_SHUTDOWN_EVENT. Without SIG_IGN in children, SIGTERM to the pod's
    process group would kill workers immediately, dropping in-flight
    files and skipping the None poison pill on upload_queue.

    Idempotent. In non-main threads (e.g. under pytest), Python refuses
    to set a signal handler; we swallow the ValueError.
    """
    def _handler(signum, frame):  # noqa: ARG001
        SHUTDOWN_REQUESTED.set()
        try:
            MP_SHUTDOWN_EVENT.set()
        except Exception:
            # An mp.Event backed by a torn-down semaphore (interpreter
            # shutdown) can raise; keep the threading flag set.
            pass

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handler)
        except ValueError:
            # "signal only works in main thread of the main interpreter"
            pass

# =============================================================================
# Constants matching Bronze schema
# =============================================================================

INTERACTION_TYPES = ["purchase", "browse", "support", "login", "abandoned_cart"]
# v2: Weighted distribution (browse-heavy, realistic 18% purchase rate)
INTERACTION_WEIGHTS = [0.18, 0.35, 0.12, 0.20, 0.15]

PRODUCT_CATEGORIES = ["electronics", "clothing", "home_garden", "books", "sports"]
CURRENCIES = ["USD", "EUR", "GBP", "CAD"]
CHANNELS = ["web", "mobile_app", "store", "call_center", "social_media"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
BROWSERS = ["chrome", "safari", "firefox", "edge"]
LOYALTY_TIERS = ["bronze", "silver", "gold"]
EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "hotmail.com"]
OPERATING_SYSTEMS = ["Windows NT 10.0", "macOS 14.0", "Linux", "iOS 17", "Android 14"]

# Data quality flags (used by Silver for filtering)
DATA_QUALITY_FLAGS = ["clean", "duplicate_suspected", "incomplete_data", "format_inconsistent"]
DATA_QUALITY_WEIGHTS = [0.92, 0.02, 0.03, 0.03]

# Data source indicators
DATA_SOURCES = ["primary_system", "legacy_import", "manual_entry", "third_party_api"]
DATA_SOURCE_WEIGHTS = [0.70, 0.15, 0.10, 0.05]

# Support ticket categories
ISSUE_CATEGORIES = ["billing", "technical", "general_inquiry"]

# UTM sources and mediums for marketing attribution
UTM_SOURCES = ["google", "facebook", "email", "direct"]
UTM_MEDIUMS = ["cpc", "organic", "referral"]

# Intentionally inconsistent city/state mappings for Silver layer cleaning
CITIES = [
    ("New York", "NY"),
    ("NYC", "New York"),
    ("New York City", "NY"),
    ("Los Angeles", "CA"),
    ("LA", "California"),
    ("Los Angeles", "California"),
    ("Chicago", "IL"),
    ("Chicago", "Illinois"),
    ("Houston", "TX"),
    ("Houston", "Texas"),
    ("Phoenix", "AZ"),
    ("Phoenix", "Arizona"),
    ("Philadelphia", "PA"),
    ("Philly", "Pennsylvania"),
    ("San Antonio", "TX"),
    ("San Antonio", "Texas"),
    ("San Diego", "CA"),
    ("San Diego", "California"),
    ("Dallas", "TX"),
    ("Dallas", "Texas"),
    ("San Jose", "CA"),
    ("San Jose", "California"),
]

# v2: Dirty data corruption variants
DIRTY_CITY_VARIANTS = {
    "Chicago": "Chicgao",
    "New York": "Newyork",
    "New York City": "Newyork City",
    "NYC": "Nyc",
    "Phoenix": "Pheonix",
    "Los Angeles": "Los Angelas",
    "LA": "La",
    "Houston": "Huston",
    "Philadelphia": "Philidelphia",
    "Philly": "Phily",
}

DIRTY_STATE_VARIANTS = {
    "CA": ["california", "CALIFORNIA", "Calif.", "Ca"],
    "NY": ["new york", "NEW YORK", "N.Y.", "Ny"],
    "New York": ["new york", "NEW YORK", "ny", "N.Y."],
    "IL": ["illinois", "ILLINOIS", "Ill.", "Il"],
    "Illinois": ["IL", "il", "Ill.", "ILLINOIS"],
    "TX": ["texas", "TEXAS", "Tex.", "Tx"],
    "Texas": ["TX", "tx", "Tex.", "TEXAS"],
    "AZ": ["arizona", "ARIZONA", "Ariz.", "Az"],
    "Arizona": ["AZ", "az", "Ariz.", "ARIZONA"],
    "PA": ["pennsylvania", "PENNSYLVANIA", "Penn.", "Pa"],
    "Pennsylvania": ["PA", "pa", "Penn.", "PENNSYLVANIA"],
    "California": ["CA", "ca", "Calif.", "CALIFORNIA"],
}


# =============================================================================
# Configuration
# =============================================================================


class Config:
    """Configuration for data generation."""

    def __init__(
        self,
        target_tb: float = 1.0,
        workers: int = 4,
        seed: int = 42,
        node_id: int = 0,
        total_nodes: int = 1,
        payload_kb: int = 2,
        file_size_mb: int = 512,
        bucket: str = "lakebench-bronze",
        prefix: str = "customer/interactions",
        checkpoint_file: str = ".datagen_checkpoint.json",
        resume: bool = False,
        timestamp_start: str = "2024-01-01",
        timestamp_end: str = "2025-12-31",
        customer_id_max: int = 500000,
        duplicate_email_pct: float = 0.10,
        dirty_ratio: float = 0.08,
        duration_seconds: int | None = None,
        schema_name: str = "customer360",
    ):
        self.target_tb = target_tb
        self.workers = workers
        self.seed = seed
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.payload_kb = payload_kb
        self.file_size_mb = file_size_mb
        self.bucket = bucket
        self.prefix = prefix
        self.checkpoint_file = checkpoint_file
        self.resume = resume
        self.timestamp_start = datetime.fromisoformat(timestamp_start)
        self.timestamp_end = datetime.fromisoformat(timestamp_end)
        self.customer_id_max = customer_id_max
        self.duplicate_email_pct = duplicate_email_pct
        self.dirty_ratio = dirty_ratio
        self.duration_seconds = duration_seconds
        self.schema_name = schema_name

        # Calculated values
        self.target_bytes = int(target_tb * 1024 * 1024 * 1024 * 1024)
        self.file_size_bytes = file_size_mb * 1024 * 1024
        self.total_files = max(1, int(self.target_bytes / self.file_size_bytes))

        # FlashBlade single-PUT ceiling is 5 GiB; boto3's `put_object`
        # (the code path we chose over `upload_fileobj` to eliminate MPU
        # ghosts) fails a single request past that. Refuse >5000 MiB at
        # config load so an operator who set `--file-size-mb 8192`
        # doesn't discover it via `EntityTooLarge` on every uploaded
        # file. The 5000 (vs 5120) leaves headroom for parquet padding
        # and codec overhead.
        if file_size_mb > 5000:
            raise ValueError(
                f"--file-size-mb={file_size_mb} exceeds the 5000 MiB single-PUT "
                "cap enforced by boto3 put_object against FlashBlade. Split "
                "into smaller files or use a multipart-capable path."
            )

        # Estimate rows per file to achieve target compressed file size.
        # Schema-specific -- prior single value (4200 = C360 with 2KB payload)
        # was 20-25x too high for the financial schema. Measured empirically
        # Bytes/row depends on both schema AND codec. The row layout is fixed
        # per schema but compression ratio is not: ZSTD-1 sees across the
        # whole row group and cracks the hex payload (customer360) or the
        # bounded-vocabulary financial columns much better than SNAPPY's
        # 32 KB window. LZ4 barely compresses either schema (payload for
        # customer360 is random hex, financial columns are already dense),
        # so its bytes/row lands within a few percent of uncompressed.
        # Measured 2026-09-18 via generate_file_data(0) at 50k rows per
        # codec, seed=42 (scratchpad/measure_bpr.py):
        #   customer360: snappy 4332, zstd1 2233, lz4 4356, none 4399
        #   financial:   snappy  199, zstd1  133, lz4  207, none  292
        # Miscalibration (using the SNAPPY constant with the ZSTD default)
        # produces files ~1/2 the target size -- the small-file problem this
        # table exists to prevent, silently reintroduced. When DG_COMPRESSION
        # defaults changed 2026-09-18, this table had to move with it.
        codec, _ = _parquet_compression()
        _BYTES_PER_ROW = {
            ("customer360", "snappy"): 4332,
            ("customer360", "zstd"): 2233,
            ("customer360", "lz4"): 4356,
            ("customer360", "none"): 4399,
            ("financial", "snappy"): 199,
            ("financial", "zstd"): 133,
            ("financial", "lz4"): 207,
            ("financial", "none"): 292,
        }
        compressed_bytes_per_row = _BYTES_PER_ROW.get(
            (self.schema_name, codec),
            _BYTES_PER_ROW.get((self.schema_name, "zstd"), 2200),
        )
        self.rows_per_file = max(1000, self.file_size_bytes // compressed_bytes_per_row)

        # S3 configuration
        self.s3_endpoint = os.environ.get("S3_ENDPOINT", os.environ.get("AWS_ENDPOINT_URL", ""))
        self.s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        self.s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        self.s3_region = os.environ.get("AWS_REGION", "us-east-1")


def get_s3_client(config: Config):
    """Create S3 client with proper configuration."""
    boto_config = BotoConfig(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        retries={"max_attempts": 3, "mode": "standard"},
    )

    kwargs = {
        "config": boto_config,
        "region_name": config.s3_region,
    }

    if config.s3_access_key and config.s3_secret_key:
        kwargs["aws_access_key_id"] = config.s3_access_key
        kwargs["aws_secret_access_key"] = config.s3_secret_key

    if config.s3_endpoint:
        kwargs["endpoint_url"] = config.s3_endpoint
        # TLS verification: CA cert path > verify_ssl toggle > boto3 default
        ca_cert = os.environ.get("S3_CA_CERT", "")
        verify_ssl = os.environ.get("S3_VERIFY_SSL", "true").lower() != "false"
        if ca_cert:
            kwargs["verify"] = ca_cert  # Path to PEM CA bundle
        elif not verify_ssl:
            kwargs["verify"] = False

    return boto3.client("s3", **kwargs)


# =============================================================================
# Loyalty Lookup (v2 feature #7: customer-consistent loyalty)
# =============================================================================


def build_loyalty_lookup(seed: int, customer_id_max: int):
    """Pre-generate loyalty arrays indexed by customer_id.

    Same customer always maps to same (is_member, tier) tuple.
    Overall ratio: 60% members, with 70/20/10 bronze/silver/gold.
    """
    lookup_rng = np.random.default_rng(seed=seed)
    member_mask = lookup_rng.random(customer_id_max + 1) < 0.6
    tier_rolls = lookup_rng.random(customer_id_max + 1)
    # 0=bronze, 1=silver, 2=gold
    tier_lookup = np.where(tier_rolls < 0.7, 0, np.where(tier_rolls < 0.9, 1, 2))
    return member_mask, tier_lookup


# =============================================================================
# Data Generation Functions
# =============================================================================


def generate_uuids(rows: int, rng: np.random.Generator) -> list:
    """Generate UUID v4 strings."""
    return [str(uuid.UUID(bytes=rng.bytes(16), version=4)) for _ in range(rows)]


def generate_timestamps(rows: int, rng: np.random.Generator, config: Config) -> list:
    """Generate random timestamps within the configured range.

    Uses `tz=timezone.utc` so the returned datetimes are UTC on every host;
    the naive `datetime.fromtimestamp(ts)` leaks the pod's local TZ setting
    into the emitted data, silently breaking the (seed, file_id, scale)
    -> same content contract when two pods (or the same pipeline re-run
    after a base-image TZ change) happen to run under different TZs."""
    start_ts = config.timestamp_start.timestamp()
    end_ts = config.timestamp_end.timestamp()
    timestamps = rng.uniform(start_ts, end_ts, size=rows)
    return [datetime.fromtimestamp(ts, tz=timezone.utc) for ts in timestamps]


def generate_emails(
    rows: int, rng: np.random.Generator, duplicate_pct: float, dirty_ratio: float
) -> list:
    """Generate emails with .DUPLICATE pattern + v2 corruption modes."""
    emails = []
    for _ in range(rows):
        user_id = rng.integers(1000, 999999)
        domain = rng.choice(EMAIL_DOMAINS)
        email = f"user{user_id}@{domain}"
        if rng.random() < duplicate_pct:
            email = email.replace("@", ".DUPLICATE@")
        emails.append(email)

    # v2: Apply corruption to dirty_ratio fraction
    if dirty_ratio > 0:
        n_dirty = int(rows * dirty_ratio)
        if n_dirty > 0:
            dirty_indices = rng.choice(rows, size=n_dirty, replace=False)
            modes = rng.integers(0, 6, size=n_dirty)
            for idx, mode in zip(dirty_indices, modes, strict=False):
                e = emails[idx]
                if mode == 0:
                    emails[idx] = e.replace("@", "")  # missing @
                elif mode == 1:
                    emails[idx] = e.upper()  # ALL CAPS
                elif mode == 2:
                    emails[idx] = f"  {e}  "  # whitespace
                elif mode == 3:
                    emails[idx] = e.replace("@", "@@")  # double @
                elif mode == 4:
                    emails[idx] = e.rsplit(".", 1)[0]  # missing TLD
                elif mode == 5:
                    emails[idx] = e.replace("@", ".at.")  # @ -> .at.

    return emails


def generate_phones(rows: int, rng: np.random.Generator, dirty_ratio: float) -> list:
    """Generate phones with mixed formats + v2 corruption modes."""
    phones = []
    for _ in range(rows):
        area = rng.integers(200, 999)
        prefix = rng.integers(200, 999)
        line = rng.integers(1000, 9999)
        if rng.random() < 0.5:
            phones.append(f"+1{area}{prefix}{line}")
        else:
            phones.append(f"({area}) {prefix}-{line}")

    # v2: Apply corruption to dirty_ratio fraction
    if dirty_ratio > 0:
        n_dirty = int(rows * dirty_ratio)
        if n_dirty > 0:
            dirty_indices = rng.choice(rows, size=n_dirty, replace=False)
            modes = rng.integers(0, 4, size=n_dirty)
            for idx, mode in zip(dirty_indices, modes, strict=False):
                area = rng.integers(200, 999)
                prefix = rng.integers(200, 999)
                line = rng.integers(1000, 9999)
                if mode == 0:
                    phones[idx] = f"{area}{prefix}{line}"  # digits only
                elif mode == 1:
                    phones[idx] = f"+1-{area}-{prefix}-{line}"  # extra chars
                elif mode == 2:
                    phones[idx] = f"{area}{prefix}"[:8]  # truncated
                elif mode == 3:
                    s = f"{area}{prefix}{line}"
                    phones[idx] = s.replace("0", "O", 1)  # letter O for zero

    return phones


def generate_ips(rows: int, rng: np.random.Generator) -> list:
    """Generate random IPv4 addresses."""
    return [
        f"{rng.integers(1, 255)}.{rng.integers(0, 255)}.{rng.integers(0, 255)}.{rng.integers(1, 254)}"
        for _ in range(rows)
    ]


def generate_user_agents(rows: int, rng: np.random.Generator) -> list:
    """Generate browser user agent strings."""
    agents = []
    for _ in range(rows):
        browser = rng.choice(BROWSERS)
        version = f"{rng.integers(90, 130)}.0"
        device = rng.choice(DEVICE_TYPES)
        os_name = rng.choice(OPERATING_SYSTEMS)
        agents.append(f"{browser}/{version} ({device}; {os_name})")
    return agents


def generate_fingerprints(rows: int, rng: np.random.Generator) -> list:
    """Generate SHA-256 style session fingerprints (64 hex chars)."""
    return [rng.bytes(32).hex() for _ in range(rows)]


def generate_payloads(rows: int, rng: np.random.Generator, size_kb: int) -> list:
    """Generate random hex payloads (compression anchor)."""
    payload_bytes = size_kb * 1024
    return [rng.bytes(payload_bytes).hex() for _ in range(rows)]


def generate_categorical(rows: int, rng: np.random.Generator, choices: list) -> list:
    """Generate categorical values from a list of choices."""
    indices = rng.integers(0, len(choices), size=rows)
    return [choices[i] for i in indices]


def generate_weighted_categorical(
    rows: int, rng: np.random.Generator, choices: list, weights: list
) -> list:
    """Generate categorical values with weighted probability distribution."""
    return list(rng.choice(choices, size=rows, p=weights))


# =============================================================================
# v2: Dirty Data Corruption for Cities/States
# =============================================================================


def corrupt_cities(cities: list, rng: np.random.Generator, dirty_ratio: float) -> list:
    """Apply misspelling corruptions to city names."""
    if dirty_ratio <= 0:
        return cities
    n_dirty = int(len(cities) * dirty_ratio)
    if n_dirty == 0:
        return cities
    dirty_indices = rng.choice(len(cities), size=n_dirty, replace=False)
    for idx in dirty_indices:
        variant = DIRTY_CITY_VARIANTS.get(cities[idx])
        if variant:
            cities[idx] = variant
    return cities


def corrupt_states(states: list, rng: np.random.Generator, dirty_ratio: float) -> list:
    """Apply case/abbreviation corruptions to state names."""
    if dirty_ratio <= 0:
        return states
    n_dirty = int(len(states) * dirty_ratio)
    if n_dirty == 0:
        return states
    dirty_indices = rng.choice(len(states), size=n_dirty, replace=False)
    for idx in dirty_indices:
        variants = DIRTY_STATE_VARIANTS.get(states[idx])
        if variants:
            states[idx] = rng.choice(variants)
    return states


# =============================================================================
# Generator Protocol
# =============================================================================


@runtime_checkable
class Generator(Protocol):
    """Common shape for per-schema data generators.

    Each schema (Customer 360, Financial, future workloads) implements
    this protocol so ``main`` and the multiprocessing workers can stay
    schema-agnostic. Instances must be picklable so ``ProcessPoolExecutor``
    can ship them to workers.

    Determinism: ``generate_file_data(file_id)`` must produce a
    byte-identical Arrow table for a given ``(config.seed, file_id)``.
    """

    schema_name: str

    def ensure_loyalty(self) -> None:
        """Warm any per-schema caches before fork or pickle.

        Named for historical reasons (Customer 360's loyalty lookup);
        implementations may treat as a no-op if they have no
        precomputable state. Idempotent.
        """
        ...

    def generate_file_data(self, file_id: int) -> pa.Table:
        """Return one Parquet-ready Arrow table for ``file_id``."""
        ...


# =============================================================================
# Customer 360 Generator
# =============================================================================


class Customer360Generator:
    """Generator for the Customer 360 synthetic schema.

    Encapsulates the loyalty-lookup cache and file generation for one
    schema. Implements the ``Generator`` protocol.
    """

    schema_name = "customer360"

    def __init__(self, config: "Config"):
        self.config = config
        self._loyalty_member = None
        self._loyalty_tier = None

    def _get_loyalty_lookup(self):
        if self._loyalty_member is None:
            self._loyalty_member, self._loyalty_tier = build_loyalty_lookup(
                self.config.seed, self.config.customer_id_max
            )
        return self._loyalty_member, self._loyalty_tier

    def ensure_loyalty(self) -> None:
        """Build the loyalty lookup if it has not been built yet.

        Called before fork or pickle so worker processes inherit the
        pre-built numpy arrays via copy-on-write rather than each
        rebuilding from the seed. Idempotent.
        """
        self._get_loyalty_lookup()

    def generate_file_data(self, file_id: int) -> pa.Table:
        """Generate one Parquet-ready Arrow table for ``file_id``.

        Deterministic in ``(config.seed, file_id)``; suitable for both
        batch (ProcessPoolExecutor) and continuous (multiprocessing
        Process) workers.
        """
        return _build_customer360_table(file_id, self.config, self._get_loyalty_lookup)


# =============================================================================
# Generator Registry and Dispatch
# =============================================================================


try:
    from financial import FinancialGenerator
except ImportError:  # pragma: no cover -- optional at import time
    FinancialGenerator = None  # type: ignore[assignment,misc]


_GENERATOR_REGISTRY: dict[str, type] = {
    "customer360": Customer360Generator,
}
if FinancialGenerator is not None:
    _GENERATOR_REGISTRY["financial"] = FinancialGenerator


def register_generator(schema_name: str, generator_cls: type) -> None:
    """Register a Generator implementation for ``schema_name``.

    New schemas (e.g. ``financial``) call this at module import time
    to make themselves discoverable to ``create_generator``.
    """
    _GENERATOR_REGISTRY[schema_name] = generator_cls


def create_generator(schema_name: str, config: Config) -> Generator:
    """Build a Generator for the requested schema.

    Raises ``ValueError`` with the list of registered schemas when
    ``schema_name`` is unknown, so misconfigured deployments fail loudly
    at startup rather than silently generating the wrong shape.
    """
    try:
        cls = _GENERATOR_REGISTRY[schema_name]
    except KeyError as exc:
        known = ", ".join(sorted(_GENERATOR_REGISTRY)) or "(none)"
        raise ValueError(
            f"Unknown datagen schema {schema_name!r}. Registered schemas: {known}."
        ) from exc
    return cls(config)


# =============================================================================
# File Generation
# =============================================================================


def _build_customer360_table(file_id: int, config: Config, get_loyalty_fn) -> pa.Table:
    """Generate data for a single Parquet file matching Bronze schema.

    Applies all 7 v2 realism features:
    1. Zipf customer IDs
    2. Weighted interaction types
    3. Conditional nulls
    4. Dirty data corruption
    5. Channel-device coherence
    6. Log-normal transaction amounts
    7. Customer-consistent loyalty
    """

    # Deterministic seed per file for reproducibility
    rng = np.random.default_rng(seed=config.seed + file_id)
    rows = config.rows_per_file

    # Calculate global row_id offset
    row_id_start = file_id * rows

    # Generate city/state pairs (intentionally inconsistent)
    city_state_indices = rng.integers(0, len(CITIES), size=rows)
    cities = [CITIES[i][0] for i in city_state_indices]
    states = [CITIES[i][1] for i in city_state_indices]

    # v2 feature #4: Dirty data corruption for cities and states
    cities = corrupt_cities(cities, rng, config.dirty_ratio)
    states = corrupt_states(states, rng, config.dirty_ratio)

    # v2 feature #2: Weighted interaction types
    interaction_types = generate_weighted_categorical(
        rows, rng, INTERACTION_TYPES, INTERACTION_WEIGHTS
    )

    # v2 feature #6: Log-normal transaction amounts for purchases
    raw_amounts = np.exp(rng.normal(4.3, 1.2, size=rows))
    raw_amounts = np.clip(raw_amounts, 1.0, 9999.99)
    raw_amounts = np.round(raw_amounts, 2)
    itype_arr = np.array(interaction_types)
    purchase_mask = itype_arr == "purchase"
    transaction_amounts = np.where(purchase_mask, raw_amounts, 0.0)

    # Generate page views based on interaction type
    page_views = []
    for it in interaction_types:
        if it in ("browse", "purchase"):
            page_views.append(int(rng.integers(1, 21)))
        else:
            page_views.append(0)
    page_views = np.array(page_views, dtype=np.int32)

    # Generate time_on_site_seconds based on page_views
    time_on_site_seconds = []
    for pv in page_views:
        if pv > 0:
            time_on_site_seconds.append(int(rng.integers(30, 3630)))
        else:
            time_on_site_seconds.append(0)
    time_on_site_seconds = np.array(time_on_site_seconds, dtype=np.int32)

    # Generate bounce_rate based on page_views
    bounce_rate = np.where(page_views == 1, 1.0, 0.0)

    # Generate support-related columns
    support_ticket_ids = []
    issue_categories = []
    satisfaction_scores = []
    for it in interaction_types:
        if it == "support":
            support_ticket_ids.append(f"TKT{rng.integers(10000, 100000)}")
            issue_categories.append(rng.choice(ISSUE_CATEGORIES))
            satisfaction_scores.append(int(rng.integers(1, 6)))
        else:
            support_ticket_ids.append(None)
            issue_categories.append(None)
            satisfaction_scores.append(None)

    # Generate marketing attribution (40% have campaign)
    campaign_ids = []
    utm_sources = []
    utm_mediums = []
    for _ in range(rows):
        if rng.random() < 0.4:
            campaign_ids.append(f"CMP{rng.integers(100, 1000)}")
            utm_sources.append(rng.choice(UTM_SOURCES))
            utm_mediums.append(rng.choice(UTM_MEDIUMS))
        else:
            campaign_ids.append(None)
            utm_sources.append(None)
            utm_mediums.append(None)

    # v2 feature #1: Zipf-distributed customer IDs
    raw_zipf = rng.zipf(1.5, size=rows)
    customer_ids = (raw_zipf % (config.customer_id_max + 1)).astype(np.int64)

    # v2 feature #7: Customer-consistent loyalty
    loyalty_member_lookup, loyalty_tier_lookup = get_loyalty_fn()
    loyalty_members = loyalty_member_lookup[customer_ids]
    tier_indices = loyalty_tier_lookup[customer_ids]

    loyalty_tiers = []
    points_earned = []
    points_redeemed = []
    for i in range(rows):
        if loyalty_members[i]:
            loyalty_tiers.append(LOYALTY_TIERS[tier_indices[i]])
            if interaction_types[i] == "purchase":
                points_earned.append(int(transaction_amounts[i] * 10))
            else:
                points_earned.append(0)
            if rng.random() < 0.1:
                points_redeemed.append(int(rng.integers(100, 1000)))
            else:
                points_redeemed.append(0)
        else:
            loyalty_tiers.append(None)
            points_earned.append(0)
            points_redeemed.append(0)

    # Generate base columns that will be conditionally nulled
    product_ids = [f"PRD{rng.integers(10000, 99999):05d}" for _ in range(rows)]
    product_categories = generate_categorical(rows, rng, PRODUCT_CATEGORIES)
    click_counts = rng.integers(1, 101, size=rows, dtype=np.int32)
    cart_values = np.round(rng.uniform(0, 9999.99, size=rows), 2)
    items_in_cart = rng.integers(0, 21, size=rows, dtype=np.int32)
    channels = generate_categorical(rows, rng, CHANNELS)
    device_types = generate_categorical(rows, rng, DEVICE_TYPES)
    browsers_list = generate_categorical(rows, rng, BROWSERS)

    # v2 feature #3: Conditional nulls for event-type-dependent columns
    # Null out product/cart columns for login and support events
    no_product_mask = np.isin(itype_arr, ["login", "support"])
    product_ids = [None if no_product_mask[i] else product_ids[i] for i in range(rows)]
    product_categories = [
        None if no_product_mask[i] else product_categories[i] for i in range(rows)
    ]
    cart_values = np.where(no_product_mask, np.nan, cart_values)
    items_in_cart_list = [
        None if no_product_mask[i] else int(items_in_cart[i]) for i in range(rows)
    ]
    click_counts_list = [None if no_product_mask[i] else int(click_counts[i]) for i in range(rows)]

    # v2 feature #5: Channel-device coherence
    # Null out device_type and browser for store and call_center channels
    channel_arr = np.array(channels)
    offline_mask = np.isin(channel_arr, ["store", "call_center"])
    device_types = [None if offline_mask[i] else device_types[i] for i in range(rows)]
    browsers_list = [None if offline_mask[i] else browsers_list[i] for i in range(rows)]

    # Generate all columns matching Customer 360 schema
    data = {
        "id": np.arange(row_id_start, row_id_start + rows, dtype=np.int64),
        "row_id": np.arange(row_id_start, row_id_start + rows, dtype=np.int64),
        "event_timestamp": generate_timestamps(rows, rng, config),
        "event_id": generate_uuids(rows, rng),
        "session_id": generate_uuids(rows, rng),
        "customer_id": customer_ids,
        "email_raw": generate_emails(rows, rng, config.duplicate_email_pct, config.dirty_ratio),
        "phone_raw": generate_phones(rows, rng, config.dirty_ratio),
        "interaction_type": interaction_types,
        "product_id": product_ids,
        "product_category": product_categories,
        "transaction_amount": transaction_amounts,
        "currency": generate_categorical(rows, rng, CURRENCIES),
        "channel": channels,
        "device_type": device_types,
        "browser": browsers_list,
        "ip_address": generate_ips(rows, rng),
        "city_raw": cities,
        "state_raw": states,
        "zip_code": [f"{rng.integers(10000, 99999):05d}" for _ in range(rows)],
        "page_views": page_views,
        "time_on_site_seconds": time_on_site_seconds,
        "bounce_rate": bounce_rate,
        "click_count": click_counts_list,
        "cart_value": cart_values,
        "items_in_cart": items_in_cart_list,
        # Support columns
        "support_ticket_id": support_ticket_ids,
        "issue_category": issue_categories,
        "satisfaction_score": satisfaction_scores,
        # Marketing attribution
        "campaign_id": campaign_ids,
        "utm_source": utm_sources,
        "utm_medium": utm_mediums,
        # Loyalty program
        "loyalty_member": loyalty_members,
        "loyalty_tier": loyalty_tiers,
        "points_earned": np.array(points_earned, dtype=np.int32),
        "points_redeemed": np.array(points_redeemed, dtype=np.int32),
        # Data quality
        "data_source": generate_weighted_categorical(rows, rng, DATA_SOURCES, DATA_SOURCE_WEIGHTS),
        "data_quality_flag": generate_weighted_categorical(
            rows, rng, DATA_QUALITY_FLAGS, DATA_QUALITY_WEIGHTS
        ),
        # Session data
        "raw_user_agent": generate_user_agents(rows, rng),
        "session_fingerprint": generate_fingerprints(rows, rng),
        "interaction_payload": generate_payloads(rows, rng, config.payload_kb),
    }

    # Define schema with correct types matching Customer 360
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("row_id", pa.int64()),
            # Explicit UTC in the schema. Naive `pa.timestamp("us")` silently
            # drops the tz metadata even when the input datetimes are
            # tz-aware, which downstream tools then read as "local" time.
            # The upstream `generate_timestamps` already returns UTC-aware
            # datetimes; carrying that into the schema keeps the (seed,
            # file_id) -> same-content contract witnessable in parquet.
            ("event_timestamp", pa.timestamp("us", tz="UTC")),
            ("event_id", pa.string()),
            ("session_id", pa.string()),
            ("customer_id", pa.int64()),
            ("email_raw", pa.string()),
            ("phone_raw", pa.string()),
            ("interaction_type", pa.string()),
            ("product_id", pa.string()),
            ("product_category", pa.string()),
            ("transaction_amount", pa.float64()),
            ("currency", pa.string()),
            ("channel", pa.string()),
            ("device_type", pa.string()),
            ("browser", pa.string()),
            ("ip_address", pa.string()),
            ("city_raw", pa.string()),
            ("state_raw", pa.string()),
            ("zip_code", pa.string()),
            ("page_views", pa.int32()),
            ("time_on_site_seconds", pa.int32()),
            ("bounce_rate", pa.float64()),
            ("click_count", pa.int32()),
            ("cart_value", pa.float64()),
            ("items_in_cart", pa.int32()),
            ("support_ticket_id", pa.string()),
            ("issue_category", pa.string()),
            ("satisfaction_score", pa.int32()),
            ("campaign_id", pa.string()),
            ("utm_source", pa.string()),
            ("utm_medium", pa.string()),
            ("loyalty_member", pa.bool_()),
            ("loyalty_tier", pa.string()),
            ("points_earned", pa.int32()),
            ("points_redeemed", pa.int32()),
            ("data_source", pa.string()),
            ("data_quality_flag", pa.string()),
            ("raw_user_agent", pa.string()),
            ("session_fingerprint", pa.string()),
            ("interaction_payload", pa.string()),
        ]
    )

    return pa.Table.from_pydict(data, schema=schema)


def _parquet_compression() -> tuple[str, int | None]:
    """Codec + optional level from DG_COMPRESSION env.

    Accepts (matches the Rust datagen's contract exactly, to prevent silent
    cross-image codec drift):
        snappy | zstd | zstdN for N in 1..=22 | lz4 | none | uncompressed
    Default: zstd1.

    Raises ValueError on any other value or out-of-range level. An early
    ValueError is easier to diagnose than a mid-run pyarrow crash or a
    silent fallback to a different codec than the operator expected."""
    v = os.environ.get("DG_COMPRESSION", "zstd1").strip().lower()
    if v in ("", "zstd", "zstd1"):
        return "zstd", 1
    if v == "snappy":
        return "snappy", None
    if v == "lz4":
        return "lz4", None
    if v in ("none", "uncompressed"):
        return "none", None
    if v.startswith("zstd"):
        try:
            lvl = int(v[4:])
        except ValueError as exc:
            raise ValueError(
                f"DG_COMPRESSION={v!r}: could not parse level after 'zstd'"
            ) from exc
        if not 1 <= lvl <= 22:
            raise ValueError(
                f"DG_COMPRESSION={v!r}: level out of range 1..=22"
            )
        return "zstd", lvl
    raise ValueError(
        f"DG_COMPRESSION={v!r}: expected snappy|zstd|zstdN|lz4|none"
    )


def write_file_to_s3(file_id: int, config: Config, generator: Generator) -> dict:
    """Generate and write a single Parquet file to S3."""
    try:
        table = generator.generate_file_data(file_id)

        buffer = io.BytesIO()
        codec, level = _parquet_compression()
        if codec == "zstd":
            pq.write_table(table, buffer, compression=codec, compression_level=level)
        else:
            pq.write_table(table, buffer, compression=codec)
        # Capture file size BEFORE upload -- boto3's upload_fileobj can close
        # the fileobj under some transports (observed with S3 endpoints that
        # trigger the multipart path). buffer.tell() after upload then raises
        # "I/O operation on closed file", flipping every file to failure
        # despite the S3 upload having completed. Read size up-front.
        file_size = buffer.getbuffer().nbytes
        num_rows = table.num_rows
        # Snapshot metrics before upload for the same reason.
        metrics_snapshot = getattr(generator, "last_file_metrics", None)
        if isinstance(metrics_snapshot, dict):
            metrics_snapshot = dict(metrics_snapshot)  # defensive copy
        buffer.seek(0)

        s3_client = get_s3_client(config)
        s3_key = f"{config.prefix}/part-{file_id:06d}.parquet"

        s3_client.upload_fileobj(buffer, config.bucket, s3_key)

        # Observability C1: emit a single JSON line per file. Wrapped in
        # its own try/except so a metrics-emission failure can't flip an
        # otherwise-successful upload to failed.
        try:
            import json as _json
            emission = {
                "event": "DATAGEN_FILE_METRICS",
                "file_id": file_id,
                "bytes": file_size,
                "s3_key": s3_key,
            }
            if isinstance(metrics_snapshot, dict):
                emission.update(metrics_snapshot)
            print(_json.dumps(emission), flush=True)
        except Exception:
            pass

        return {
            "file_id": file_id,
            "success": True,
            "rows": num_rows,
            "size_bytes": file_size,
            "s3_key": s3_key,
        }
    except Exception as e:
        return {
            "file_id": file_id,
            "success": False,
            "error": str(e),
        }


# =============================================================================
# Continuous Mode (Multiprocessing Generator + Threading Uploader)
# =============================================================================


def _continuous_generator_worker(
    file_queue: MPQueue,
    upload_queue: MPQueue,
    config_dict: dict,
    generator_id: int,
    mp_shutdown_event,
):
    """
    Generator process: pulls file_ids from file_queue, generates data,
    compresses to Parquet bytes, and pushes to upload_queue.

    ``mp_shutdown_event`` is a multiprocessing.Event shared with the parent.
    When set (e.g. by the parent's SIGTERM handler), this worker exits at
    the next poll boundary without draining the rest of file_queue.
    """
    # Reset signal handlers inherited from the parent. SIG_IGN is deliberate:
    # SIGTERM to the pod's process group must NOT kill this worker mid-file.
    # Graceful shutdown drives the exit path via mp_shutdown_event, set by
    # the main process's SIGTERM handler. If the pod grace period expires
    # the kubelet sends SIGKILL, which is uncatchable.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    config = Config(**config_dict)
    has_duration = bool(config.duration_seconds)

    # Build a per-process generator; warm any per-schema caches here so
    # each subsequent file_id only pays the cache-hit cost.
    generator = create_generator(config.schema_name, config)
    generator.ensure_loyalty()

    def _shutdown() -> bool:
        return mp_shutdown_event.is_set()

    def _shutdown_exit():
        # multiprocessing.Queue uses a background feeder thread to move
        # items from the process-local buffer through the pipe. If the
        # reader (uploader) has already exited on SHUTDOWN_REQUESTED,
        # any pending items in the feeder buffer cause the process to
        # hang on exit waiting for feeder-thread join. cancel_join_thread
        # tells the interpreter to abandon those pending items and let
        # the process exit -- which is exactly what we want during
        # shutdown (dropped items are regenerated on resume). Without
        # this, generators consumed 3s each in the parent's join(timeout=3)
        # before .kill() finished them, burning the pod's grace budget.
        # (file_queue only has a feeder in the parent -- child never
        # put()s to it -- so no cancel needed on that one.)
        try:
            upload_queue.cancel_join_thread()
        except Exception:
            pass

    while True:
        # mp_shutdown fast-path: exit immediately without touching
        # upload_queue. Uploader threads already exit on
        # SHUTDOWN_REQUESTED (their own top-of-loop check) so no pill is
        # needed. put_nowait on a full queue would block otherwise; that
        # blocked generator kills for us and burns grace-period budget.
        if _shutdown():
            _shutdown_exit()
            return

        # Short poll timeout so shutdown is detected within ~0.5s regardless
        # of file_queue depth.
        try:
            file_id = file_queue.get(timeout=0.5)
        except Exception:
            if _shutdown():
                _shutdown_exit()
                return
            if has_duration:
                continue
            if file_queue.empty():
                break
            continue

        if file_id is None:
            # Legacy pill on file_queue (from a non-shutdown Phase 1 or
            # duration expiry). Simply exit; the main thread will set the
            # generators_done_event after joining, so uploaders shut down
            # via that path rather than pill-relay.
            break

        # Check shutdown once more before starting a potentially expensive
        # generate+encode cycle.
        if _shutdown():
            _shutdown_exit()
            return

        try:
            table = generator.generate_file_data(file_id)
            rows = table.num_rows

            # Emit per-file metrics from the generator worker.
            metrics = getattr(generator, "last_file_metrics", None)
            if isinstance(metrics, dict):
                try:
                    import json as _json
                    emission = {"event": "DATAGEN_FILE_METRICS", "worker": generator_id}
                    emission.update(metrics)
                    print(_json.dumps(emission), flush=True)
                except Exception:
                    pass

            buffer = io.BytesIO()
            codec, level = _parquet_compression()
            if codec == "zstd":
                pq.write_table(table, buffer, compression=codec, compression_level=level)
            else:
                pq.write_table(table, buffer, compression=codec)
            parquet_bytes = buffer.getvalue()
            size_bytes = len(parquet_bytes)

            del table
            buffer.close()

            # Bounded put: if upload_queue is full and shutdown fires we
            # must not block forever. Retry with periodic shutdown checks.
            while True:
                try:
                    upload_queue.put((file_id, parquet_bytes, rows, size_bytes), timeout=0.5)
                    break
                except Exception:
                    if _shutdown():
                        _shutdown_exit()
                        return

        except Exception as e:
            try:
                upload_queue.put((file_id, None, 0, 0, str(e)), timeout=2)
            except Exception:
                pass


def _continuous_uploader_worker(
    upload_queue: MPQueue,
    generators_done_event: threading.Event,
    config: Config,
    results: list,
    results_lock: threading.Lock,
    progress_callback,
):
    """Uploader thread: pulls compressed data from upload_queue and uploads to S3.

    Shutdown protocol has two exits:

    - Fast SIGTERM/SIGINT path: SHUTDOWN_REQUESTED (module-level threading
      Event) is set by the signal handler. Uploader checks it at the top
      of every iteration and returns immediately, dropping any queued items
      (their generators haven't yet checkpointed them, so resume regenerates).

    - Normal drain path: main sets ``generators_done_event`` after all
      generator processes have joined. Uploader exits on the next empty
      poll if the event is set. This replaced the prior pill-counter
      design, which could hang at duration expiry: generators relayed
      pills via ``upload_queue.put_nowait`` which silently dropped them
      when the queue was full, so the pill counter never reached its
      target and uploaders spun forever on an already-drained queue.
    """
    s3_client = get_s3_client(config)

    while True:
        if SHUTDOWN_REQUESTED.is_set():
            return

        try:
            item = upload_queue.get(timeout=1)
        except Exception:
            # Empty poll: exit if generators are done. Otherwise there
            # may still be items in transit through the mp.Queue pipe.
            if generators_done_event.is_set():
                return
            continue

        if item is None:
            # Legacy pill (from a generator that hit its file_queue's
            # None). No longer used for shutdown coordination but still
            # emitted for backwards behavior; just consume and continue.
            continue

        if len(item) == 5:
            file_id, _, _, _, error = item
            with results_lock:
                results.append(
                    {"file_id": file_id, "success": False, "error": f"Generation error: {error}"}
                )
            progress_callback(0, 0, False)
            continue

        file_id, parquet_bytes, rows, size_bytes = item
        s3_key = f"{config.prefix}/part-{file_id:06d}.parquet"

        # Shutdown fast-path: if SIGTERM has fired, DROP this in-flight
        # item rather than start a new S3 upload. put_object below is a
        # single request (not multipart), so a completed upload is atomic
        # and no MPU ghosts leak -- but each upload can still take multiple
        # seconds for a 512MB payload, which could push us past the pod's
        # remaining grace period. Dropping a queued item is safe: no
        # checkpoint has been written for it, so a resumed pod will
        # regenerate it. Continue rather than return so the pill loop above
        # can advance and terminate the thread through the normal path.
        if SHUTDOWN_REQUESTED.is_set():
            with results_lock:
                results.append(
                    {"file_id": file_id, "success": False, "error": "dropped: shutdown"}
                )
            progress_callback(0, 0, False)
            continue

        try:
            # put_object (single PUT) rather than upload_fileobj (which
            # switches to boto3 TransferManager multipart for >8MB, and
            # its worker threads are not interruptible by shutdown -- an
            # in-flight multipart on a 512MB file could orphan parts on
            # FlashBlade if the pod dies mid-upload). put_object is atomic:
            # either the object exists in full or it doesn't. FlashBlade
            # accepts single PUTs up to 5GB, well over our per-file cap.
            s3_client.put_object(
                Bucket=config.bucket,
                Key=s3_key,
                Body=parquet_bytes,
            )

            with results_lock:
                results.append(
                    {
                        "file_id": file_id,
                        "success": True,
                        "rows": rows,
                        "size_bytes": size_bytes,
                        "s3_key": s3_key,
                    }
                )

            progress_callback(rows, size_bytes, True)

        except Exception as e:
            with results_lock:
                results.append(
                    {"file_id": file_id, "success": False, "error": f"Upload error: {e}"}
                )
            progress_callback(0, 0, False)


def run_continuous(
    config: Config, my_files: list[int], completed: set, checkpoint_file: str
) -> tuple[int, int, list[dict]]:
    """Run continuous mode with multiprocessing generators and threading uploaders.

    If ``config.duration_seconds`` is set, the function runs in two phases:

    Phase 1 -- generate the initial file assignments (honours ``--target-tb``
    as a minimum data volume).  File IDs come from the pre-computed ``my_files``
    list, interleaved across pods.

    Phase 2 -- after Phase 1 completes, if time remains, new files are generated
    with auto-incrementing IDs (``total_files + cycle * total_nodes + node_id``)
    until the duration timer expires.  Each pod produces unique IDs that never
    collide with other pods.  Duration-phase files are NOT checkpointed.

    Without ``--duration`` the function behaves as before: generate exactly
    ``len(remaining)`` files and exit.
    """
    remaining = [f for f in my_files if f not in completed]

    if not remaining and not config.duration_seconds:
        return 0, 0, []

    # MP_SHUTDOWN_EVENT and SHUTDOWN_REQUESTED are module-level singletons
    # created at import; no need to make new ones here. Re-install handlers
    # in case main() didn't (defensive; harmless if it did).
    _install_shutdown_handlers()

    file_queue = MPQueue()
    upload_queue = MPQueue(maxsize=QUEUE_DEPTH)

    # Phase 1: enqueue initial file assignments
    for file_id in remaining:
        file_queue.put(file_id)

    # If no duration mode, send poison pills now so generators stop after Phase 1
    if not config.duration_seconds:
        for _ in range(NUM_GENERATORS):
            file_queue.put(None)

    config_dict = {
        "target_tb": config.target_tb,
        "workers": config.workers,
        "seed": config.seed,
        "node_id": config.node_id,
        "total_nodes": config.total_nodes,
        "payload_kb": config.payload_kb,
        "file_size_mb": config.file_size_mb,
        "bucket": config.bucket,
        "prefix": config.prefix,
        "checkpoint_file": config.checkpoint_file,
        "resume": config.resume,
        "timestamp_start": config.timestamp_start.isoformat(),
        "timestamp_end": config.timestamp_end.isoformat(),
        "customer_id_max": config.customer_id_max,
        "duplicate_email_pct": config.duplicate_email_pct,
        "dirty_ratio": config.dirty_ratio,
        "duration_seconds": config.duration_seconds,
        "schema_name": config.schema_name,
    }

    results = []
    results_lock = threading.Lock()

    total_rows = 0
    total_bytes = 0
    files_done = 0
    progress_lock = threading.Lock()

    def progress_callback(rows, size_bytes, success):
        nonlocal total_rows, total_bytes, files_done
        with progress_lock:
            if success:
                total_rows += rows
                total_bytes += size_bytes
            files_done += 1

    generators = []
    for i in range(NUM_GENERATORS):
        p = Process(
            target=_continuous_generator_worker,
            args=(file_queue, upload_queue, config_dict, i, MP_SHUTDOWN_EVENT),
            name=f"gen-{i}",
        )
        p.start()
        generators.append(p)

    # Threading Event that main sets after all generator processes have
    # been joined. Uploaders exit on the next empty poll if this is set.
    # Replaces the prior pill-counter design (see uploader worker docstring
    # for why relayed pills through a bounded queue were unsafe).
    generators_done_event = threading.Event()
    uploaders = []
    for i in range(NUM_UPLOADERS):
        t = threading.Thread(
            target=_continuous_uploader_worker,
            args=(
                upload_queue,
                generators_done_event,
                config,
                results,
                results_lock,
                progress_callback,
            ),
            name=f"upload-{i}",
        )
        t.start()
        uploaders.append(t)

    start_time = time.monotonic()
    phase1_target = max(len(remaining), 1)
    phase1_done = False

    with tqdm(total=phase1_target, unit="files") as pbar:
        last_done = 0
        checkpoint_interval = 10
        # Duration-phase file ID counter -- starts beyond the initial file set
        next_duration_id = config.total_files
        duration_files_enqueued = 0

        while True:
            with progress_lock:
                current_done = files_done
                current_rows = total_rows
                current_bytes = total_bytes

            # SIGTERM/SIGINT triggers graceful shutdown. The signal handler
            # sets both SHUTDOWN_REQUESTED (threading Event, for main + uploader
            # threads) and mp_shutdown_event (shared with generator children).
            # Generators check the mp event between file_queue.get()s and
            # inside their upload_queue.put() retry loop, so they exit within
            # ~0.5s regardless of how many pending file_ids sit ahead of any
            # queued poison pill. Also enqueue N pills as a backup path for
            # generators that were mid-put() and skipped the shutdown check
            # window; put_nowait avoids blocking if the queue happens to be
            # full (unlikely but possible under high pressure).
            if SHUTDOWN_REQUESTED.is_set():
                for _ in range(NUM_GENERATORS):
                    try:
                        file_queue.put(None, timeout=0.1)
                    except Exception:
                        pass
                if current_done > last_done:
                    pbar.update(current_done - last_done)
                pbar.set_description("Shutdown requested (SIGTERM/SIGINT)")
                break

            # Check duration expiry
            if config.duration_seconds:
                elapsed = time.monotonic() - start_time
                if elapsed >= config.duration_seconds:
                    # Time's up. Use the SAME mechanism as SIGTERM: flip
                    # MP_SHUTDOWN_EVENT so generators exit via their fast
                    # out-of-band check rather than by draining thousands
                    # of file_ids to find pill(s) at the tail of file_queue.
                    # (Without this the process wedges: generators sit in
                    # CPU-bound generate_file_data() calls, complete the
                    # current file, put to upload_queue, loop back to
                    # file_queue.get() -- but there are 1500+ file_ids
                    # ahead of the pills so they keep pumping. Duration-
                    # expiry shutdown then blows past 30s.)
                    MP_SHUTDOWN_EVENT.set()
                    # Belt-and-braces pills for the rare generator that's
                    # already blocked on file_queue.get() with no signal.
                    for _ in range(NUM_GENERATORS):
                        try:
                            file_queue.put(None, timeout=0.1)
                        except Exception:
                            pass
                    if current_done > last_done:
                        pbar.update(current_done - last_done)
                    pbar.set_description(f"Duration {config.duration_seconds}s reached")
                    break

            # Phase 1 completion check
            if not phase1_done and current_done >= len(remaining):
                phase1_done = True
                if config.duration_seconds:
                    # Phase 1 done but duration remains -- update progress bar
                    if current_done > last_done:
                        pbar.update(current_done - last_done)
                        last_done = current_done
                    remaining_secs = config.duration_seconds - (time.monotonic() - start_time)
                    pbar.set_description(f"Phase 1 done, feeding for {remaining_secs:.0f}s more")
                    pbar.total = None  # Switch to indeterminate mode
                    pbar.refresh()
                else:
                    # No duration mode -- stop
                    if current_done > last_done:
                        pbar.update(current_done - last_done)
                    break

            # Phase 2: feed new file IDs if duration mode and queue is low
            if config.duration_seconds and phase1_done:
                try:
                    queue_size = file_queue.qsize()
                except NotImplementedError:
                    queue_size = 0
                # Keep the queue fed but not overloaded
                while queue_size < NUM_GENERATORS * 2:
                    # Each pod uses unique IDs: total_files + cycle * total_nodes + node_id
                    file_id = next_duration_id + config.node_id
                    next_duration_id += config.total_nodes
                    file_queue.put(file_id)
                    duration_files_enqueued += 1
                    queue_size += 1

            if current_done > last_done:
                pbar.update(current_done - last_done)
                pbar.set_postfix(
                    {
                        "rows": f"{current_rows / 1e6:.1f}M",
                        "size": f"{current_bytes / 1e9:.1f}GB",
                        "queue": upload_queue.qsize(),
                    }
                )

                if not config.duration_seconds and current_done % checkpoint_interval == 0:
                    with results_lock:
                        for r in results:
                            if r["success"]:
                                completed.add(r["file_id"])
                    save_checkpoint(checkpoint_file, completed)

                last_done = current_done

            # 0.1s not 0.5s so a SIGTERM landing between iterations still
            # exits within a bounded window. Cheap; wakeups are just cheap
            # progress polling.
            threading.Event().wait(0.1)

    # Reap generators first. On graceful shutdown they exit within ~0.5s of
    # the mp event flipping (cancel_join_thread on their outgoing queue
    # prevents the feeder from blocking exit). If any is stuck (rare),
    # .kill() is SIGKILL directly -- .terminate() would send SIGTERM which
    # the worker's SIG_IGN handler drops.
    for p in generators:
        p.join(timeout=3)
        if p.is_alive():
            p.kill()
            p.join(timeout=1)

    # Then tell uploaders no more items are coming, so an empty poll can
    # confidently exit rather than spin waiting for more work.
    generators_done_event.set()
    for t in uploaders:
        t.join(timeout=10)
        if t.is_alive():
            print(f"WARN: uploader {t.name} did not exit within 10s", file=sys.stderr, flush=True)

    # Cancel the parent's mp.Queue feeder threads. At duration expiry, the
    # parent pre-loaded file_queue with the full remaining list (thousands
    # of file_ids); only a handful were consumed before the timer fired.
    # The rest sit in the parent's feeder-thread outgoing buffer waiting
    # to be piped to children that are now dead. Interpreter exit joins
    # those feeders, which block forever on the closed pipe. cancel_join
    # abandons the buffered items so the interpreter can exit cleanly.
    try:
        file_queue.cancel_join_thread()
    except Exception:
        pass
    try:
        upload_queue.cancel_join_thread()
    except Exception:
        pass

    # Only checkpoint Phase 1 files (not duration-phase ephemeral files)
    with results_lock:
        for r in results:
            if r["success"] and r["file_id"] < config.total_files:
                completed.add(r["file_id"])
    save_checkpoint(checkpoint_file, completed)

    return total_rows, total_bytes, results


# =============================================================================
# Checkpoint Management
# =============================================================================


def load_checkpoint(checkpoint_file: str) -> set:
    """Load completed file IDs from checkpoint."""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file) as f:
                data = json.load(f)
                return set(data.get("completed", []))
        except Exception:
            pass
    return set()


def save_checkpoint(checkpoint_file: str, completed: set):
    """Save completed file IDs to checkpoint."""
    with open(checkpoint_file, "w") as f:
        json.dump({"completed": list(completed), "updated": datetime.now().isoformat()}, f)


# =============================================================================
# Main Execution
# =============================================================================


def main():
    from concurrent.futures import ProcessPoolExecutor, as_completed

    parser = argparse.ArgumentParser(
        description="Generate synthetic Customer 360 data to S3 Bronze bucket (v2 with realism features)"
    )
    parser.add_argument(
        "--target-tb", type=float, required=True, help="Target data size in terabytes"
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=None,
        help=(
            "Lakebench scale factor (1, 5, 10, 100...). Used by schema-specific "
            "generators (financial: number of typology instances per typology). "
            "If omitted, computed from --target-tb assuming ~10GB per scale unit."
        ),
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel workers (default: 4)"
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)"
    )
    parser.add_argument(
        "--node-id",
        type=int,
        default=None,
        help="Node ID for multi-node generation (default: from JOB_COMPLETION_INDEX or 0)",
    )
    parser.add_argument(
        "--total-nodes", type=int, default=1, help="Total number of nodes (default: 1)"
    )
    parser.add_argument("--payload-kb", type=int, default=2, help="Payload size in KB (default: 2)")
    parser.add_argument(
        "--bucket",
        type=str,
        default="lakebench-bronze",
        help="S3 bucket name (default: lakebench-bronze)",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="customer/interactions",
        help="S3 key prefix (default: customer/interactions)",
    )
    parser.add_argument(
        "--resume", action="store_true", help="Resume from checkpoint if interrupted"
    )
    parser.add_argument(
        "--file-size-mb", type=int, default=512, help="Target file size in MB (default: 512)"
    )
    parser.add_argument(
        "--mode",
        choices=["batch", "sequential", "continuous"],
        default="batch",
        help="Generation mode: batch/sequential (4GB/4CPU) or continuous (8GB/8CPU)",
    )
    parser.add_argument(
        "--dirty-ratio",
        type=float,
        default=0.08,
        help="Fraction of rows receiving dirty data corruption (default: 0.08)",
    )
    parser.add_argument(
        "--timestamp-start",
        type=str,
        default="2024-01-01",
        help="Start date for generated timestamps (ISO format, default: 2024-01-01)",
    )
    parser.add_argument(
        "--timestamp-end",
        type=str,
        default="2025-12-31",
        help="End date for generated timestamps (ISO format, default: 2025-12-31)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Generate data for this many seconds then exit. "
        "After producing target-tb worth of files, continues generating "
        "new files until the timer expires. Use with --mode continuous "
        "for sustained pipeline feeding.",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="customer360",
        help="Workload schema to generate (default: customer360). "
        "Must match a registered Generator.",
    )

    args = parser.parse_args()

    # Get node_id from argument, environment variable (K8s Indexed Job), or default to 0
    if args.node_id is not None:
        node_id = args.node_id
    else:
        node_id = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))

    # Scale factor: passed explicitly by lakebench (--scale), otherwise
    # derived from target_tb assuming ~10 GB per scale unit. Used by
    # schema-specific generators (e.g. FinancialGenerator.typology counts).
    scale_factor = args.scale if args.scale is not None else max(1.0, args.target_tb * 102.4)

    config = Config(
        target_tb=args.target_tb,
        workers=args.workers,
        seed=args.seed,
        node_id=node_id,
        total_nodes=args.total_nodes,
        payload_kb=args.payload_kb,
        bucket=args.bucket,
        prefix=args.prefix,
        resume=args.resume,
        file_size_mb=args.file_size_mb,
        dirty_ratio=args.dirty_ratio,
        timestamp_start=args.timestamp_start,
        timestamp_end=args.timestamp_end,
        duration_seconds=args.duration,
        schema_name=args.schema,
    )
    # Attach the resolved scale as a plain attribute so generators can read it
    # via config.scale without further Config schema changes.
    config.scale = scale_factor

    # Validate S3 credentials
    if not config.s3_access_key or not config.s3_secret_key:
        print("Error: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
        sys.exit(1)

    # Calculate this node's file assignments (interleaved distribution)
    my_files = [f for f in range(config.total_files) if f % config.total_nodes == config.node_id]

    # Load checkpoint if resuming
    completed = set()
    if config.resume:
        completed = load_checkpoint(config.checkpoint_file)

    remaining = [f for f in my_files if f not in completed]

    # Print summary
    if args.mode == "continuous":
        duration_info = f", duration: {config.duration_seconds}s" if config.duration_seconds else ""
        mode_desc = (
            f"continuous ({NUM_GENERATORS} generators, {NUM_UPLOADERS} uploaders{duration_info})"
        )
    else:
        mode_desc = f"sequential ({config.workers} workers)"
    print(f"""
Datagen (realism features enabled)
========================================
Target: {config.target_tb} TB ({config.total_files:,} files total)
Node: {config.node_id + 1}/{config.total_nodes}
My files: {len(my_files):,} ({len(remaining):,} remaining)
Mode: {mode_desc}
Rows per file: ~{config.rows_per_file:,}
Dirty ratio: {config.dirty_ratio:.0%}
Bucket: s3://{config.bucket}/{config.prefix}/
Endpoint: {config.s3_endpoint or "AWS default"}
""")

    if not remaining and not config.duration_seconds:
        print("All files already generated. Use --resume=false to regenerate.")
        return

    # Test S3 connectivity
    try:
        s3_client = get_s3_client(config)
        s3_client.head_bucket(Bucket=config.bucket)
        print(f"S3 connection verified: {config.bucket}")
    except Exception as e:
        print(f"Error connecting to S3: {e}")
        sys.exit(1)

    # Install SIGTERM/SIGINT handlers early. For continuous mode,
    # run_continuous re-installs with the mp_shutdown_event once it has
    # created it, so child generator processes also see shutdown flips.
    # For batch/sequential mode, this install alone (no mp_event) still
    # gives Ctrl-C a clean path: it sets SHUTDOWN_REQUESTED, which the
    # main loop can check between file completions rather than
    # KeyboardInterrupt-ing mid-ProcessPoolExecutor context manager.
    _install_shutdown_handlers()

    # Build the schema-specific generator via dispatch and warm any
    # per-schema caches before forking child processes so workers inherit
    # them via copy-on-write.
    generator = create_generator(config.schema_name, config)
    generator.ensure_loyalty()

    # Generate files
    total_rows = 0
    total_bytes = 0
    errors = []

    if config.duration_seconds:
        print(
            f"\nGenerating files for {config.duration_seconds}s ({len(remaining):,} initial files)..."
        )
    else:
        print(f"\nGenerating {len(remaining):,} files...")

    if args.mode == "continuous":
        total_rows, total_bytes, all_results = run_continuous(
            config, my_files, completed, config.checkpoint_file
        )
        # "dropped: shutdown" is a graceful outcome (SIGTERM fired
        # mid-pipeline; the file will regenerate on resume) so it does not
        # count as a run error. Real S3/generation failures still do.
        errors = [
            r for r in all_results
            if not r["success"] and not r.get("error", "").startswith("dropped:")
        ]
        for r in all_results:
            if r["success"]:
                completed.add(r["file_id"])
        save_checkpoint(config.checkpoint_file, completed)
    else:
        with ProcessPoolExecutor(max_workers=config.workers) as executor:
            futures = {
                executor.submit(write_file_to_s3, file_id, config, generator): file_id
                for file_id in remaining
            }

            with tqdm(total=len(remaining), unit="files") as pbar:
                for future in as_completed(futures):
                    # SIGTERM/SIGINT during batch mode: cancel every future
                    # that hasn't started, then break the loop. The context
                    # manager exit still waits for in-flight workers, but
                    # they upload atomically via put_object so no ghost
                    # multipart uploads are left behind (unlike continuous
                    # mode's multipart path).
                    if SHUTDOWN_REQUESTED.is_set():
                        for f in futures:
                            f.cancel()
                        print(
                            "Shutdown requested; cancelling pending batch work.",
                            file=sys.stderr,
                        )
                        break

                    result = future.result()

                    if result["success"]:
                        completed.add(result["file_id"])
                        total_rows += result["rows"]
                        total_bytes += result["size_bytes"]
                        save_checkpoint(config.checkpoint_file, completed)
                    else:
                        errors.append(result)

                    pbar.update(1)
                    pbar.set_postfix(
                        {"rows": f"{total_rows / 1e6:.1f}M", "size": f"{total_bytes / 1e9:.1f}GB"}
                    )

    # Upload the typology manifest sidecar (financial schema only).
    # Every pod's generator produces the same deterministic manifest bytes
    # for a given (seed, config), so idempotent overwrite to a stable key
    # is safe under K8s Indexed Job pod parallelism -- the last writer wins
    # and every writer wrote the same bytes.
    try:
        manifest_bytes_fn = getattr(generator, "manifest_bytes", None)
        if callable(manifest_bytes_fn):
            blob = manifest_bytes_fn()
            if blob:
                s3_client = get_s3_client(config)
                manifest_key = f"{config.prefix}/manifest/manifest.parquet"
                s3_client.put_object(
                    Bucket=config.bucket,
                    Key=manifest_key,
                    Body=blob,
                )
                print(
                    f"Uploaded typology manifest ({len(blob)} bytes) "
                    f"to s3://{config.bucket}/{manifest_key}"
                )
    except Exception as e:
        # Manifest is a debug/verification aid; do not fail the whole run.
        print(f"WARN: manifest upload failed: {e}")

    # Print summary
    print(f"""
Complete!
=========
Files generated: {len(completed):,}
Total rows: {total_rows:,}
Total size: {total_bytes / (1024**3):.2f} GB
Errors: {len(errors)}
""")

    if errors:
        print("Errors encountered:")
        for err in errors[:10]:
            print(f"  File {err['file_id']}: {err['error']}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")

    # Force clean process exit. Python 3.11+ interpreter shutdown after a
    # continuous-mode run occasionally hangs in the multiprocessing
    # atexit hook (`_exit_function`) even when every non-daemon thread
    # has already exited (verified via `threading.enumerate()` returning
    # only ['MainThread']). At that point every result has been printed,
    # every S3 upload has completed, checkpoint is saved, and there is
    # nothing more for the interpreter to do -- os._exit(0) skips the
    # atexit dance and returns rc=0 to the shell / kubelet, avoiding a
    # spurious SIGKILL after terminationGracePeriodSeconds. Failure code
    # 1 if there were errors so K8s Job status reflects reality.
    rc = 1 if errors else 0
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(rc)


if __name__ == "__main__":
    main()
