#!/usr/bin/env python3
"""
Datagen v2: Synthetic Customer 360 Data Generator

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
import os
import sys
import threading
import uuid
from datetime import datetime
from multiprocessing import Process
from multiprocessing import Queue as MPQueue

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig
from tqdm import tqdm

# =============================================================================
# Continuous Mode Constants
# =============================================================================

NUM_GENERATORS = 8  # Number of generator processes
NUM_UPLOADERS = 2  # Number of uploader threads
QUEUE_DEPTH = 8  # Max items in upload queue (backpressure)

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

        # Calculated values
        self.target_bytes = int(target_tb * 1024 * 1024 * 1024 * 1024)
        self.file_size_bytes = file_size_mb * 1024 * 1024
        self.total_files = max(1, int(self.target_bytes / self.file_size_bytes))

        # Estimate rows per file to achieve target compressed file size
        compressed_bytes_per_row = 4200  # ~4.1KB per row after snappy compression
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
        kwargs["verify"] = False  # For self-signed certs

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


# Global loyalty lookup (built once, shared across files)
_LOYALTY_MEMBER = None
_LOYALTY_TIER = None


def get_loyalty_lookup(seed: int, customer_id_max: int):
    """Get or build the global loyalty lookup."""
    global _LOYALTY_MEMBER, _LOYALTY_TIER
    if _LOYALTY_MEMBER is None:
        _LOYALTY_MEMBER, _LOYALTY_TIER = build_loyalty_lookup(seed, customer_id_max)
    return _LOYALTY_MEMBER, _LOYALTY_TIER


# =============================================================================
# Data Generation Functions
# =============================================================================


def generate_uuids(rows: int, rng: np.random.Generator) -> list:
    """Generate UUID v4 strings."""
    return [str(uuid.UUID(bytes=rng.bytes(16), version=4)) for _ in range(rows)]


def generate_timestamps(rows: int, rng: np.random.Generator, config: Config) -> list:
    """Generate random timestamps within the configured range."""
    start_ts = config.timestamp_start.timestamp()
    end_ts = config.timestamp_end.timestamp()
    timestamps = rng.uniform(start_ts, end_ts, size=rows)
    return [datetime.fromtimestamp(ts) for ts in timestamps]


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
# File Generation
# =============================================================================


def generate_file_data(file_id: int, config: Config) -> pa.Table:
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
    loyalty_member_lookup, loyalty_tier_lookup = get_loyalty_lookup(
        config.seed, config.customer_id_max
    )
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
            ("event_timestamp", pa.timestamp("us")),
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


def write_file_to_s3(file_id: int, config: Config) -> dict:
    """Generate and write a single Parquet file to S3."""
    try:
        table = generate_file_data(file_id, config)

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        s3_client = get_s3_client(config)
        s3_key = f"{config.prefix}/part-{file_id:06d}.parquet"

        s3_client.upload_fileobj(buffer, config.bucket, s3_key)

        file_size = buffer.tell()

        return {
            "file_id": file_id,
            "success": True,
            "rows": table.num_rows,
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
    file_queue: MPQueue, upload_queue: MPQueue, config_dict: dict, generator_id: int
):
    """
    Generator process: pulls file_ids from file_queue, generates data,
    compresses to Parquet bytes, and pushes to upload_queue.
    """
    config = Config(**config_dict)

    # Rebuild loyalty lookup in child process
    get_loyalty_lookup(config.seed, config.customer_id_max)

    while True:
        try:
            file_id = file_queue.get(timeout=1)
        except Exception:
            if file_queue.empty():
                break
            continue

        if file_id is None:
            upload_queue.put(None)
            break

        try:
            table = generate_file_data(file_id, config)
            rows = table.num_rows

            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            parquet_bytes = buffer.getvalue()
            size_bytes = len(parquet_bytes)

            del table
            buffer.close()

            upload_queue.put((file_id, parquet_bytes, rows, size_bytes))

        except Exception as e:
            upload_queue.put((file_id, None, 0, 0, str(e)))


def _continuous_uploader_worker(
    upload_queue: MPQueue,
    poison_pills_expected: int,
    config: Config,
    results: list,
    results_lock: threading.Lock,
    progress_callback,
):
    """
    Uploader thread: pulls compressed data from upload_queue and uploads to S3.
    """
    s3_client = get_s3_client(config)
    pills_received = 0

    while pills_received < poison_pills_expected:
        try:
            item = upload_queue.get(timeout=1)
        except Exception:
            continue

        if item is None:
            pills_received += 1
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

        try:
            s3_client.upload_fileobj(io.BytesIO(parquet_bytes), config.bucket, s3_key)

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
    """
    Run continuous mode with multiprocessing generators and threading uploaders.
    """
    remaining = [f for f in my_files if f not in completed]

    if not remaining:
        return 0, 0, []

    file_queue = MPQueue()
    upload_queue = MPQueue(maxsize=QUEUE_DEPTH)

    for file_id in remaining:
        file_queue.put(file_id)

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
            args=(file_queue, upload_queue, config_dict, i),
            name=f"gen-{i}",
        )
        p.start()
        generators.append(p)

    pills_per_uploader = NUM_GENERATORS // NUM_UPLOADERS
    uploaders = []
    for i in range(NUM_UPLOADERS):
        t = threading.Thread(
            target=_continuous_uploader_worker,
            args=(
                upload_queue,
                pills_per_uploader,
                config,
                results,
                results_lock,
                progress_callback,
            ),
            name=f"upload-{i}",
        )
        t.start()
        uploaders.append(t)

    with tqdm(total=len(remaining), unit="files") as pbar:
        last_done = 0
        checkpoint_interval = 10

        while True:
            with progress_lock:
                current_done = files_done
                current_rows = total_rows
                current_bytes = total_bytes

            if current_done >= len(remaining):
                if current_done > last_done:
                    pbar.update(current_done - last_done)
                break

            if current_done > last_done:
                pbar.update(current_done - last_done)
                pbar.set_postfix(
                    {
                        "rows": f"{current_rows / 1e6:.1f}M",
                        "size": f"{current_bytes / 1e9:.1f}GB",
                        "queue": upload_queue.qsize(),
                    }
                )

                if current_done % checkpoint_interval == 0:
                    with results_lock:
                        for r in results:
                            if r["success"]:
                                completed.add(r["file_id"])
                    save_checkpoint(checkpoint_file, completed)

                last_done = current_done

            threading.Event().wait(0.5)

    for t in uploaders:
        t.join()

    for p in generators:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()

    with results_lock:
        for r in results:
            if r["success"]:
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

    args = parser.parse_args()

    # Get node_id from argument, environment variable (K8s Indexed Job), or default to 0
    if args.node_id is not None:
        node_id = args.node_id
    else:
        node_id = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))

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
    )

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
        mode_desc = f"continuous ({NUM_GENERATORS} generators, {NUM_UPLOADERS} uploaders)"
    else:
        mode_desc = f"sequential ({config.workers} workers)"
    print(f"""
Datagen v2.0 (realism features enabled)
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

    if not remaining:
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

    # Build loyalty lookup before forking child processes
    get_loyalty_lookup(config.seed, config.customer_id_max)

    # Generate files
    total_rows = 0
    total_bytes = 0
    errors = []

    print(f"\nGenerating {len(remaining):,} files...")

    if args.mode == "continuous":
        total_rows, total_bytes, all_results = run_continuous(
            config, my_files, completed, config.checkpoint_file
        )
        errors = [r for r in all_results if not r["success"]]
        for r in all_results:
            if r["success"]:
                completed.add(r["file_id"])
        save_checkpoint(config.checkpoint_file, completed)
    else:
        with ProcessPoolExecutor(max_workers=config.workers) as executor:
            futures = {
                executor.submit(write_file_to_s3, file_id, config): file_id for file_id in remaining
            }

            with tqdm(total=len(remaining), unit="files") as pbar:
                for future in as_completed(futures):
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


if __name__ == "__main__":
    main()
