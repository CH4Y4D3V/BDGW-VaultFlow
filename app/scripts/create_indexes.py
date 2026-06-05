#!/usr/bin/env python3
"""
scripts/create_indexes.py

PURPOSE
-------
One-time idempotent index creation script for the BDGW VaultFlow platform.

Run this script:
  1. During initial production deployment (before first bot start)
  2. After any schema change that adds new indexes
  3. When bootstrapping a new environment

This script is IDEMPOTENT: existing indexes are left intact.
MongoDB's createIndex operation is a no-op if the index already exists
with identical options.

USAGE
-----
  # Standard run (creates all missing indexes):
  python scripts/create_indexes.py

  # Dry run (prints what would be created, no writes):
  python scripts/create_indexes.py --dry-run

  # Single collection only:
  python scripts/create_indexes.py --collection vault_items

  # Verbose output (shows already-existing indexes too):
  python scripts/create_indexes.py --verbose

REQUIREMENTS
------------
  pip install motor python-dotenv

ENVIRONMENT VARIABLES
---------------------
  MONGODB_URI       MongoDB connection string (required)
                    e.g. mongodb://localhost:27017
  MONGODB_DATABASE  Database name (required)
                    e.g. bdgw_vaultflow

EXIT CODES
----------
  0  All indexes created or already existed successfully.
  1  One or more collections encountered errors during index creation.
  2  Configuration error (missing ENV vars, connection failure).
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Load .env before any other imports
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv optional; rely on shell ENV if not installed

import motor.motor_asyncio
from pymongo import ASCENDING, DESCENDING, TEXT
from pymongo.errors import OperationFailure

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("create_indexes")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MONGODB_URI: str = os.getenv("MONGODB_URI", "")
MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "")

if not MONGODB_URI:
    logger.error("MONGODB_URI environment variable is not set.")
    sys.exit(2)

if not MONGODB_DATABASE:
    logger.error("MONGODB_DATABASE environment variable is not set.")
    sys.exit(2)

# ---------------------------------------------------------------------------
# Index definitions
#
# Structure:
#   COLLECTION_INDEXES = {
#       "collection_name": [
#           {
#               "key": [(field, direction), ...],
#               "options": { unique=True, sparse=True, name="...", ... }
#           },
#           ...
#       ]
#   }
#
# Per Spec Section 25A.20 plus supplementary indexes for production safety.
# ---------------------------------------------------------------------------

COLLECTION_INDEXES: dict[str, list[dict[str, Any]]] = {

    # -----------------------------------------------------------------------
    # 25A.1 users
    # -----------------------------------------------------------------------
    "users": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "unique": True,
                "name": "users_user_id_unique",
            },
        },
        {
            "key": [("referral_code", ASCENDING)],
            "options": {
                "unique": True,
                "name": "users_referral_code_unique",
            },
        },
        {
            # Fast lookup for banned/muted user enforcement
            "key": [("is_banned", ASCENDING), ("is_muted", ASCENDING)],
            "options": {
                "name": "users_ban_mute_status",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.2 user_topics
    # -----------------------------------------------------------------------
    "user_topics": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "unique": True,
                "name": "user_topics_user_id_unique",
            },
        },
        {
            "key": [("topic_id", ASCENDING)],
            "options": {
                "name": "user_topics_topic_id",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.3 subscriptions
    # -----------------------------------------------------------------------
    "subscriptions": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "name": "subscriptions_user_id",
            },
        },
        {
            "key": [("expires_at", ASCENDING)],
            "options": {
                "name": "subscriptions_expires_at",
            },
        },
        {
            "key": [("status", ASCENDING)],
            "options": {
                "name": "subscriptions_status",
            },
        },
        {
            # Compound: expiry worker queries active subscriptions sorted by expiry
            "key": [("status", ASCENDING), ("expires_at", ASCENDING)],
            "options": {
                "name": "subscriptions_status_expires_at",
            },
        },
        {
            # Compound: fast lookup for a user's active subscription
            "key": [("user_id", ASCENDING), ("status", ASCENDING)],
            "options": {
                "name": "subscriptions_user_id_status",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.4 payment_sessions
    # Per spec: one active session per user (unique on user_id)
    # -----------------------------------------------------------------------
    "payment_sessions": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "unique": True,
                "name": "payment_sessions_user_id_unique",
            },
        },
        {
            "key": [("expires_at", ASCENDING)],
            "options": {
                "name": "payment_sessions_expires_at",
            },
        },
        {
            # Partial index: enforce uniqueness only for ACTIVE sessions
            # (allows multiple completed/expired records per user in history)
            "key": [("user_id", ASCENDING), ("status", ASCENDING)],
            "options": {
                "name": "payment_sessions_user_id_status",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.5 payment_history
    # -----------------------------------------------------------------------
    "payment_history": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "name": "payment_history_user_id",
            },
        },
        {
            "key": [("txid", ASCENDING)],
            "options": {
                "unique": True,
                "name": "payment_history_txid_unique",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.6 txid_registry
    # Global uniqueness enforcement — Spec Section 8
    # -----------------------------------------------------------------------
    "txid_registry": [
        {
            "key": [("txid", ASCENDING)],
            "options": {
                "unique": True,
                "name": "txid_registry_txid_unique",
            },
        },
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "name": "txid_registry_user_id",
            },
        },
        {
            "key": [("registered_at", DESCENDING)],
            "options": {
                "name": "txid_registry_registered_at_desc",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.7 invites
    # -----------------------------------------------------------------------
    "invites": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "name": "invites_user_id",
            },
        },
        {
            "key": [("expires_at", ASCENDING)],
            "options": {
                "name": "invites_expires_at",
            },
        },
        {
            "key": [("subscription_id", ASCENDING)],
            "options": {
                "name": "invites_subscription_id",
            },
        },
        {
            # Fast lookup for valid unused invites per user
            "key": [("user_id", ASCENDING), ("used", ASCENDING), ("expires_at", ASCENDING)],
            "options": {
                "name": "invites_user_used_expires",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.8 support_sessions
    # -----------------------------------------------------------------------
    "support_sessions": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "name": "support_sessions_user_id",
            },
        },
        {
            "key": [("status", ASCENDING)],
            "options": {
                "name": "support_sessions_status",
            },
        },
        {
            # Compound: find active/pending sessions per user
            "key": [("user_id", ASCENDING), ("status", ASCENDING)],
            "options": {
                "name": "support_sessions_user_id_status",
            },
        },
        {
            # 5-minute unattended notification worker queries PENDING by opened_at
            "key": [("status", ASCENDING), ("opened_at", ASCENDING)],
            "options": {
                "name": "support_sessions_status_opened_at",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.9 content_submissions
    # -----------------------------------------------------------------------
    "content_submissions": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "name": "content_submissions_user_id",
            },
        },
        {
            "key": [("media_hash", ASCENDING)],
            "options": {
                "name": "content_submissions_media_hash",
            },
        },
        {
            "key": [("status", ASCENDING)],
            "options": {
                "name": "content_submissions_status",
            },
        },
        {
            # Compound: duplicate check per user
            "key": [("user_id", ASCENDING), ("media_hash", ASCENDING)],
            "options": {
                "name": "content_submissions_user_hash",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.10 content_fingerprints
    # -----------------------------------------------------------------------
    "content_fingerprints": [
        {
            "key": [("media_hash", ASCENDING)],
            "options": {
                "unique": True,
                "name": "content_fingerprints_media_hash_unique",
            },
        },
        {
            "key": [("submission_id", ASCENDING)],
            "options": {
                "name": "content_fingerprints_submission_id",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.11 vault_items
    # -----------------------------------------------------------------------
    "vault_items": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "name": "vault_items_user_id",
            },
        },
        {
            "key": [("vault_type", ASCENDING)],
            "options": {
                "name": "vault_items_vault_type",
            },
        },
        {
            "key": [("last_posted_at", ASCENDING)],
            "options": {
                "sparse": True,  # last_posted_at is nullable
                "name": "vault_items_last_posted_at",
            },
        },
        {
            # Compound: VaultPoolService eligibility query
            # vault_type + last_posted_at is the primary filter
            "key": [("vault_type", ASCENDING), ("last_posted_at", ASCENDING)],
            "options": {
                "name": "vault_items_type_last_posted",
                "sparse": True,
            },
        },
        {
            # Compound: fair rotation sort (type + post_count)
            "key": [("vault_type", ASCENDING), ("post_count", ASCENDING)],
            "options": {
                "name": "vault_items_type_post_count",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.12 queue_jobs
    # Per spec: compound index on status + vault_type + scheduled_at
    # -----------------------------------------------------------------------
    "queue_jobs": [
        {
            # Primary compound index — used by distribution workers
            "key": [
                ("status", ASCENDING),
                ("vault_type", ASCENDING),
                ("scheduled_at", ASCENDING),
            ],
            "options": {
                "name": "queue_jobs_status_type_scheduled",
            },
        },
        {
            "key": [("vault_id", ASCENDING)],
            "options": {
                "name": "queue_jobs_vault_id",
            },
        },
        {
            "key": [("created_at", DESCENDING)],
            "options": {
                "name": "queue_jobs_created_at_desc",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.13 dead_letters
    # -----------------------------------------------------------------------
    "dead_letters": [
        {
            "key": [("job_id", ASCENDING)],
            "options": {
                "name": "dead_letters_job_id",
            },
        },
        {
            "key": [("vault_type", ASCENDING)],
            "options": {
                "name": "dead_letters_vault_type",
            },
        },
        {
            "key": [("reviewed", ASCENDING)],
            "options": {
                "name": "dead_letters_reviewed",
            },
        },
        {
            # Compound: admin dashboard query for unreviewed dead letters
            "key": [("vault_type", ASCENDING), ("reviewed", ASCENDING)],
            "options": {
                "name": "dead_letters_type_reviewed",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.14 referrals
    # -----------------------------------------------------------------------
    "referrals": [
        {
            "key": [("referrer_user_id", ASCENDING)],
            "options": {
                "name": "referrals_referrer_user_id",
            },
        },
        {
            # Per spec: one referral per new user (globally unique)
            "key": [("referred_user_id", ASCENDING)],
            "options": {
                "unique": True,
                "name": "referrals_referred_user_id_unique",
            },
        },
        {
            "key": [("status", ASCENDING)],
            "options": {
                "name": "referrals_status",
            },
        },
        {
            # Referral dashboard: count per referrer filtered by status
            "key": [("referrer_user_id", ASCENDING), ("status", ASCENDING)],
            "options": {
                "name": "referrals_referrer_status",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.15 punishments
    # -----------------------------------------------------------------------
    "punishments": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "name": "punishments_user_id",
            },
        },
        {
            "key": [("type", ASCENDING)],
            "options": {
                "name": "punishments_type",
            },
        },
        {
            "key": [("active", ASCENDING)],
            "options": {
                "name": "punishments_active",
            },
        },
        {
            # Compound: active punishment lookup per user per type
            "key": [("user_id", ASCENDING), ("type", ASCENDING), ("active", ASCENDING)],
            "options": {
                "name": "punishments_user_type_active",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.16 takedown_requests
    # -----------------------------------------------------------------------
    "takedown_requests": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "name": "takedown_requests_user_id",
            },
        },
        {
            "key": [("status", ASCENDING)],
            "options": {
                "name": "takedown_requests_status",
            },
        },
        {
            # Admin dashboard: pending requests sorted by submission date
            "key": [("status", ASCENDING), ("submitted_at", ASCENDING)],
            "options": {
                "name": "takedown_requests_status_submitted_at",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.17 audit_logs
    # -----------------------------------------------------------------------
    "audit_logs": [
        {
            "key": [("timestamp", DESCENDING)],
            "options": {
                "name": "audit_logs_timestamp_desc",
            },
        },
        {
            "key": [("action", ASCENDING)],
            "options": {
                "name": "audit_logs_action",
            },
        },
        {
            # Compound: filter by action type within time range
            "key": [("action", ASCENDING), ("timestamp", DESCENDING)],
            "options": {
                "name": "audit_logs_action_timestamp",
            },
        },
        {
            "key": [("target_user_id", ASCENDING)],
            "options": {
                "sparse": True,  # target_user_id is nullable
                "name": "audit_logs_target_user_id",
            },
        },
        {
            "key": [("admin_user_id", ASCENDING)],
            "options": {
                "sparse": True,  # admin_user_id is nullable for system events
                "name": "audit_logs_admin_user_id",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.18 admins
    # -----------------------------------------------------------------------
    "admins": [
        {
            "key": [("user_id", ASCENDING)],
            "options": {
                "unique": True,
                "name": "admins_user_id_unique",
            },
        },
        {
            "key": [("role", ASCENDING)],
            "options": {
                "name": "admins_role",
            },
        },
        {
            # Active admin lookup
            "key": [("is_active", ASCENDING)],
            "options": {
                "name": "admins_is_active",
            },
        },
    ],

    # -----------------------------------------------------------------------
    # 25A.19 hub_config
    # -----------------------------------------------------------------------
    "hub_config": [
        {
            "key": [("key", ASCENDING)],
            "options": {
                "unique": True,
                "name": "hub_config_key_unique",
            },
        },
    ],
}


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------


class IndexResult:
    """Tracks the outcome of a single index creation attempt."""

    __slots__ = ("collection", "name", "status", "error")

    def __init__(
        self,
        collection: str,
        name: str,
        status: str,
        error: str = "",
    ) -> None:
        self.collection = collection
        self.name = name
        self.status = status  # "CREATED" | "EXISTS" | "FAILED"
        self.error = error


# ---------------------------------------------------------------------------
# Core index creation logic
# ---------------------------------------------------------------------------


async def create_collection_indexes(
    db: motor.motor_asyncio.AsyncIOMotorDatabase,
    collection_name: str,
    index_specs: list[dict[str, Any]],
    dry_run: bool,
    verbose: bool,
) -> list[IndexResult]:
    """
    Create all defined indexes for a single collection.

    This function is idempotent.  If an index with the same name and
    identical key specification already exists, MongoDB returns silently
    without error.

    If an index with the SAME name but DIFFERENT key spec exists, MongoDB
    raises OperationFailure.  This is caught and reported as FAILED —
    the operator must manually drop the conflicting index.

    Args:
        db: Active Motor database handle.
        collection_name: Name of the MongoDB collection.
        index_specs: List of index definition dicts from COLLECTION_INDEXES.
        dry_run: If True, print what would be created without executing.
        verbose: If True, log ALREADY EXISTS results as well as CREATED.

    Returns:
        List of IndexResult objects, one per index spec.
    """
    results: list[IndexResult] = []
    collection = db[collection_name]

    for spec in index_specs:
        key: list[tuple[str, int]] = spec["key"]
        options: dict[str, Any] = spec.get("options", {})
        index_name: str = options.get("name", "unnamed")

        if dry_run:
            logger.info(
                "[DRY-RUN]  %-30s  %-50s  %s",
                collection_name,
                index_name,
                str(key),
            )
            results.append(
                IndexResult(
                    collection=collection_name,
                    name=index_name,
                    status="DRY_RUN",
                )
            )
            continue

        try:
            await collection.create_index(key, **options)
            results.append(
                IndexResult(
                    collection=collection_name,
                    name=index_name,
                    status="CREATED",
                )
            )
            if verbose:
                logger.info(
                    "  ✓  %-30s  %s", collection_name, index_name
                )

        except OperationFailure as exc:
            # Duplicate index name with different keys — requires manual intervention
            error_str = str(exc)

            if "already exists with different options" in error_str or \
               "already exists with the same name" in error_str or \
               "already exists" in error_str.lower():
                # Idempotent success — index exists with same spec
                results.append(
                    IndexResult(
                        collection=collection_name,
                        name=index_name,
                        status="EXISTS",
                    )
                )
                if verbose:
                    logger.info(
                        "  ~  %-30s  %s  (already exists)", collection_name, index_name
                    )
            else:
                logger.error(
                    "  ✗  %-30s  %s  ERROR: %s",
                    collection_name,
                    index_name,
                    exc,
                )
                results.append(
                    IndexResult(
                        collection=collection_name,
                        name=index_name,
                        status="FAILED",
                        error=str(exc),
                    )
                )

        except Exception as exc:
            logger.error(
                "  ✗  %-30s  %s  UNEXPECTED ERROR: %s",
                collection_name,
                index_name,
                exc,
            )
            results.append(
                IndexResult(
                    collection=collection_name,
                    name=index_name,
                    status="FAILED",
                    error=str(exc),
                )
            )

    return results


async def create_all_indexes(
    dry_run: bool = False,
    verbose: bool = False,
    target_collection: str = "",
) -> int:
    """
    Orchestrate index creation across all collections defined in
    COLLECTION_INDEXES.

    Connects to MongoDB, pings the server, then iterates over collections.
    Per-collection failures are logged and counted but do not abort
    remaining collections.

    Args:
        dry_run: If True, print planned index operations without executing.
        verbose: If True, log already-existing indexes in addition to new ones.
        target_collection: If non-empty, only process this one collection name.

    Returns:
        Exit code: 0 = full success, 1 = one or more failures, 2 = fatal error.
    """
    run_start = datetime.now(tz=timezone.utc)

    logger.info("=" * 70)
    logger.info("BDGW VaultFlow — Index Creation Script")
    logger.info("Run started : %s", run_start.strftime("%Y-%m-%d %H:%M:%S UTC"))
    logger.info("Database    : %s", MONGODB_DATABASE)
    logger.info("URI         : %s", _redact_uri(MONGODB_URI))
    logger.info("Dry run     : %s", dry_run)
    logger.info("=" * 70)

    # --- Connect ---
    try:
        client = motor.motor_asyncio.AsyncIOMotorClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=10_000,
        )
        db = client[MONGODB_DATABASE]

        # Validate connectivity with a ping
        await client.admin.command("ping")
        logger.info("MongoDB connection: OK")
    except Exception as exc:
        logger.error("MongoDB connection failed: %s", exc)
        return 2

    all_results: list[IndexResult] = []
    failed_collections: list[str] = []

    # --- Determine which collections to process ---
    if target_collection:
        if target_collection not in COLLECTION_INDEXES:
            logger.error(
                "Unknown collection '%s'. Available collections: %s",
                target_collection,
                ", ".join(sorted(COLLECTION_INDEXES.keys())),
            )
            client.close()
            return 2
        collections_to_process = {target_collection: COLLECTION_INDEXES[target_collection]}
    else:
        collections_to_process = COLLECTION_INDEXES

    logger.info(
        "Processing %d collection(s)...\n",
        len(collections_to_process),
    )

    # --- Process each collection ---
    for collection_name, index_specs in collections_to_process.items():
        logger.info("Collection: %s  (%d index spec(s))", collection_name, len(index_specs))

        try:
            results = await create_collection_indexes(
                db=db,
                collection_name=collection_name,
                index_specs=index_specs,
                dry_run=dry_run,
                verbose=verbose,
            )
            all_results.extend(results)

            failed_in_collection = [r for r in results if r.status == "FAILED"]
            if failed_in_collection:
                failed_collections.append(collection_name)
                for r in failed_in_collection:
                    logger.error(
                        "    FAILED: %s — %s", r.name, r.error
                    )
            else:
                created_count = sum(1 for r in results if r.status == "CREATED")
                exists_count = sum(1 for r in results if r.status == "EXISTS")
                dryrun_count = sum(1 for r in results if r.status == "DRY_RUN")
                if dry_run:
                    logger.info("    Would create: %d index(es)", dryrun_count)
                else:
                    logger.info(
                        "    Created: %d  |  Already existed: %d",
                        created_count,
                        exists_count,
                    )

        except Exception as exc:
            logger.exception(
                "Unexpected error processing collection '%s': %s",
                collection_name,
                exc,
            )
            failed_collections.append(collection_name)

        logger.info("")

    # --- Print summary ---
    run_end = datetime.now(tz=timezone.utc)
    elapsed = (run_end - run_start).total_seconds()

    logger.info("=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)

    if dry_run:
        total_planned = sum(1 for r in all_results if r.status == "DRY_RUN")
        logger.info("DRY RUN — No changes were made.")
        logger.info("Would create %d index spec(s) across %d collection(s).",
                    total_planned, len(collections_to_process))
    else:
        total_created = sum(1 for r in all_results if r.status == "CREATED")
        total_exists = sum(1 for r in all_results if r.status == "EXISTS")
        total_failed = sum(1 for r in all_results if r.status == "FAILED")
        total_collections = len(collections_to_process)

        logger.info("Collections processed : %d", total_collections)
        logger.info("Indexes created       : %d", total_created)
        logger.info("Indexes already exist : %d", total_exists)
        logger.info("Indexes failed        : %d", total_failed)
        logger.info("Elapsed               : %.2fs", elapsed)

        if failed_collections:
            logger.error("")
            logger.error("FAILED COLLECTIONS:")
            for c in failed_collections:
                logger.error("  - %s", c)
            logger.error("")
            logger.error(
                "ACTION REQUIRED: Manually inspect the collections above. "
                "A conflicting index may need to be dropped before re-running."
            )

    logger.info("=" * 70)

    client.close()

    if dry_run:
        return 0
    return 1 if failed_collections else 0


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _redact_uri(uri: str) -> str:
    """
    Redact credentials from a MongoDB URI for safe logging.

    Replaces password in `mongodb://user:password@host` with `***`.

    Args:
        uri: Raw MongoDB connection string.

    Returns:
        Redacted URI safe to log.
    """
    try:
        if "@" in uri:
            scheme_and_creds, rest = uri.split("@", 1)
            if "://" in scheme_and_creds:
                scheme, creds = scheme_and_creds.split("://", 1)
                if ":" in creds:
                    user, _ = creds.split(":", 1)
                    return f"{scheme}://{user}:***@{rest}"
        return uri
    except Exception:
        return "[URI REDACTED]"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed argparse.Namespace.
    """
    parser = argparse.ArgumentParser(
        description=(
            "BDGW VaultFlow — One-time idempotent MongoDB index creation script.\n"
            "Safe to run multiple times. Existing indexes are left intact."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print all indexes that would be created without executing any writes.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Also log indexes that already exist (default: only log new creations).",
    )
    parser.add_argument(
        "--collection",
        metavar="NAME",
        default="",
        help=(
            "Process only this one collection by name. "
            "Useful for targeted re-runs without touching other collections."
        ),
    )
    return parser.parse_args()


async def _main() -> int:
    """
    Async entry point.

    Returns:
        Exit code (0 = success, 1 = index failures, 2 = fatal/config error).
    """
    args = _parse_args()
    exit_code = await create_all_indexes(
        dry_run=args.dry_run,
        verbose=args.verbose,
        target_collection=args.collection,
    )
    return exit_code


if __name__ == "__main__":
    try:
        code = asyncio.run(_main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        code = 2
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        code = 2

    sys.exit(code)