
# BDGW VaultFlow Engineering Rules

## Project Type

Large-scale Telegram automation infrastructure.

## Stack

- Python 3.12+
- Pyrogram
- MongoDB + Motor
- APScheduler
- Structlog
- Pydantic v2

---

# Core Engineering Principles

- Production-first
- Async-first
- Restart-safe
- FloodWait-safe
- Modular architecture only
- Explicit dependency boundaries
- No monolithic files
- Strong observability
- Idempotent workers

---

# Dependency Direction

config
→ database
→ repositories
→ services
→ workers

Never reverse dependency direction.

Workers must not import repositories directly unless explicitly justified.

---

# Telegram Infrastructure Rules

- Never trust Telegram delivery ordering.
- Media groups must support partial arrival and timeout buffering.
- All outbound sends must support FloodWait retry handling.
- Never assume Telegram message IDs are globally unique.
- Never assume media_group_id completeness.
- Protected content must be handled safely.
- All Telegram operations must be retry-safe.
- Album ordering must be deterministic.
- Duplicate repost prevention is mandatory.

---

# Queue System Rules

- Queue operations must be idempotent.
- Workers must support restart recovery.
- Stale locks must expire automatically.
- Jobs must support retry counters.
- Dead-letter queue support is mandatory.
- Duplicate delivery prevention is mandatory.
- Queue workers must never busy-loop.
- Every queue operation must be observable via logs.
- Queue processing must support graceful shutdown.

---

# APScheduler Rules

- APScheduler jobs must never contain business logic directly.
- Scheduled jobs must delegate into services/workers.
- Scheduler recovery after restart must be deterministic.
- Misfire handling must be explicitly configured.
- Long-running jobs must never block scheduler thread.
- Scheduler jobs must be idempotent.

---

# FFmpeg Rules

- FFmpeg subprocesses must always use timeouts.
- Zombie FFmpeg processes must be prevented.
- Temporary files must always be cleaned up.
- Watermark workers must support concurrency limits.
- Video processing must support cancellation handling.
- Video processing must support retry handling.
- FFmpeg stderr/stdout must be captured for debugging.

---

# MongoDB Rules

- Every high-volume collection must define indexes.
- TTL indexes must be used for temporary collections.
- Repositories must contain all DB logic.
- Services must never directly access Mongo collections.
- Large collections must avoid full scans.
- Atomic update patterns are preferred.
- Collection growth must be considered during design.

---

# Worker Lifecycle Rules

- Workers must support graceful shutdown.
- Workers must support restart recovery.
- Heartbeat monitoring is mandatory.
- Long-running loops must include cancellation handling.
- asyncio.create_task usage must be tracked and bounded.
- Workers must not leak background tasks.

---

# Logging Rules

- Use structured JSON logging only.
- Every worker should include correlation IDs where possible.
- Exceptions must always include contextual metadata.
- Silent exception swallowing is forbidden.
- Logging must support production observability.

---

# Architecture Rules

- Business logic belongs in services.
- Repositories handle persistence only.
- Handlers must stay thin.
- Circular imports are forbidden.
- Wildcard imports are forbidden.
- Relative imports should be avoided.
- Domain boundaries must remain explicit.
- Shared abstractions must remain centralized.

---

# Reliability Rules

- All critical operations must be retry-safe.
- Idempotency is mandatory for distribution operations.
- Duplicate media prevention is mandatory.
- Vault archival must be immutable.
- Failure recovery must be explicit.
- Restart recovery must be deterministic.

---

# Forbidden Patterns

## BAD: Global Mongo Clients

```python
client = AsyncIOMotorClient(...)
```

## BAD: Fire-and-forget tasks

```python
asyncio.create_task(...)
```

without lifecycle tracking.

## BAD: Blocking subprocesses

```python
subprocess.run(...)
```

inside async execution paths.

## BAD: Direct DB access inside handlers

Handlers must delegate into services.

## BAD: Unbounded in-memory queues

All queues must support bounded flow control.

## BAD: Giant files

Forbidden:

* giant models.py
* giant utils.py
* giant services.py
* giant handlers.py

---

# Repository Goals

The system must safely support:

* large Telegram vaults
* automated scheduling
* premium subscriptions
* NSFW routing
* watermark processing
* worker orchestration
* restart recovery
* FloodWait resilience
* scalable MongoDB usage
* safe async execution
* long-term maintainability
