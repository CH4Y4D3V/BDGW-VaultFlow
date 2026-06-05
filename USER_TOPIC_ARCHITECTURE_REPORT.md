# BDGW VaultFlow User-Centric Topic Architecture Report
Date: Friday, June 5, 2026
Status: IMPLEMENTED & VALIDATED

## 1. Architectural Redesign Overview
The Verification Hub has been transitioned from a fragmented category-based topic system (Support, Payments, Content) to a **User-Centric Architecture**. Every user now has exactly ONE permanent forum topic in the Hub chat that serves as their complete interaction history.

## 2. Database Schema Changes
### `user_topics` Collection
- **Old Schema**: `user_id`, `topic_type`, `topic_id`
- **New Schema**:
  ```json
  {
    "_id": "ObjectId",
    "user_id": 7535201490,
    "topic_id": 127,
    "topic_name": "👤 Rahat Ferdous | 7535201490",
    "status": "active|accepted|closed|pending",
    "created_at": "datetime",
    "last_activity_at": "datetime",
    "accepted_by": "int|null",
    "accepted_at": "datetime|null"
  }
  ```
- **Indexes**:
  - `user_id`: UNIQUE
  - `topic_id`: UNIQUE

## 3. Topic Creation & Header
- **Format**: `👤 {Full Name} | {User ID}`
- **Pinned Header**: Automatically sent and pinned upon creation. Contains:
  - User details (Name, Username, ID)
  - Subscription status
  - Moderation stats (Warnings, Mute/Ban status)
  - List of available admin commands.

## 4. Admin Command System (Inside Topics)
Commands now work contextually within user topics without requiring the `user_id` as an argument (reverse lookup from `topic_id`).

### Implemented Commands:
- `/accept`: Marks session as accepted.
- `/close`: Closes active support session.
- `/ban` / `/unban`: Global user ban management.
- `/warn`: Logs a user warning.
- `/mute` / `/unmute`: Controls user's ability to send DMs.
- `/paymentdone`: Shortcut to approve active payment session.
- `/profile`: Detailed user summary.
- `/history`: Audit trail for the user.
- `/payments`: Payment history list.
- `/note <text>` / `/notes`: Staff-only private notes persisted in DB.

## 5. Unified Routing Logic
- **`TopicManager`**: The single source of truth for user-topic mappings.
- **`TopicRouter`**: Deliveries admin replies from the user thread directly to the user's private DM, excluding commands and moderation cards.
- **Service Integration**: `SupportService`, `PaymentService`, `SubmissionService`, and `TakedownService` all utilize the unified topic for notifications and interactions.

## 6. Restart Safety & Migration
- **Persistence**: MongoDB is the primary registry. The bot restores the topic cache from DB on startup.
- **Redundant Paths Removed**: `app/payments/topics.py` was deleted; all topic creation flows consolidated into `TopicManager`.
- **Legacy Compatibility**: `TopicService` shim maintained to prevent breaking existing imports while refactoring.

## 7. Files Modified
- `app/services/topic_manager.py`
- `app/services/topic_service.py`
- `app/services/support_service.py`
- `app/handlers/topic_router.py`
- `app/handlers/admin_handler.py`
- `app/handlers/support_handler.py`
- `app/handlers/submission_handler.py`
- `app/handlers/payment_handler.py`
- `app/handlers/takedown_handler.py`
- `app/moderation/moderation_actions.py`
- `app/payments/service.py`
- `app/handlers/group_handler.py`
- `app/services/takedown_service.py`

---
*Architectural Redesign by Principal Engineer (Gemini CLI)*
