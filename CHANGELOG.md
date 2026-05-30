# Changelog

## [1.2.0] - 2026-05-30
### Fixed
- **Support System:** Added missing callback handlers for admin actions (reply, resolve, close).
- **Payment Delivery:** Fixed conflict between `topic_router` and payment handlers. Admin replies now correctly trigger state transitions.
- **Payment Approval:** Fixed state mismatch bug where approval failed because session was not in `PROCESSING` state.
- **Start UI:** Restored professional production menu layout.

### Changed
- **Referral Rewards:** Reduced reward from 10 points to 1 point per referral.
- **Content Rewards:** Increased reward frequency to 1 point per 2 approved albums (was 5).
- **Topic Management:** Migrated all handlers and services to `TopicManager`.
- **Environment:** Pruned unused variables and synchronized `.env.example` with `settings.py`.

### Removed
- `app/services/topic_service.py` (deprecated stub)
- `app/services/topic_router.py` (deprecated stub)
