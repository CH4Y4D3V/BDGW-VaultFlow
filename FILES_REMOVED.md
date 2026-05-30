# Files Removed During Cleanup

| File Path | Reason for Removal | Replacement / Logic Location |
| :--- | :--- | :--- |
| `app/services/topic_service.py` | Deprecated stub. Replaced by `TopicManager`. | `app/services/topic_manager.py` |
| `app/services/topic_router.py` | Deprecated stub. Registering handlers here caused double-delivery. | `app/handlers/topic_router.py` |
