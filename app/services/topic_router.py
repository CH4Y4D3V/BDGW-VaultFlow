from __future__ import annotations

"""
app/services/topic_router.py

IMPORTANT: This file intentionally contains NO Pyrogram handler registrations.

The admin → user reply routing handler (`route_admin_reply_to_user`) lives
exclusively in `app/handlers/topic_router.py`.

Previously this file contained an exact duplicate of the handler, which caused
every admin reply in a support/payment topic to be delivered to the user TWICE —
once from each registered `@Client.on_message(filters.chat(VERIFICATION_GROUP_ID))`
decorator. Pyrogram registers decorators at import time, so both fired on every
admin reply.

B-01 FIX: Handler removed. This file is kept as a stub so any existing imports
of `app.services.topic_router` do not break, but it registers nothing.

If you need the routing logic, import from `app.handlers.topic_router`.
"""