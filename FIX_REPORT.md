# Fix Report: Production Emergency Response

## Incident Summary
Four critical workflows were identified as broken following deployment: Support System, Payment Number Delivery, Payment Approval Flow, and the main `/start` UI. Additionally, business rules for rewards were incorrectly implemented.

## Issue 1: Support System Broken
- **Root Cause:** Callback handlers for the admin support card buttons (`reply`, `resolve`, `close`) were missing.
- **Fix:** Implemented `@Client.on_callback_query` handlers in `app/handlers/support_handler.py`.
- **Functions Changed:** `handle_support_reply_callback`, `handle_support_closure_callback`.

## Issue 2: Payment Number Delivery Broken
- **Root Cause:** A handler conflict in `topic_router.py` was intercepting admin manual replies before the specific payment handler could execute, preventing the session status from advancing.
- **Fix:** Integrated payment state transition logic directly into `route_admin_reply_to_user` in `app/handlers/topic_router.py`. Removed the redundant `handle_admin_manual_details_reply` in `app/payments/handlers.py`.
- **Functions Changed:** `route_admin_reply_to_user` (updated), `handle_admin_manual_details_reply` (removed).

## Issue 3: Payment Approval Flow Broken
- **Root Cause:** `approve_payment` expected the status to be `PROCESSING`, but the session was in `UNDER_REVIEW` when approved via the admin card.
- **Fix:** Modified `approve_payment` to accept `UNDER_REVIEW` and transition it to `PROCESSING` internally.
- **Functions Changed:** `PaymentService.approve_payment`.

## Issue 4: /start UI Broken
- **Root Cause:** The main menu keyboard did not match the required professional production layout.
- **Fix:** Updated `KeyboardBuilder.build_main_menu` to the exact 4-row layout. Added the Referral Program button to the User Status card to maintain accessibility.
- **Functions Changed:** `KeyboardBuilder.build_main_menu`, `build_user_status_card`.

## Business Rules Corrected
- **Referrals:** Changed reward from 10 points to 1 point per qualified referral.
- **Content Rewards:** Changed reward frequency from every 5th approved album to every 2nd approved album.

## Cleanup
- Deprecated `topic_service.py` and `topic_router.py` removed.
- Imports updated to use `TopicManager`.
- Environment variables synchronized and pruned.
