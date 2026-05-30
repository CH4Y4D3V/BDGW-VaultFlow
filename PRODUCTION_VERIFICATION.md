# Production Verification Guide (Master Audit Pass)

## 1. Onboarding & Registration (System 1)
- [ ] Run `/start`.
- [ ] Verify onboarding message appears (only for first-time users).
- [ ] Verify user is registered in MongoDB `users` collection with correct metadata.

## 2. Main Menu (System 2 & 3)
- [ ] Verify 4-row layout:
  1. 💎 Premium Access
  2. 📤 Submit Content Anonymously
  3. 🎁 Referral Program | 📊 My Status
  4. 🆘 Need Help
- [ ] Verify slash commands in Bot Menu: `/start`, `/takedown`, `/help`.

## 3. Auto-Delete (System 4)
- [ ] Send any message starting with `./`.
- [ ] Verify it is deleted automatically after 10 seconds.

## 4. Payments (System 5, 7, 14)
- [ ] Initiate payment.
- [ ] Verify reward points are snapshotted and locked (deducted from balance).
- [ ] Let session expire (set timeout to 1 min for test).
- [ ] Verify points are REFUNDED to wallet after expiration.

## 5. Broadcast (System 9)
- [ ] Run `/broadcast` in Verification Hub.
- [ ] Send text/photo/album.
- [ ] Preview and Confirm.
- [ ] Verify delivery to all users and log in Audit thread.

## 6. Support System (System 10)
- [ ] Open a support ticket.
- [ ] Verify admin card has user stats (Join Date, Plan, etc.).
- [ ] Verify admin MUST click `✅ Accept Support` before they can reply.
- [ ] Verify inactivity warning after 5 mins of `pending`.

## 7. Takedown System (System 11)
- [ ] Run `/takedown`.
- [ ] Complete the FSM flow (Content ID -> Reason -> Link).
- [ ] Reject the request with a reason.
- [ ] Verify a support ticket is automatically opened for the user with rejection context.

## 8. Moderation & Cleanup (System 13)
- [ ] Approve or Reject a submission.
- [ ] Verify the mod card AND the media messages in the Hub are deleted.

## 9. Audit Logs (System 18)
- [ ] Perform any admin action.
- [ ] Verify a formatted audit log appears in the dedicated Hub Audit thread.
