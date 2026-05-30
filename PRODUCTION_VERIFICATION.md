# Production Verification Guide

## 1. Start UI Verification
- [ ] Run `/start` in the bot.
- [ ] Verify the following buttons appear vertically:
    1. 💎 Premium Access
    2. 📤 Submit Content
    3. 📊 My Status
    4. 🆘 Need Help

## 2. Support System Verification
- [ ] Click "🆘 Need Help".
- [ ] Send a message to the bot.
- [ ] In the Verification Hub, verify a new topic was created.
- [ ] As an admin, click "✅ Resolve" or "🚫 Close Ticket" in the topic.
- [ ] Verify the user receives a closure notification.
- [ ] Verify the topic thread is closed in the Verification Hub.

## 3. Payment Workflow Verification
- [ ] As a user, click "💎 Premium Access" and select a plan.
- [ ] Request payment details for a method.
- [ ] As an admin, click "📩 Send Payment Details" in the payment topic.
- [ ] Reply to the moderation card with a payment number.
- [ ] Verify the user receives the payment number.
- [ ] Verify the user can now send a TXID (this means the session moved to `AWAITING_PAYMENT`).
- [ ] After the user sends a screenshot, as an admin, click "✅ Approve".
- [ ] Verify the user's premium access is activated and they receive an invite link.

## 4. Referral & Rewards Verification
- [ ] Refer a new user.
- [ ] Verify exactly 1 point is granted after qualification.
- [ ] Submit 2 albums and have them approved.
- [ ] Verify 1 point is granted to the submitter (if they were referred).
- [ ] Verify the "My Status" card shows the correctly earned points.
