import pytest
from unittest.mock import AsyncMock, MagicMock
from app.services.onboarding_service import OnboardingService, UserState
from app.models.subscription import Subscription, Plan, SubscriptionStatus
from datetime import datetime, timezone

@pytest.fixture
def mock_sub_repo():
    return AsyncMock()

@pytest.fixture
def onboarding_service(mock_sub_repo):
    return OnboardingService(mock_sub_repo)

@pytest.mark.asyncio
async def test_get_user_state_new(onboarding_service, mock_sub_repo):
    mock_sub_repo.get_by_user_id.return_value = None
    state = await onboarding_service.get_user_state(123)
    assert state == UserState.NEW

@pytest.mark.asyncio
async def test_get_user_state_premium(onboarding_service, mock_sub_repo):
    sub = Subscription(
        user_id=123,
        plan=Plan.PREMIUM,
        status=SubscriptionStatus.ACTIVE,
        started_at=datetime.now(timezone.utc),
        expires_at=None,
        grace_until=None,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc)
    )
    mock_sub_repo.get_by_user_id.return_value = sub
    state = await onboarding_service.get_user_state(123)
    assert state == UserState.PREMIUM

@pytest.mark.asyncio
async def test_get_user_state_banned(onboarding_service, mock_sub_repo):
    sub = Subscription(
        user_id=123,
        plan=Plan.FREE,
        status=SubscriptionStatus.BANNED,
        started_at=datetime.now(timezone.utc),
        expires_at=None,
        grace_until=None,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc)
    )
    mock_sub_repo.get_by_user_id.return_value = sub
    state = await onboarding_service.get_user_state(123)
    assert state == UserState.BANNED

@pytest.mark.asyncio
async def test_get_user_state_admin(onboarding_service, mock_sub_repo):
    sub = Subscription(
        user_id=123,
        plan=Plan.ADMIN,
        status=SubscriptionStatus.ACTIVE,
        started_at=datetime.now(timezone.utc),
        expires_at=None,
        grace_until=None,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc)
    )
    mock_sub_repo.get_by_user_id.return_value = sub
    state = await onboarding_service.get_user_state(123)
    assert state == UserState.ADMIN

@pytest.mark.asyncio
async def test_render_onboarding(onboarding_service, mock_sub_repo):
    mock_sub_repo.get_by_user_id.return_value = None
    text, keyboard = await onboarding_service.render_onboarding(123, "Test")
    assert "VAULTFLOW PREMIER" in text
    assert "Welcome, <b>Test</b>" in text
    assert keyboard is not None
