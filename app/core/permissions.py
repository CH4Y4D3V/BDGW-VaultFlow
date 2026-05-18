from __future__ import annotations

from enum import Enum
from typing import List

from app.config import settings


class Role(str, Enum):
    OWNER = "owner"
    SUPER_ADMIN = "super_admin"
    MODERATOR = "moderator"
    SUPPORT_ADMIN = "support_admin"
    PAYMENT_ADMIN = "payment_admin"
    SCHEDULER_ADMIN = "scheduler_admin"


def get_user_roles(user_id: int) -> List[Role]:
    roles: List[Role] = []
    if user_id == settings.OWNER_ID:
        roles.append(Role.OWNER)
    if user_id in settings.SUDO_IDS:
        roles.append(Role.SUPER_ADMIN)
    if user_id in settings.ADMIN_IDS or user_id in settings.MODERATOR_IDS:
        roles.append(Role.MODERATOR)
    if user_id in settings.SUPPORT_ADMIN_IDS:
        roles.append(Role.SUPPORT_ADMIN)
    if user_id in settings.PAYMENT_ADMIN_IDS:
        roles.append(Role.PAYMENT_ADMIN)
    if user_id in settings.SCHEDULER_ADMIN_IDS:
        roles.append(Role.SCHEDULER_ADMIN)
    return roles


def has_role(user_id: int, role: Role) -> bool:
    """OWNER and SUPER_ADMIN inherit all roles."""
    if user_id == settings.OWNER_ID:
        return True
    if user_id in settings.SUDO_IDS:
        return True
    return role in get_user_roles(user_id)


def is_moderator(user_id: int) -> bool:
    return has_role(user_id, Role.MODERATOR)


def is_support_admin(user_id: int) -> bool:
    return has_role(user_id, Role.SUPPORT_ADMIN)


def is_payment_admin(user_id: int) -> bool:
    return has_role(user_id, Role.PAYMENT_ADMIN)


def is_scheduler_admin(user_id: int) -> bool:
    return has_role(user_id, Role.SCHEDULER_ADMIN)


def is_any_admin(user_id: int) -> bool:
    return bool(get_user_roles(user_id))