"""Username / password authentication."""

from __future__ import annotations

import hmac

from .config import BrokerConfig


class Authenticator:
    """Validates CONNECT credentials against configured users."""

    def __init__(self, config: BrokerConfig) -> None:
        self._users = dict(config.users)
        self._allow_anonymous = config.allow_anonymous

    def authenticate(self, username: str | None, password: bytes | None) -> bool:
        if username is None:
            return self._allow_anonymous
        expected = self._users.get(username)
        if expected is None:
            return False
        supplied = (password or b"").decode("utf-8", errors="replace")
        # constant-time comparison to avoid leaking via timing
        return hmac.compare_digest(expected.encode("utf-8"), supplied.encode("utf-8"))
