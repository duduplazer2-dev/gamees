import asyncio
import logging
import random
import time
from typing import Any, Callable, Dict, Optional

from .base_connector import BaseConnector, ConnectorStatus

logger = logging.getLogger("MockConnector")


class MockConnector(BaseConnector):
    """
    Simulated connector that generates gifts for demonstration or testing.
    """
    def __init__(self, on_gift_callback: Optional[Callable[[Dict[str, Any]], Any]] = None):
        super().__init__(on_gift_callback)
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self.auto_emit = False
        self.emit_interval = 4.0

    async def start(self, account_id: str = "demo_host", auto_emit: bool = False) -> bool:
        self.account_id = account_id
        self.auto_emit = auto_emit
        self.status = ConnectorStatus.CONNECTING
        logger.info(f"MockConnector connecting with host: {account_id}")
        await asyncio.sleep(0.5)
        self.status = ConnectorStatus.LIVE
        self._running = True
        logger.info("MockConnector is now LIVE (simulation mode)")

        if self.auto_emit:
            self._task = asyncio.create_task(self._auto_gift_loop())
        return True

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.status = ConnectorStatus.DISCONNECTED
        logger.info("MockConnector stopped.")

    async def _auto_gift_loop(self) -> None:
        mock_users = ["Carlos_BR", "Marina_Live", "Pedrinho99", "Julia_TikTok", "Lucas_Gamer"]
        mock_gifts = [
            ("rose", "Rose", 1),
            ("rose", "Rose", 5),
            ("tiktok", "TikTok", 1),
            ("tiktok", "TikTok", 5),
            ("cap", "Cap", 1),
            ("heart", "Heart", 1),
            ("fireworks", "Fireworks", 1),
        ]

        while self._running:
            await asyncio.sleep(self.emit_interval + random.uniform(-1.0, 1.5))
            if not self._running:
                break
            gift_id, gift_name, count = random.choice(mock_gifts)
            user = random.choice(mock_users)
            self.simulate_gift(gift_id=gift_id, gift_name=gift_name, repeat_count=count, user=user)

    def simulate_gift(
        self,
        gift_id: str,
        gift_name: str,
        repeat_count: int = 1,
        user: str = "Testador",
        streak_id: Optional[str] = None,
        is_repeat_end: bool = True,
    ) -> Dict[str, Any]:
        """Manually trigger a simulated gift event."""
        payload = {
            "gift_id": gift_id,
            "gift_name": gift_name,
            "repeat_count": max(1, repeat_count),
            "user": user,
            "streak_id": streak_id,
            "is_repeat_end": is_repeat_end,
            "timestamp": time.time(),
        }
        self.dispatch_gift(payload)
        return payload

    def simulate_chat_vote(self, team_key: str, user: str = "Espectador_Chat") -> Dict[str, Any]:
        """Manually trigger a simulated chat vote (13 or 22)."""
        self.dispatch_chat_vote(team_key, user)
        return {"team": team_key, "user": user}
