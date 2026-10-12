from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, Optional


class ConnectorStatus(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    LIVE = "live"
    ERROR = "error"


class BaseConnector(ABC):
    def __init__(
        self,
        on_gift_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_chat_vote_callback: Optional[Callable[[str, str], Any]] = None,
    ):
        self.on_gift_callback = on_gift_callback
        self.on_chat_vote_callback = on_chat_vote_callback
        self.status = ConnectorStatus.DISCONNECTED
        self.last_error: Optional[str] = None
        self.account_id: str = ""

    @abstractmethod
    async def start(self, account_id: str) -> bool:
        """Starts connection to the livestream."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Gracefully stops connection."""
        pass

    def get_status(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "account_id": self.account_id,
            "last_error": self.last_error,
        }

    def dispatch_gift(self, gift_event: Dict[str, Any]) -> None:
        if self.on_gift_callback:
            try:
                self.on_gift_callback(gift_event)
            except Exception as e:
                import logging
                logging.getLogger("BaseConnector").error(f"Error in gift callback: {e}")

    def dispatch_chat_vote(self, team_key: str, user: str, vote_count: int = 1) -> None:
        if self.on_chat_vote_callback:
            try:
                self.on_chat_vote_callback(team_key, user, vote_count)
            except TypeError:
                self.on_chat_vote_callback(team_key, user)
            except Exception as e:
                import logging
                logging.getLogger("BaseConnector").error(f"Error in chat vote callback: {e}")
