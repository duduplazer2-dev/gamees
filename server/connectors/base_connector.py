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
        on_status_callback: Optional[Callable[[str, str, Optional[str]], Any]] = None,
        on_like_callback: Optional[Callable[[str, int], Any]] = None,
        on_follow_callback: Optional[Callable[[str], Any]] = None,
    ):
        self.on_gift_callback = on_gift_callback
        self.on_chat_vote_callback = on_chat_vote_callback
        self.on_status_callback = on_status_callback
        self.on_like_callback = on_like_callback
        self.on_follow_callback = on_follow_callback
        self.status = ConnectorStatus.DISCONNECTED
        self.last_error: Optional[str] = None
        self.account_id: str = ""

    def dispatch_status(self, status: str, account_id: str, last_error: Optional[str] = None) -> None:
        if self.on_status_callback:
            try:
                self.on_status_callback(status, account_id, last_error)
            except Exception as e:
                import logging
                logging.getLogger("BaseConnector").error(f"Error in status callback: {e}")

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

    def dispatch_chat_vote(self, team_key: str, user: str, vote_count: int = 1, comment: str = "") -> None:
        if self.on_chat_vote_callback:
            try:
                self.on_chat_vote_callback(team_key, user, vote_count, comment)
            except TypeError:
                try:
                    self.on_chat_vote_callback(team_key, user, vote_count)
                except TypeError:
                    self.on_chat_vote_callback(team_key, user)
            except Exception as e:
                import logging
                logging.getLogger("BaseConnector").error(f"Error in chat vote callback: {e}")

    def dispatch_like(self, user: str, count: int = 1) -> None:
        if hasattr(self, "on_like_callback") and self.on_like_callback:
            try:
                self.on_like_callback(user, count)
            except Exception as e:
                import logging
                logging.getLogger("BaseConnector").error(f"Error in like callback: {e}")

    def dispatch_follow(self, user: str) -> None:
        if hasattr(self, "on_follow_callback") and self.on_follow_callback:
            try:
                self.on_follow_callback(user)
            except Exception as e:
                import logging
                logging.getLogger("BaseConnector").error(f"Error in follow callback: {e}")
