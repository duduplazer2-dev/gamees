import asyncio
import logging
import time
from typing import Any, Callable, Dict, Optional

from .base_connector import BaseConnector, ConnectorStatus

logger = logging.getLogger("TikTokConnector")


class TikTokConnector(BaseConnector):
    """
    Isolated TikTok LIVE connector using the community TikTokLive library.
    Does NOT require passwords, cookies, or account credentials.
    Works purely with public stream events for @username.
    """
    def __init__(
        self,
        on_gift_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_chat_vote_callback: Optional[Callable[[str, str], Any]] = None,
        reconnect_interval_sec: float = 5.0,
        max_reconnect_attempts: int = 5,
    ):
        super().__init__(on_gift_callback, on_chat_vote_callback)
        self.reconnect_interval_sec = reconnect_interval_sec
        self.max_reconnect_attempts = max_reconnect_attempts
        self._client = None
        self._task: Optional[asyncio.Task] = None
        self._should_run = False
        self._reconnect_count = 0

    async def start(self, account_id: str) -> bool:
        clean_id = account_id.strip()
        if clean_id.startswith("@"):
            clean_id = clean_id[1:]

        if not clean_id:
            self.status = ConnectorStatus.ERROR
            self.last_error = "Identificador público da conta (@username) não informado."
            return False

        self.account_id = clean_id
        self._should_run = True
        self._reconnect_count = 0
        self.status = ConnectorStatus.CONNECTING
        self.last_error = None

        if self._task and not self._task.done():
            self._task.cancel()

        self._task = asyncio.create_task(self._run_loop())
        return True

    async def stop(self) -> None:
        self._should_run = False
        self.status = ConnectorStatus.DISCONNECTED
        if self._client:
            try:
                # Disconnect TikTok client if active
                if hasattr(self._client, "disconnect"):
                    await self._client.disconnect()
                elif hasattr(self._client, "stop"):
                    self._client.stop()
            except Exception as e:
                logger.debug(f"Error disconnecting client: {e}")
            self._client = None

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("TikTokConnector disconnected and stopped.")

    async def _run_loop(self) -> None:
        while self._should_run and self._reconnect_count < self.max_reconnect_attempts:
            try:
                # Dynamic import to keep connector completely decoupled
                from TikTokLive import TikTokLiveClient
                from TikTokLive.events import GiftEvent, ConnectEvent, DisconnectEvent, CommentEvent
            except ImportError as e:
                self.status = ConnectorStatus.ERROR
                self.last_error = "Biblioteca TikTokLive não instalada. Execute 'scripts\\install.bat'."
                logger.error(self.last_error)
                return

            try:
                self.status = ConnectorStatus.CONNECTING
                logger.info(f"Connecting to TikTok LIVE of @{self.account_id}...")

                client = TikTokLiveClient(unique_id=self.account_id)
                self._client = client

                @client.on(ConnectEvent)
                async def on_connect(event: ConnectEvent):
                    self.status = ConnectorStatus.LIVE
                    self._reconnect_count = 0
                    self.last_error = None
                    logger.info(f"Successfully connected to @{self.account_id}'s TikTok LIVE!")

                @client.on(DisconnectEvent)
                async def on_disconnect(event: DisconnectEvent):
                    if self._should_run:
                        self.status = ConnectorStatus.CONNECTING
                        logger.warning("Disconnected from TikTok LIVE. Will attempt reconnection...")

                @client.on(GiftEvent)
                async def on_gift(event: GiftEvent):
                    try:
                        self._handle_raw_gift_event(event)
                    except Exception as ex:
                        logger.error(f"Error handling gift event: {ex}")

                @client.on(CommentEvent)
                async def on_comment(event: CommentEvent):
                    try:
                        text = getattr(event, "comment", "") or ""
                        user = "Espectador"
                        if hasattr(event, "user") and event.user:
                            user = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "Espectador")

                        norm = text.lower().strip()
                        # Red Team (Lula 13)
                        has_13 = (
                            "13" in norm or "lula" in norm or "lulinha" in norm or "pt" in norm or 
                            "faz o l" in norm or "faz o 13" in norm or norm == "l" or 
                            norm.startswith("l ") or norm.endswith(" l") or "barba" in norm
                        )
                        # Green Team (Bolsonaro 22)
                        has_22 = (
                            "22" in norm or "bolsonaro" in norm or "bolso" in norm or "mito" in norm or 
                            "capitao" in norm or "capitão" in norm or "jair" in norm or "flávio" in norm or 
                            "flavio" in norm
                        )

                        if has_13 and not has_22:
                            count = min(6, max(1, max(norm.count("13"), norm.count("lula"))))
                            logger.info(f"[CHAT VOTE] {user} voted for RED (Lula) x{count}: {text}")
                            self.dispatch_chat_vote("red", str(user), count)
                        elif has_22 and not has_13:
                            count = min(6, max(1, max(norm.count("22"), norm.count("bolso"))))
                            logger.info(f"[CHAT VOTE] {user} voted for GREEN (Bolsonaro) x{count}: {text}")
                            self.dispatch_chat_vote("green", str(user), count)
                    except Exception as ex:
                        logger.error(f"Error handling comment event: {ex}")

                # Start client connection
                # In TikTokLive, connect() is an async coroutine
                await client.connect()

                # If connection finishes cleanly while still wanting to run
                if self._should_run:
                    self._reconnect_count += 1
                    logger.warning(
                        f"Stream ended or connection dropped. Reconnect attempt {self._reconnect_count}/{self.max_reconnect_attempts} in {self.reconnect_interval_sec}s"
                    )
                    await asyncio.sleep(self.reconnect_interval_sec)

            except asyncio.CancelledError:
                break
            except Exception as e:
                err_msg = str(e)
                if "UserOffline" in type(e).__name__ or "UserOffline" in err_msg or "offline" in err_msg.lower():
                    self.status = ConnectorStatus.ERROR
                    self.last_error = f"A conta @{self.account_id} está OFFLINE no TikTok. Inicie a sua LIVE no app primeiro!"
                    logger.warning(f"TikTok user @{self.account_id} is currently offline.")
                    break
                else:
                    self.last_error = err_msg
                    logger.error(f"TikTok connection error: {err_msg}")
                    self._reconnect_count += 1
                    if self._reconnect_count >= self.max_reconnect_attempts:
                        self.status = ConnectorStatus.ERROR
                        self.last_error = f"Falha após {self.max_reconnect_attempts} tentativas. Verifique se a conta @{self.account_id} está realmente ao vivo."
                        break
                    await asyncio.sleep(self.reconnect_interval_sec)

    def _handle_raw_gift_event(self, event: Any) -> None:
        """Parses TikTokLive GiftEvent into standardized internal structure."""
        gift = getattr(event, "gift", None)
        user = getattr(event, "user", None)

        gift_name = getattr(gift, "name", "") if gift else ""
        if not gift_name and hasattr(event, "gift_name"):
            gift_name = getattr(event, "gift_name", "")
        if not gift_name:
            gift_name = "Desconhecido"

        gift_id = getattr(gift, "id", "") if gift else ""
        if not gift_id and hasattr(event, "gift_id"):
            gift_id = getattr(event, "gift_id", "")
        
        # Determine repeat count / combo
        repeat_count = 1
        if hasattr(event, "repeat_count") and event.repeat_count:
            repeat_count = event.repeat_count
        elif gift and hasattr(gift, "repeat_count") and gift.repeat_count:
            repeat_count = gift.repeat_count

        user_name = "Espectador"
        if user:
            user_name = getattr(user, "nickname", None) or getattr(user, "unique_id", "Espectador")

        # Streak indicator
        is_repeat_end = False
        if hasattr(event, "repeat_end"):
            is_repeat_end = bool(event.repeat_end)
        elif gift and hasattr(gift, "repeat_end"):
            is_repeat_end = bool(gift.repeat_end)

        # Unique streak id or group id from TikTok Webcast
        streak_id = None
        group_id = getattr(event, "group_id", 0)
        if group_id:
            streak_id = str(group_id)
        elif hasattr(gift, "streak_id") and gift.streak_id:
            streak_id = str(gift.streak_id)

        # Unique message event id for dedup
        msg_id = None
        common = getattr(event, "common", None)
        if common and hasattr(common, "msg_id") and common.msg_id:
            msg_id = str(common.msg_id)

        standardized_event = {
            "gift_id": str(gift_id),
            "gift_name": str(gift_name),
            "repeat_count": int(repeat_count),
            "user": str(user_name),
            "streak_id": streak_id,
            "is_repeat_end": is_repeat_end,
            "event_id": msg_id,
            "timestamp": time.time(),
        }

        self.dispatch_gift(standardized_event)
