import asyncio
import logging
import time
import re
from typing import Any, Callable, Dict, Optional

from .base_connector import BaseConnector, ConnectorStatus

logger = logging.getLogger("TikTokConnector")


class TikTokConnector(BaseConnector):
    """
    Isolated TikTok LIVE connector using the community TikTokLive library.
    Does NOT require passwords, cookies, or account credentials.
    Works purely with public stream events for @username.
    Features smart live-checking to prevent EulerStream sign server rate limits.
    """
    def __init__(
        self,
        on_gift_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_chat_vote_callback: Optional[Callable[[str, str], Any]] = None,
        on_status_callback: Optional[Callable[[str, str, Optional[str]], Any]] = None,
        on_like_callback: Optional[Callable[[str, int], Any]] = None,
        on_follow_callback: Optional[Callable[[str], Any]] = None,
        reconnect_interval_sec: float = 5.0,
        max_reconnect_attempts: int = 9999,
        api_key: Optional[str] = None,
    ):
        super().__init__(on_gift_callback, on_chat_vote_callback, on_status_callback, on_like_callback, on_follow_callback)
        self.reconnect_interval_sec = reconnect_interval_sec
        self.max_reconnect_attempts = max_reconnect_attempts
        import os
        self.api_key = (api_key or "").strip() or os.environ.get("TIKTOK_SIGN_API_KEY") or os.environ.get("EULERSTREAM_API_KEY") or "euler_Yjc4MzQ0ZmRiZDgyNTU1YjczMTQwN2UxY2VhOGM5ZGYxMjk1NDFhMzNhZGU0NWIxNGI5MmJj"
        self._client = None
        self._task: Optional[asyncio.Task] = None
        self._should_run = False
        self._reconnect_count = 0

    def _sanitize_username(self, raw_input: str) -> str:
        """Extrai o username limpo a partir de @user, user, ou URL completa do TikTok."""
        clean = (raw_input or "").strip()
        if not clean:
            return ""

        # Remove query params (?is_from_webapp=...)
        clean = clean.split("?")[0].strip()

        # Se for URL completa (ex: https://www.tiktok.com/@isabelleesilv1 ou /live)
        if "/" in clean:
            parts = [p.strip() for p in clean.rstrip("/").split("/") if p.strip()]
            for p in reversed(parts):
                if p.startswith("@"):
                    clean = p
                    break
                elif p and p.lower() != "live" and not p.lower().startswith("http") and not "tiktok.com" in p.lower():
                    clean = p
                    break

        if clean.startswith("@"):
            clean = clean[1:]

        # Remove caracteres inválidos em usernames do TikTok (apenas letras, números, pontos e sublinhados)
        clean = re.sub(r"[^a-zA-Z0-9._-]", "", clean).strip()
        return clean

    async def start(self, account_id: str) -> bool:
        clean_id = self._sanitize_username(account_id)

        if not clean_id:
            self.status = ConnectorStatus.ERROR
            self.last_error = "Identificador público da conta (@username) não informado."
            self.dispatch_status("error", "", self.last_error)
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
        self.last_error = None

        if self._client:
            try:
                if hasattr(self._client, "disconnect"):
                    await self._client.disconnect()
                elif hasattr(self._client, "stop"):
                    self._client.stop()
            except Exception as e:
                logger.debug(f"Erro ao desconectar client: {e}")
            self._client = None

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("TikTokConnector desconectado e parado com sucesso.")
        self.dispatch_status("disconnected", self.account_id)

    async def _run_loop(self) -> None:
        while self._should_run:
            client = None
            try:
                # Import dinâmico da biblioteca TikTokLive
                from TikTokLive import TikTokLiveClient
                from TikTokLive.events import GiftEvent, ConnectEvent, DisconnectEvent, CommentEvent, LikeEvent
                try:
                    from TikTokLive.events import FollowEvent, SocialEvent
                except ImportError:
                    FollowEvent = None
                    SocialEvent = None
                from TikTokLive.client.errors import UserNotFoundError, UserOfflineError
                from TikTokLive.client.web.web_settings import WebDefaults

                if self.api_key:
                    WebDefaults.tiktok_sign_api_key = self.api_key
                    logger.info("Chave de API do EulerStream ativada com sucesso.")
            except ImportError:
                self.status = ConnectorStatus.ERROR
                self.last_error = "Biblioteca TikTokLive não instalada. Execute 'scripts\\install.bat'."
                logger.error(self.last_error)
                self.dispatch_status("error", self.account_id, self.last_error)
                return

            try:
                client = TikTokLiveClient(unique_id=self.account_id)
                self._client = client

                # -----------------------------------------------------------------
                # 1. RESOLUÇÃO DIRETA DE ROOM ID VIA API TIKTOK (SUPORTE TOTAL A TIKTOK LIVE STUDIO)
                # -----------------------------------------------------------------
                self.status = ConnectorStatus.CONNECTING
                self.dispatch_status("connecting", self.account_id)
                logger.info(f"Resolvendo transmissão ao vivo para @{self.account_id}...")

                resolved_room_id = None
                try:
                    resolved_room_id = await client._web.fetch_room_id_from_api(self.account_id)
                    logger.info(f"🟢 Room ID detectado com sucesso: {resolved_room_id}")
                except UserNotFoundError:
                    self.status = ConnectorStatus.ERROR
                    self.last_error = f"A conta @{self.account_id} não foi encontrada no TikTok. Verifique o @ digitado."
                    logger.warning(self.last_error)
                    self.dispatch_status("error", self.account_id, self.last_error)
                    await asyncio.sleep(10.0)
                    continue
                except Exception as ex_room:
                    logger.debug(f"Aviso de resolução de Room ID: {ex_room}")

                if not resolved_room_id:
                    # Se não encontrou Room ID ativo, aguarda e tenta novamente sem travar
                    self.status = ConnectorStatus.CONNECTING
                    self.last_error = f"Aguardando a Live de @{self.account_id} no TikTok..."
                    logger.info(self.last_error)
                    self.dispatch_status("connecting", self.account_id, self.last_error)
                    await asyncio.sleep(8.0)
                    continue

                # -----------------------------------------------------------------
                # 3. REGISTRO DE EVENTOS (CONNECT, DISCONNECT, GIFT, COMMENT)
                # -----------------------------------------------------------------
                @client.on(ConnectEvent)
                async def on_connect(event: ConnectEvent):
                    self.status = ConnectorStatus.LIVE
                    self._reconnect_count = 0
                    self.last_error = None
                    logger.info(f"🟢 CONECTADO COM SUCESSO À LIVE DE @{self.account_id}!")
                    self.dispatch_status("live", self.account_id)

                @client.on(DisconnectEvent)
                async def on_disconnect(event: DisconnectEvent):
                    if self._should_run:
                        self.status = ConnectorStatus.CONNECTING
                        logger.warning(f"Desconectado da LIVE de @{self.account_id}. Loop reconectará...")
                        self.dispatch_status("connecting", self.account_id)

                @client.on(GiftEvent)
                async def on_gift(event: GiftEvent):
                    try:
                        self._handle_raw_gift_event(event)
                    except Exception as ex:
                        logger.error(f"Erro ao processar presente da live: {ex}")

                @client.on(LikeEvent)
                async def on_like(event: LikeEvent):
                    try:
                        user = "Espectador"
                        if hasattr(event, "user") and event.user:
                            user = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "Espectador")
                        like_count = getattr(event, "likes", 1) or getattr(event, "count", 1) or 1
                        logger.info(f"[TIKTOK LIKE] {user} curtiu a live (x{like_count})")
                        self.dispatch_like(str(user), int(like_count))
                    except Exception as ex:
                        logger.error(f"Erro ao processar like da live: {ex}")

                # Listener para Novos Seguidores (FollowEvent & SocialEvent)
                async def _handle_follow_payload(event, event_label="FOLLOW"):
                    try:
                        user = "Novo Seguidor"
                        if hasattr(event, "user") and event.user:
                            user = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "Novo Seguidor")
                        logger.info(f"[TIKTOK {event_label}] {user} seguiu a live! Disparando +6 pontos.")
                        self.dispatch_follow(str(user))
                    except Exception as ex:
                        logger.error(f"Erro ao processar {event_label} da live: {ex}")

                if FollowEvent is not None:
                    @client.on(FollowEvent)
                    async def on_follow(event: FollowEvent):
                        await _handle_follow_payload(event, "FOLLOW")

                if SocialEvent is not None:
                    @client.on(SocialEvent)
                    async def on_social(event: SocialEvent):
                        # SocialEvent no TikTok é acionado para seguir o anfitrião e compartilhamento
                        await _handle_follow_payload(event, "SOCIAL_FOLLOW")

                @client.on(CommentEvent)
                async def on_comment(event: CommentEvent):
                    try:
                        text = getattr(event, "comment", "") or ""
                        user = "Espectador"
                        if hasattr(event, "user") and event.user:
                            user = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "Espectador")

                        norm = text.lower().strip()

                        # Deduplicação de comentários para evitar duplicar votos idênticos em rajada
                        now_time = time.time()
                        comment_key = f"{user.lower().strip()}:{norm}"
                        if not hasattr(self, "_recent_comments"):
                            self._recent_comments = {}
                        if comment_key in self._recent_comments and (now_time - self._recent_comments[comment_key]) < 1.5:
                            return
                        self._recent_comments[comment_key] = now_time
                        if len(self._recent_comments) > 250:
                            self._recent_comments = {k: v for k, v in self._recent_comments.items() if (now_time - v) < 4.0}

                        if any(k in norm for k in ["segui", "seguir", "seguiu", "follow", "followed", "sdv"]):
                            logger.info(f"[CHAT FOLLOW] {user} seguiu a live via chat: {text}")
                            self.dispatch_follow(str(user))

                        # Red Team (Lula 13, Digite 1, Vermelho, Carro 1)
                        has_13 = (
                            norm == "1" or norm.startswith("1 ") or norm.endswith(" 1") or
                            "13" in norm or "lula" in norm or "lulinha" in norm or "pt" in norm or 
                            "faz o l" in norm or "faz o 13" in norm or norm == "l" or 
                            norm.startswith("l ") or norm.endswith(" l") or "barba" in norm or
                            "treze" in norm or "vermelho" in norm or "red" in norm
                        )
                        # Blue/Green Team (Bolsonaro 22, Digite 2, Azul, Carro 2)
                        has_22 = (
                            norm == "2" or norm.startswith("2 ") or norm.endswith(" 2") or
                            "22" in norm or "bolsonaro" in norm or "bolso" in norm or "mito" in norm or 
                            "capitao" in norm or "capitão" in norm or "jair" in norm or "flávio" in norm or 
                            "flavio" in norm or "vinte e dois" in norm or "verde" in norm or "azul" in norm or "blue" in norm
                        )

                        if has_13 and not has_22:
                            logger.info(f"[CHAT VOTE 13] {user} votou Vermelho/13: {text}")
                            self.dispatch_chat_vote("red", str(user), 1, text)
                        elif has_22 and not has_13:
                            logger.info(f"[CHAT VOTE 22] {user} votou Azul/22: {text}")
                            self.dispatch_chat_vote("green", str(user), 1, text)
                        else:
                            # Encaminha comentários gerais e escolhas de veículos do jogo de carros (gol, 160, pop, chevette, xre, uno, opala, bmw, ferrari, fusca, kombi, biz, saveiro)
                            self.dispatch_chat_vote("vehicle_choice", str(user), 1, text)
                    except Exception as ex:
                        logger.error(f"Erro ao processar comentário do chat: {ex}")

                # -----------------------------------------------------------------
                # 4. CONEXÃO COM O STREAMING VIA EULERSTREAM
                # -----------------------------------------------------------------
                self.dispatch_status("connecting", self.account_id)
                logger.info(f"Iniciando conexão Webcast para @{self.account_id}...")
                await client.connect(room_id=resolved_room_id, fetch_live_check=False)

                # Se a conexão finalizar normalmente enquanto ainda estiver habilitado:
                if self._should_run:
                    self._reconnect_count += 1
                    # Checa se a live encerrou
                    still_live = False
                    try:
                        still_live = await client._web.fetch_is_live(unique_id=self.account_id)
                    except Exception:
                        still_live = False

                    if not still_live:
                        self.status = ConnectorStatus.CONNECTING
                        self.last_error = f"A LIVE de @{self.account_id} foi encerrada ou pausada. Aguardando transmissão..."
                        logger.info(self.last_error)
                        self.dispatch_status("connecting", self.account_id, self.last_error)
                        await asyncio.sleep(10.0)
                    else:
                        backoff = min(15.0, 5.0 + min(self._reconnect_count, 4) * 2.0)
                        logger.warning(f"Conexão oscilou. Reconectando em {backoff:.1f}s...")
                        await asyncio.sleep(backoff)

            except asyncio.CancelledError:
                break
            except Exception as e:
                err_msg = str(e)
                err_lower = err_msg.lower()

                # -----------------------------------------------------------------
                # 5. PROTEÇÃO CONTRA RATE LIMIT (429 EULERSTREAM)
                # -----------------------------------------------------------------
                if "rate_limit" in err_lower or "too many connections" in err_lower or "429" in err_msg:
                    self.status = ConnectorStatus.ERROR
                    self.last_error = "OFFLINE"
                    logger.warning(f"TikTok rate limit ou pausa temporária: {err_msg}")
                    self.dispatch_status("error", self.account_id, "OFFLINE")
                    await asyncio.sleep(20.0)
                elif "offline" in err_lower or "useroffline" in err_lower:
                    self.status = ConnectorStatus.ERROR
                    self.last_error = "OFFLINE"
                    logger.info(f"Streamer @{self.account_id} está offline no momento.")
                    self.dispatch_status("error", self.account_id, "OFFLINE")
                    await asyncio.sleep(10.0)
                else:
                    self.status = ConnectorStatus.ERROR
                    self.last_error = "OFFLINE"
                    logger.error(f"Erro no conector TikTok: {err_msg}")
                    self.dispatch_status("error", self.account_id, "OFFLINE")
                    self._reconnect_count += 1
                    backoff = min(20.0, 8.0 + min(self._reconnect_count, 4) * 3.0)
                    await asyncio.sleep(backoff)

            finally:
                if client is not None:
                    try:
                        if hasattr(client, "disconnect"):
                            await client.disconnect()
                        elif hasattr(client, "stop"):
                            client.stop()
                    except Exception:
                        pass

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
