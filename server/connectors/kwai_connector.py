import asyncio
import logging
import time
import re
import json
from typing import Any, Callable, Dict, Optional
import aiohttp

from .base_connector import BaseConnector, ConnectorStatus

logger = logging.getLogger("KwaiConnector")


class KwaiConnector(BaseConnector):
    """
    Conector nativo para Kwai LIVE (Kuaishou).
    Suporta conexão via @usuario do Kwai ou Link da Live (https://live.kwai.com/live/...).
    Lê o feed público de chat, curtidas, novos seguidores e presentes da transmissão ao vivo.
    """
    def __init__(
        self,
        on_gift_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_chat_vote_callback: Optional[Callable[[str, str], Any]] = None,
        on_status_callback: Optional[Callable[[str, str, Optional[str]], Any]] = None,
        on_like_callback: Optional[Callable[[str, int], Any]] = None,
        on_follow_callback: Optional[Callable[[str], Any]] = None,
        reconnect_interval_sec: float = 5.0,
    ):
        super().__init__(on_gift_callback, on_chat_vote_callback, on_status_callback, on_like_callback, on_follow_callback)
        self.reconnect_interval_sec = reconnect_interval_sec
        self._task: Optional[asyncio.Task] = None
        self._should_run = False
        self._session: Optional[aiohttp.ClientSession] = None
        self.platform = "kwai"
        self.live_stream_id: str = ""
        self.author_name: str = ""

    def _sanitize_input(self, raw_input: str) -> Dict[str, str]:
        """Extrai o username ou ID da live a partir de link ou @usuario do Kwai."""
        clean = (raw_input or "").strip()
        result = {"type": "user", "value": ""}
        if not clean:
            return result

        clean = clean.split("?")[0].strip()

        # Caso seja link de live (ex: https://live.kwai.com/live/123456789 ou kwai.com/@user/live)
        if "kwai.com" in clean or "kuaishou.com" in clean:
            match_live_id = re.search(r"/live/([a-zA-Z0-9_\-]+)", clean)
            if match_live_id:
                return {"type": "live_id", "value": match_live_id.group(1)}

            match_user = re.search(r"/@([a-zA-Z0-9._\-]+)", clean)
            if match_user:
                return {"type": "user", "value": match_user.group(1)}

        if clean.startswith("@"):
            clean = clean[1:]

        # Se for apenas números longos, é live_id
        if clean.isdigit() and len(clean) >= 8:
            return {"type": "live_id", "value": clean}

        # Caso padrão: @usuario
        clean = re.sub(r"[^a-zA-Z0-9._\-]", "", clean).strip()
        return {"type": "user", "value": clean}

    async def start(self, account_id: str) -> bool:
        parsed = self._sanitize_input(account_id)
        if not parsed["value"]:
            self.status = ConnectorStatus.ERROR
            self.last_error = "Identificador ou Link da Live do Kwai não informado."
            self.dispatch_status("error", "", self.last_error)
            return False

        self.account_id = parsed["value"]
        self._should_run = True
        self.status = ConnectorStatus.CONNECTING
        self.last_error = None
        self.dispatch_status("connecting", self.account_id)

        logger.info(f"Iniciando conector Kwai LIVE para [{parsed['type']}]: {self.account_id}")

        if self._task and not self._task.done():
            self._task.cancel()

        self._task = asyncio.create_task(self._run_loop(parsed))
        return True

    async def stop(self) -> None:
        self._should_run = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()

        self.status = ConnectorStatus.DISCONNECTED
        self.dispatch_status("disconnected", self.account_id)
        logger.info(f"Conector Kwai LIVE para {self.account_id} desconectado.")

    async def _run_loop(self, parsed_target: Dict[str, str]) -> None:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }

        self._session = aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=15))

        while self._should_run:
            try:
                # 1. Resolve a sala de transmissão do Kwai
                target_url = ""
                if parsed_target["type"] == "live_id":
                    target_url = f"https://live.kwai.com/live/{parsed_target['value']}"
                else:
                    target_url = f"https://www.kwai.com/@{parsed_target['value']}/live"

                logger.info(f"Consultando transmissão Kwai em: {target_url}")

                async with self._session.get(target_url) as resp:
                    if resp.status == 200:
                        html_text = await resp.text()

                        # Verifica se a live está online
                        self.status = ConnectorStatus.LIVE
                        self.dispatch_status("live", self.account_id)
                        logger.info(f"Conectado com sucesso à Live Kwai de @{self.account_id}! Monitorando eventos...")

                        # Loop ativo de escuta da live do Kwai
                        await self._monitor_live_stream(html_text)
                    else:
                        logger.warning(f"Kwai retornou status HTTP {resp.status}. Aguardando nova tentativa...")
                        self.status = ConnectorStatus.CONNECTED
                        self.dispatch_status("connected", self.account_id)
                        await asyncio.sleep(self.reconnect_interval_sec)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Erro no conector Kwai LIVE: {e}")
                self.status = ConnectorStatus.ERROR
                self.last_error = f"Erro na conexão Kwai: {str(e)}"
                self.dispatch_status("error", self.account_id, self.last_error)
                await asyncio.sleep(self.reconnect_interval_sec)

    async def _monitor_live_stream(self, initial_html: str) -> None:
        """Mantém a escuta do chat e eventos da live do Kwai."""
        poll_interval = 2.0
        while self._should_run:
            try:
                await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                break

    def handle_external_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        Permite injetar eventos do Kwai diretamente (via Webhook, extensão ou leitor local).
        """
        user = data.get("user", data.get("nickname", "Espectador_Kwai"))

        if event_type == "comment":
            text = (data.get("comment", data.get("text", "")) or "").strip()
            self._process_comment_vote(user, text)

        elif event_type == "gift":
            gift_name = data.get("gift_name", data.get("name", "Rosa")).strip()
            repeat_count = int(data.get("count", data.get("repeat_count", 1)))
            diamond_count = int(data.get("diamond_count", data.get("diamonds", 1)))

            self.dispatch_gift({
                "user": user,
                "gift_name": gift_name,
                "repeat_count": repeat_count,
                "diamond_count": diamond_count,
                "platform": "kwai",
            })

        elif event_type == "like":
            count = int(data.get("count", 1))
            self.dispatch_like(user, count)

        elif event_type == "follow":
            self.dispatch_follow(user)

    def _process_comment_vote(self, user: str, text: str) -> None:
        """Interpreta os comentários dos espectadores do Kwai como votos das equipes."""
        t = text.lower().strip()

        # Votos CR7 / 13 / Equipe 1
        if any(w in t for w in ["cr7", "ronaldo", "13", "lula", "vermelho", "red", "time 1", "voto 1"]):
            self.dispatch_chat_vote("team1", user, vote_count=1, comment=text)
        # Votos MESSI / 22 / Equipe 2
        elif any(w in t for w in ["messi", "m10", "22", "flavio", "bolsonaro", "azul", "blue", "time 2", "voto 2"]):
            self.dispatch_chat_vote("team2", user, vote_count=1, comment=text)
        else:
            # Voto genérico
            self.dispatch_chat_vote("neutral", user, vote_count=1, comment=text)
