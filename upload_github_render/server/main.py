import asyncio
import json
import logging
import os
import random
import shutil
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Set

from datetime import datetime
from fastapi import FastAPI, HTTPException, UploadFile, File, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .connectors import BaseConnector, ConnectorStatus, MockConnector, TikTokConnector
from .gift_processor import GiftProcessor
from .race_engine import RaceEngine
from .subscription_manager import sub_manager

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
)
logger = logging.getLogger("RaceServer")

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "game_config.json")
GIFTS_PATH = os.path.join(BASE_DIR, "config", "gifts.json")
GAME_DIR = os.path.join(BASE_DIR, "game")


def load_game_config() -> Dict[str, Any]:
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {CONFIG_PATH}: {e}")
    return {}


# Models for REST API
class SimulateGiftRequest(BaseModel):
    gift_name: str = Field(..., description="Nome ou ID do presente (ex: Rose, TikTok, Cap, Heart)")
    repeat_count: int = Field(default=1, ge=1, le=1000, description="Quantidade de presentes")
    user: str = Field(default="Espectador_Demo", description="Nome do usuário remetente")
    streak_id: Optional[str] = Field(default=None, description="Identificador opcional de sequência")


class ConnectTikTokRequest(BaseModel):
    unique_id: str = Field(..., description="Nome de usuário público da LIVE (ex: @seuperfil)")


class AdjustTimerRequest(BaseModel):
    delta_seconds: float = Field(..., description="Segundos a adicionar (positivo) ou subtrair (negativo)")


class SetTimerRequest(BaseModel):
    seconds: float = Field(..., ge=10, le=7200, description="Duração total da corrida em segundos")


class LoginRequest(BaseModel):
    email: str
    name: Optional[str] = ""


class AdminActivateRequest(BaseModel):
    email: str
    name: Optional[str] = ""
    plan: Optional[str] = "mensal"
    game: Optional[str] = "all"


class AdminDeactivateRequest(BaseModel):
    email: str


class SupabaseConfigRequest(BaseModel):
    supabase_url: str
    supabase_key: str
    enabled: bool = True


class WebSocketManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast_json(self, data: Dict[str, Any]):
        if not self.active_connections:
            return
        dead = []
        for ws in self.active_connections:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.active_connections.discard(ws)


# Global singletons
cfg = load_game_config()
race_cfg = cfg.get("race", {})
server_cfg = cfg.get("server", {})
tiktok_cfg = cfg.get("tiktok", {})

engine = RaceEngine(
    target_distance=race_cfg.get("target_distance", 500.0),
    base_speed=race_cfg.get("base_speed", 2.0),
    max_speed=race_cfg.get("max_speed", 40.0),
    auto_restart_delay_sec=race_cfg.get("auto_restart_delay_sec", 10.0),
    red_team_name=race_cfg.get("red_team_name", "Equipe Vermelha"),
    green_team_name=race_cfg.get("green_team_name", "Equipe Verde"),
)

gift_processor = GiftProcessor(config_path=GIFTS_PATH)
ws_manager = WebSocketManager()
active_connector: Optional[BaseConnector] = None
mock_connector = MockConnector()
sim_task: Optional[asyncio.Task] = None


def handle_gift_event_sync(event_data: Dict[str, Any]) -> None:
    """Callback triggered whenever any connector receives a gift."""
    asyncio.create_task(_process_gift_async(event_data))


async def _process_gift_async(event_data: Dict[str, Any]) -> None:
    result = gift_processor.process_event(
        gift_id=event_data.get("gift_id", ""),
        gift_name=event_data.get("gift_name", ""),
        repeat_count=event_data.get("repeat_count", 1),
        user=event_data.get("user", "Espectador"),
        streak_id=event_data.get("streak_id"),
        is_repeat_end=event_data.get("is_repeat_end", False),
        event_id=event_data.get("event_id"),
    )

    if result.get("valid"):
        boost_event = await engine.apply_gift_boost(result)
        if boost_event:
            # Broadcast immediate gift alert to frontends
            await ws_manager.broadcast_json({
                "type": "gift_alert",
                "gift": boost_event,
                "team": boost_event.get("team"),
            })


mock_connector.on_gift_callback = handle_gift_event_sync


def handle_chat_vote_sync(team_key: str, user: str, vote_count: int = 1) -> None:
    asyncio.create_task(_process_chat_vote_async(team_key, user, vote_count))


async def _process_chat_vote_async(team_key: str, user: str, vote_count: int = 1) -> None:
    event = await engine.apply_chat_vote(team_key, user, vote_count)
    if event:
        await ws_manager.broadcast_json({
            "type": "chat_vote_alert",
            "vote": event,
            "team": team_key,
        })


mock_connector.on_chat_vote_callback = handle_chat_vote_sync

# Auto-Pilot Mode (Simula comentários e presentes automáticos continuamente)
autopilot_enabled = False
autopilot_task: Optional[asyncio.Task] = None

MOCK_USERNAMES = [
    "Carlos_SP", "Julia_TikTok", "Lucas_RJ", "Mariana_MG", "Pedro_BR",
    "Ana_Silva", "Rafael_Sul", "Camila_Nordeste", "Matheus99", "Beatriz_Live",
    "Torcedor13", "Capitão22", "Gabriel_Tech", "Fernanda_Gamer", "Thiago_BR",
    "LiveUser_77", "Aline_Brasil", "Felipe_22", "Renata_13", "Danilo_Show"
]


async def autopilot_loop():
    global autopilot_enabled
    logger.info("Auto-Pilot loop iniciado: gerando votos e presentes automáticos.")
    step_count = 0

    while autopilot_enabled:
        try:
            # Se TikTok LIVE real estiver conectado, encerra o piloto automático imediatamente
            if active_connector and active_connector.status in (ConnectorStatus.LIVE, ConnectorStatus.CONNECTING):
                logger.info("TikTok LIVE ativo: Auto-Pilot desativado para aceitar APENAS interações reais da live!")
                autopilot_enabled = False
                await ws_manager.broadcast_json({"type": "autopilot_status", "enabled": False})
                break

            # Se a corrida estiver aguardando início, inicia automaticamente
            if engine.state == "WAITING":
                await engine.start_race()

            # Intervalo dinâmico entre interações (0.8s a 2.0s)
            await asyncio.sleep(random.uniform(0.8, 2.0))
            if not autopilot_enabled:
                break

            if engine.state != "RACING":
                continue

            step_count += 1

            # A cada 5 a 8 votos de chat, envia 1 presente configurado
            is_gift = (step_count % random.randint(5, 8) == 0) and len(gift_processor.rules) > 0

            if is_gift:
                enabled_rules = [r for r in gift_processor.rules if r.enabled]
                if enabled_rules:
                    rule = random.choice(enabled_rules)
                    gift_user = random.choice(MOCK_USERNAMES)
                    repeat_count = 1 if rule.meters_per_unit > 40 else random.choice([1, 1, 2, 3, 5])
                    event_data = {
                        "gift_id": rule.rule_id,
                        "gift_name": rule.name,
                        "repeat_count": repeat_count,
                        "user": gift_user,
                        "streak_id": None,
                        "is_repeat_end": True,
                    }
                    await _process_gift_async(event_data)
            else:
                # Voto no chat: 13 (red) ou 22 (green)
                team = random.choice(["red", "green"])
                user = random.choice(MOCK_USERNAMES)
                await _process_chat_vote_async(team, user)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Erro no loop do Auto-Pilot: {e}")
            await asyncio.sleep(1.0)

    logger.info("Auto-Pilot loop finalizado.")


async def set_autopilot(enable: bool) -> bool:
    global autopilot_enabled, autopilot_task
    # Se TikTok LIVE real estiver conectado, impede geração de presentes falsos
    if enable and active_connector and active_connector.status == ConnectorStatus.LIVE:
        logger.warning("Bloqueado: TikTok LIVE conectado. Apenas presentes reais da live são aceitos.")
        autopilot_enabled = False
        await ws_manager.broadcast_json({
            "type": "autopilot_status",
            "enabled": False,
        })
        return False

    autopilot_enabled = bool(enable)
    if autopilot_enabled:
        if not autopilot_task or autopilot_task.done():
            autopilot_task = asyncio.create_task(autopilot_loop())
    else:
        if autopilot_task and not autopilot_task.done():
            autopilot_task.cancel()

    await ws_manager.broadcast_json({
        "type": "autopilot_status",
        "enabled": autopilot_enabled,
    })
    return autopilot_enabled


async def simulation_loop():
    """High-frequency game update loop running at configured tick rate."""
    tick_rate = max(10, min(60, race_cfg.get("tick_rate", 20)))
    interval = 1.0 / tick_rate
    last_time = time.time()

    while True:
        now = time.time()
        dt = now - last_time
        last_time = now

        snapshot = await engine.update(dt)

        # Append connector status to snapshot
        connector_status = (
            active_connector.get_status()
            if active_connector
            else {"status": ConnectorStatus.DISCONNECTED.value, "account_id": "", "last_error": None}
        )
        snapshot["connector"] = connector_status
        snapshot["autopilot"] = autopilot_enabled

        # Broadcast tick to connected clients
        await ws_manager.broadcast_json({
            "type": "race_update",
            "data": snapshot,
        })

        # Sleep remaining time
        elapsed = time.time() - now
        await asyncio.sleep(max(0.005, interval - elapsed))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global sim_task
    logger.info("Starting Race Engine simulation loop...")
    sim_task = asyncio.create_task(simulation_loop())
    yield
    logger.info("Shutting down Race Engine simulation loop...")
    if sim_task:
        sim_task.cancel()
        try:
            await sim_task
        except asyncio.CancelledError:
            pass
    if active_connector:
        await active_connector.stop()


app = FastAPI(title="TikTok LIVE Racing Game", version="1.0.0", lifespan=lifespan)

# CORS setup for local integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# REST API Endpoints
@app.get("/api/status")
async def get_status():
    snapshot = engine.get_snapshot()
    connector_status = (
        active_connector.get_status()
        if active_connector
        else {"status": ConnectorStatus.DISCONNECTED.value, "account_id": "", "last_error": None}
    )
    snapshot["connector"] = connector_status
    snapshot["autopilot"] = autopilot_enabled
    return snapshot


@app.post("/api/race/start")
async def start_race():
    snapshot = await engine.start_race()
    return {"success": True, "snapshot": snapshot}


@app.post("/api/race/pause")
async def pause_race():
    snapshot = await engine.pause_race()
    return {"success": True, "snapshot": snapshot}


@app.post("/api/race/resume")
async def resume_race():
    snapshot = await engine.resume_race()
    return {"success": True, "snapshot": snapshot}


@app.post("/api/race/reset")
async def reset_race():
    snapshot = await engine.reset_race()
    return {"success": True, "snapshot": snapshot}


@app.post("/api/race/timer/adjust")
async def adjust_timer_endpoint(req: AdjustTimerRequest):
    snapshot = await engine.adjust_timer(req.delta_seconds)
    await ws_manager.broadcast_json({"type": "race_update", "data": snapshot})
    return {"success": True, "snapshot": snapshot}


@app.post("/api/race/timer/set")
async def set_timer_endpoint(req: SetTimerRequest):
    snapshot = await engine.set_timer_duration(req.seconds)
    await ws_manager.broadcast_json({"type": "race_update", "data": snapshot})
    return {"success": True, "snapshot": snapshot}


@app.post("/api/autopilot/toggle")
async def toggle_autopilot_endpoint():
    new_state = await set_autopilot(not autopilot_enabled)
    return {"enabled": new_state}


@app.get("/api/autopilot/status")
async def get_autopilot_status_endpoint():
    return {"enabled": autopilot_enabled}


@app.post("/api/gift/simulate")
async def simulate_gift(req: SimulateGiftRequest):
    event = mock_connector.simulate_gift(
        gift_id=req.gift_name.lower(),
        gift_name=req.gift_name,
        repeat_count=req.repeat_count,
        user=req.user,
        streak_id=req.streak_id,
        is_repeat_end=True,
    )
    return {"success": True, "event": event}


class SaveGiftsConfigRequest(BaseModel):
    rules: List[Dict[str, Any]]


@app.get("/api/gifts/config")
async def get_gifts_config():
    return {
        "rules": [r.to_dict() for r in gift_processor.rules]
    }


@app.post("/api/gifts/config")
async def save_gifts_config(req: SaveGiftsConfigRequest):
    try:
        gift_processor.save_config(req.rules)
        return {
            "success": True,
            "rules": [r.to_dict() for r in gift_processor.rules],
        }
    except Exception as e:
        logger.error(f"Error saving gifts config: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/connector/connect")
async def connect_tiktok(req: ConnectTikTokRequest):
    global active_connector
    if active_connector:
        await active_connector.stop()

    # Desativa piloto automático para garantir que apenas pessoas reais da live enviem presentes!
    await set_autopilot(False)

    connector = TikTokConnector(
        on_gift_callback=handle_gift_event_sync,
        on_chat_vote_callback=handle_chat_vote_sync,
        reconnect_interval_sec=tiktok_cfg.get("reconnect_interval_sec", 5.0),
        max_reconnect_attempts=tiktok_cfg.get("max_reconnect_attempts", 5),
    )
    active_connector = connector
    success = await connector.start(req.unique_id)
    return {"success": success, "status": connector.get_status()}


@app.post("/api/connector/disconnect")
async def disconnect_tiktok():
    global active_connector
    if active_connector:
        await active_connector.stop()
        active_connector = None
    return {"success": True, "status": "disconnected"}


# Music Management Endpoints
@app.get("/api/music/list")
async def list_music_tracks():
    music_dir = os.path.join(GAME_DIR, "assets", "audio", "music")
    os.makedirs(music_dir, exist_ok=True)
    tracks = []
    for fname in sorted(os.listdir(music_dir)):
        if fname.lower().endswith((".mp3", ".wav", ".ogg", ".m4a", ".aac")):
            title = os.path.splitext(fname)[0].replace("_", " ").title()
            tracks.append({
                "id": fname,
                "name": title,
                "filename": fname,
                "url": f"assets/audio/music/{fname}",
            })
    return {"tracks": tracks}


@app.post("/api/music/upload")
async def upload_music_tracks(files: List[UploadFile] = File(...)):
    music_dir = os.path.join(GAME_DIR, "assets", "audio", "music")
    os.makedirs(music_dir, exist_ok=True)
    uploaded = []
    for f in files:
        safe_name = os.path.basename(f.filename or "musica.mp3")
        safe_name = "".join(c for c in safe_name if c.isalnum() or c in "._- ")
        if not safe_name:
            safe_name = f"track_{int(time.time())}.mp3"
        dest = os.path.join(music_dir, safe_name)
        with open(dest, "wb") as buffer:
            shutil.copyfileobj(f.file, buffer)
        uploaded.append({
            "id": safe_name,
            "name": os.path.splitext(safe_name)[0].replace("_", " ").title(),
            "filename": safe_name,
            "url": f"assets/audio/music/{safe_name}",
        })
    return {"success": True, "uploaded": uploaded}


@app.delete("/api/music/{filename}")
async def delete_music_track(filename: str):
    music_dir = os.path.join(GAME_DIR, "assets", "audio", "music")
    dest = os.path.join(music_dir, os.path.basename(filename))
    if os.path.exists(dest):
        try:
            os.remove(dest)
            return {"success": True}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
# ============================================================
# SUBSCRIPTION, AUTH, ADMIN & WEBHOOK ENDPOINTS
# ============================================================
@app.post("/api/auth/login")
async def auth_login(req: LoginRequest):
    email = req.email.strip().lower()
    if not email or "@" not in email:
        return {"success": False, "message": "Por favor, digite um e-mail válido."}

    user = sub_manager.get_user(email)
    if not user:
        return {
            "success": False,
            "error_type": "not_found",
            "message": "Conta não encontrada. Clique em 'Criar Conta' ou assine um plano na ZYNVEX GAMES para liberar o seu acesso."
        }

    # Só entra com conta ativa (VIP) ou se for administrador
    if not user.get("is_vip") and not user.get("is_admin"):
        return {
            "success": False,
            "error_type": "no_plan",
            "user": user,
            "message": "🔒 ACESSO BLOQUEADO: Sua conta existe, mas NÃO possui um plano ativo! Adquira uma assinatura VIP para desbloquear a plataforma."
        }

    return {"success": True, "user": user}


@app.post("/api/auth/register")
async def auth_register(req: LoginRequest):
    email = req.email.strip().lower()
    if not email or "@" not in email:
        return {"success": False, "message": "Por favor, digite um e-mail válido."}

    existing = sub_manager.get_user(email)
    if existing:
        if existing.get("is_vip") or existing.get("is_admin"):
            return {
                "success": True,
                "user": existing,
                "message": "Conta já cadastrada e com plano ativo! Redirecionando..."
            }
        else:
            return {
                "success": False,
                "error_type": "no_plan",
                "user": existing,
                "message": "🔒 Conta já cadastrada, mas NÃO possui um plano ativo! Adquira uma assinatura para liberar seu acesso."
            }

    # Cria a conta nova
    user = sub_manager.register_user(email, req.name or "")
    return {
        "success": False,
        "error_type": "no_plan",
        "user": user,
        "message": "✅ Conta criada com sucesso! Porém, para acessar o painel e os jogos, é necessário assinar um plano ativo. Escolha seu plano abaixo:"
    }


@app.get("/api/user/status")
async def check_user_status(email: str):
    email = email.strip().lower()
    user = sub_manager.get_user(email)
    if user and (user.get("is_vip") or user.get("is_admin")):
        return {"found": True, "user": user}
    return {
        "found": False,
        "user": None,
        "message": "Conta inativa ou não encontrada"
    }


@app.get("/api/admin/users")
async def admin_list_users():
    return {"success": True, "users": sub_manager.list_users()}


@app.post("/api/admin/activate")
async def admin_activate_user(req: AdminActivateRequest):
    game_label = "Corrida" if req.game in ("corrida_presidentes", "corrida") else "Todos os Jogos"
    plan_name = f"{req.plan.upper()} ({game_label})"
    user = sub_manager.activate_user(
        email=req.email,
        name=req.name or "",
        plan=plan_name,
        source="admin_panel",
    )
    return {"success": True, "user": user}


@app.post("/api/admin/deactivate")
async def admin_deactivate_user(req: AdminDeactivateRequest):
    user = sub_manager.deactivate_user(email=req.email)
    return {"success": True, "user": user}


@app.post("/api/webhook")
@app.post("/api/webhook/kiwify")
@app.post("/api/webhook/hotmart")
@app.post("/api/webhook/pagamento")
async def receive_payment_webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        try:
            form = await request.form()
            data = dict(form)
        except Exception:
            data = {}
    result = sub_manager.process_webhook(data)
    return result


@app.get("/api/config/supabase")
async def get_supabase_configuration():
    return sub_manager.get_supabase_config()


@app.post("/api/config/supabase")
async def set_supabase_configuration(req: SupabaseConfigRequest):
    success = sub_manager.save_supabase_config(
        url=req.supabase_url,
        key=req.supabase_key,
        enabled=req.enabled,
    )
    return {"success": success}


@app.post("/api/admin/supabase-sync")
async def trigger_supabase_sync():
    res = sub_manager.sync_all_to_supabase()
    return {"success": True, "sync_report": res}


# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global active_connector
    await ws_manager.connect(websocket)
    try:
        # Send initial snapshot on connect
        snapshot = engine.get_snapshot()
        connector_status = (
            active_connector.get_status()
            if active_connector
            else {"status": ConnectorStatus.DISCONNECTED.value, "account_id": "", "last_error": None}
        )
        snapshot["connector"] = connector_status
        await websocket.send_json({"type": "initial_state", "data": snapshot})

        while True:
            # Handle client-sent messages (e.g. admin buttons from UI)
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                action = msg.get("action")

                if action == "start":
                    await engine.start_race()
                    await websocket.send_json({"type": "race_update", "data": engine.get_snapshot()})
                elif action == "pause":
                    await engine.pause_race()
                    await websocket.send_json({"type": "race_update", "data": engine.get_snapshot()})
                elif action == "resume":
                    await engine.resume_race()
                    await websocket.send_json({"type": "race_update", "data": engine.get_snapshot()})
                elif action == "reset":
                    await engine.reset_race()
                    await websocket.send_json({"type": "race_update", "data": engine.get_snapshot()})
                elif action == "adjust_timer":
                    delta = float(msg.get("delta", 60.0))
                    snapshot = await engine.adjust_timer(delta)
                    await ws_manager.broadcast_json({"type": "race_update", "data": snapshot})
                elif action == "set_timer":
                    seconds = float(msg.get("seconds", 480.0))
                    snapshot = await engine.set_timer_duration(seconds)
                    await ws_manager.broadcast_json({"type": "race_update", "data": snapshot})
                elif action == "set_finish_mode":
                    mode = msg.get("mode", "timer")
                    snapshot = await engine.set_finish_mode(mode)
                    await ws_manager.broadcast_json({"type": "race_update", "data": snapshot})
                elif action == "set_target_distance":
                    dist = float(msg.get("distance", 1000.0))
                    snapshot = await engine.set_target_distance(dist)
                    await ws_manager.broadcast_json({"type": "race_update", "data": snapshot})
                elif action == "chat_vote":
                    team_target = msg.get("team", "red")
                    handle_chat_vote_sync(team_target, msg.get("user", "Espectador_Chat"))
                elif action == "simulate_gift":
                    mock_connector.simulate_gift(
                        gift_id=msg.get("gift_name", "rose").lower(),
                        gift_name=msg.get("gift_name", "Rose"),
                        repeat_count=int(msg.get("repeat_count", 1)),
                        user=msg.get("user", "Espectador_Demo"),
                    )
                elif action == "connect_tiktok":
                    unique_id = msg.get("unique_id", "")
                    if unique_id:
                        if active_connector:
                            await active_connector.stop()
                        await set_autopilot(False)
                        connector = TikTokConnector(
                            on_gift_callback=handle_gift_event_sync,
                            on_chat_vote_callback=handle_chat_vote_sync,
                        )
                        active_connector = connector
                        await connector.start(unique_id)
                elif action == "disconnect_tiktok":
                    if active_connector:
                        await active_connector.stop()
                        active_connector = None
                elif action == "toggle_autopilot":
                    await set_autopilot(not autopilot_enabled)
                elif action == "set_autopilot":
                    await set_autopilot(msg.get("enabled", True))
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        ws_manager.disconnect(websocket)


# Landing Page Static Files
LANDING_DIR = os.path.join(BASE_DIR, "landing")
landing_css_dir = os.path.join(LANDING_DIR, "css")
landing_js_dir = os.path.join(LANDING_DIR, "js")

if os.path.exists(landing_css_dir):
    app.mount("/landing-css", StaticFiles(directory=landing_css_dir), name="landing-css")
if os.path.exists(landing_js_dir):
    app.mount("/landing-js", StaticFiles(directory=landing_js_dir), name="landing-js")
if os.path.exists(LANDING_DIR):
    app.mount("/landing", StaticFiles(directory=LANDING_DIR, html=True), name="landing")

# Static Game Files
css_dir = os.path.join(GAME_DIR, "css")
js_dir = os.path.join(GAME_DIR, "js")
libs_dir = os.path.join(GAME_DIR, "libs")
assets_dir = os.path.join(GAME_DIR, "assets")

if os.path.exists(css_dir):
    app.mount("/css", StaticFiles(directory=css_dir), name="css")
if os.path.exists(js_dir):
    app.mount("/js", StaticFiles(directory=js_dir), name="js")
if os.path.exists(libs_dir):
    app.mount("/libs", StaticFiles(directory=libs_dir), name="libs")
if os.path.exists(assets_dir):
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

if os.path.exists(GAME_DIR):
    app.mount("/game", StaticFiles(directory=GAME_DIR, html=True), name="game")


@app.get("/")
@app.get("/portal")
@app.get("/lp")
async def serve_landing():
    landing_file = os.path.join(LANDING_DIR, "index.html")
    if os.path.exists(landing_file):
        return FileResponse(landing_file)
    index_file = os.path.join(GAME_DIR, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"message": "TikTok LIVE Gaming Platform running."}


@app.get("/dashboard")
@app.get("/painel")
async def serve_dashboard():
    dash_file = os.path.join(LANDING_DIR, "dashboard.html")
    if os.path.exists(dash_file):
        return FileResponse(dash_file)
    return {"message": "Painel do Streamer não encontrado."}


@app.get("/corrida")
@app.get("/play")
async def serve_game_direct():
    index_file = os.path.join(GAME_DIR, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"message": "Jogo de Corrida não encontrado."}
