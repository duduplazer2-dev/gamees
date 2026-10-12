import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional
import httpx

logger = logging.getLogger("SubscriptionManager")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "config", "users.db")
SUPABASE_CONFIG_PATH = os.path.join(BASE_DIR, "config", "supabase.json")


class SubscriptionManager:
    def __init__(self):
        self._init_db()
        self._init_supabase_config()

    def _init_db(self):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    email TEXT PRIMARY KEY,
                    name TEXT,
                    is_vip INTEGER DEFAULT 0,
                    plan TEXT DEFAULT 'free',
                    source TEXT DEFAULT 'manual',
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            conn.commit()

        # Adiciona um usuário admin padrão se vazio
        self.ensure_default_admin()

    def ensure_default_admin(self):
        admin_email = "admin@zynvexgames.com.br"
        user = self.get_user(admin_email)
        if not user:
            self.activate_user(admin_email, name="Administrador", plan="vitalicio", source="system")

    def _init_supabase_config(self):
        if not os.path.exists(SUPABASE_CONFIG_PATH):
            default_cfg = {
                "supabase_url": "",
                "supabase_key": "",
                "enabled": False,
                "table_name": "subscribers"
            }
            try:
                with open(SUPABASE_CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(default_cfg, f, indent=2)
            except Exception as e:
                logger.error(f"Erro ao salvar config do Supabase: {e}")

    def get_supabase_config(self) -> Dict[str, Any]:
        if os.path.exists(SUPABASE_CONFIG_PATH):
            try:
                with open(SUPABASE_CONFIG_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {"supabase_url": "", "supabase_key": "", "enabled": False, "table_name": "subscribers"}

    def save_supabase_config(self, url: str, key: str, enabled: bool) -> bool:
        cfg = {
            "supabase_url": url.strip(),
            "supabase_key": key.strip(),
            "enabled": enabled,
            "table_name": "subscribers"
        }
        try:
            with open(SUPABASE_CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
            
            # Tenta sincronizar se tiver habilitado
            if enabled and url and key:
                self.sync_all_to_supabase()
            return True
        except Exception as e:
            logger.error(f"Erro salvando config Supabase: {e}")
            return False

    def sync_to_supabase(self, user: Dict[str, Any]):
        """Sincroniza um usuário diretamente na tabela do Supabase REST API (upsert)"""
        cfg = self.get_supabase_config()
        if not cfg.get("enabled") or not cfg.get("supabase_url") or not cfg.get("supabase_key"):
            return
        
        url = cfg["supabase_url"].rstrip("/") + f"/rest/v1/{cfg.get('table_name', 'subscribers')}"
        headers = {
            "apikey": cfg["supabase_key"],
            "Authorization": f"Bearer {cfg['supabase_key']}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }
        payload = {
            "email": user["email"],
            "name": user.get("name", ""),
            "is_vip": user.get("is_vip", False),
            "plan": user.get("plan", "free"),
            "source": user.get("source", "app"),
            "updated_at": user.get("updated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        }
        try:
            with httpx.Client(timeout=4.0) as client:
                res = client.post(url, headers=headers, json=payload)
                if res.status_code in (200, 201):
                    logger.info(f"Usuário {user['email']} sincronizado com sucesso no Supabase!")
                    return {"success": True, "email": user["email"]}
                else:
                    logger.warning(f"Resposta Supabase ({res.status_code}): {res.text[:200]}")
                    return {"success": False, "status": res.status_code, "error": res.text, "email": user["email"]}
        except Exception as e:
            logger.warning(f"Não foi possível sincronizar com Supabase agora: {e}")
            return {"success": False, "error": str(e), "email": user["email"]}

    def sync_all_to_supabase(self) -> Dict[str, Any]:
        """Sincroniza todos os usuários locais com o Supabase"""
        users = self.list_users()
        results = []
        for u in users:
            r = self.sync_to_supabase(u)
            if r:
                results.append(r)
        success_count = sum(1 for r in results if r.get("success"))
        return {"total": len(users), "synced": success_count, "results": results}

    def activate_user(self, email: str, name: str = "", plan: str = "vip", source: str = "admin") -> Dict[str, Any]:
        email = email.strip().lower()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT email, created_at FROM users WHERE email = ?", (email,))
            row = cursor.fetchone()
            if row:
                created_at = row[1]
                cursor.execute("""
                    UPDATE users 
                    SET is_vip = 1, plan = ?, source = ?, updated_at = ?, name = CASE WHEN ? != '' THEN ? ELSE name END
                    WHERE email = ?
                """, (plan, source, now, name, name, email))
            else:
                created_at = now
                display_name = name or email.split("@")[0].capitalize()
                cursor.execute("""
                    INSERT INTO users (email, name, is_vip, plan, source, created_at, updated_at)
                    VALUES (?, ?, 1, ?, ?, ?, ?)
                """, (email, display_name, plan, source, created_at, now))
            conn.commit()

        logger.info(f"Usuário ATIVADO com sucesso: {email} ({plan}) via {source}")
        user = self.get_user(email)
        if user:
            self.sync_to_supabase(user)
        return user

    def register_user(self, email: str, name: str = "") -> Dict[str, Any]:
        email = email.strip().lower()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT email, created_at, is_vip, plan FROM users WHERE email = ?", (email,))
            row = cursor.fetchone()
            if row:
                # Se já existe, mantém status
                pass
            else:
                display_name = name or email.split("@")[0].capitalize()
                cursor.execute("""
                    INSERT INTO users (email, name, is_vip, plan, source, created_at, updated_at)
                    VALUES (?, ?, 0, 'free', 'register', ?, ?)
                """, (email, display_name, now, now))
                conn.commit()

        user = self.get_user(email)
        if user:
            self.sync_to_supabase(user)
        return user

    def deactivate_user(self, email: str) -> Dict[str, Any]:
        email = email.strip().lower()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE users 
                SET is_vip = 0, plan = 'free', updated_at = ?
                WHERE email = ?
            """, (now, email))
            conn.commit()
        
        logger.info(f"Usuário BLOQUEADO/DESATIVADO: {email}")
        user = self.get_user(email)
        if user:
            self.sync_to_supabase(user)
        return user

    ADMIN_EMAILS = {"admin@zynvexgames.com.br", "admin@zevonlive.com.br", "duduplazar@gmail.com"}

    def is_admin(self, email: str, source: str = "") -> bool:
        email = email.strip().lower()
        return email in self.ADMIN_EMAILS or source == "system" or email.startswith("admin@")

    def get_user(self, email: str) -> Optional[Dict[str, Any]]:
        email = email.strip().lower()
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE email = ?", (email,))
            row = cursor.fetchone()
            if row:
                is_adm = self.is_admin(row["email"], row["source"] or "")
                return {
                    "email": row["email"],
                    "name": row["name"] or row["email"].split("@")[0].capitalize(),
                    "is_vip": bool(row["is_vip"]),
                    "is_admin": is_adm,
                    "plan": row["plan"],
                    "source": row["source"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                }
        return None

    def list_users(self) -> List[Dict[str, Any]]:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users ORDER BY updated_at DESC")
            rows = cursor.fetchall()
            return [
                {
                    "email": r["email"],
                    "name": r["name"] or r["email"].split("@")[0].capitalize(),
                    "is_vip": bool(r["is_vip"]),
                    "is_admin": self.is_admin(r["email"], r["source"] or ""),
                    "plan": r["plan"],
                    "source": r["source"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"]
                }
                for r in rows
            ]

    def process_webhook(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Interpreta payloads de webhook de qualquer plataforma (Kiwify, Hotmart, PerfectPay, Asaas, etc)
        Quando cliente comprar: libera o acesso imediato (is_vip = 1)
        Quando pedir reembolso / cancelamento / chargeback: BLOQUEIA IMEDIATAMENTE (is_vip = 0)
        """
        logger.info(f"Recebendo Webhook de Pagamento: {json.dumps(data, default=str)[:300]}")
        
        email = None
        name = ""
        status = ""
        source = "webhook"

        # Kiwify (Customer maiúsculo ou minúsculo)
        if "Customer" in data and isinstance(data["Customer"], dict):
            email = data["Customer"].get("email")
            name = data["Customer"].get("full_name", "") or data["Customer"].get("name", "")
            status = data.get("order_status", "")
            source = "kiwify"
        elif "customer" in data and isinstance(data["customer"], dict):
            email = data["customer"].get("email")
            name = data["customer"].get("full_name", "") or data["customer"].get("name", "")
            status = data.get("order_status") or data.get("status", "")
            source = "kiwify"
        # Hotmart
        elif "data" in data and isinstance(data["data"], dict) and "buyer" in data["data"]:
            buyer = data["data"]["buyer"]
            email = buyer.get("email")
            name = buyer.get("name", "")
            status = data.get("event") or data["data"].get("status", "")
            source = "hotmart"
        elif "buyer" in data and isinstance(data["buyer"], dict):
            email = data["buyer"].get("email")
            name = data["buyer"].get("name", "")
            status = data.get("event") or data.get("status", "")
            source = "hotmart"
        # Supabase Webhook
        elif "record" in data and isinstance(data["record"], dict):
            rec = data["record"]
            email = rec.get("email")
            name = rec.get("name", "")
            status = rec.get("status", "paid")
            source = "supabase"
        # Genérico / Formato simples
        else:
            email = data.get("email") or data.get("customer_email") or data.get("buyer_email") or data.get("payer_email")
            name = data.get("name") or data.get("customer_name") or data.get("buyer_name") or ""
            status = data.get("status") or data.get("order_status") or data.get("event") or data.get("current_status") or "paid"
            source = data.get("platform") or "universal_webhook"

        if not email:
            return {"status": "ignored", "reason": "E-mail não encontrado no payload"}

        status_lower = str(status).lower()

        # Palavras-chave de REEMBOLSO / CANCELAMENTO / CHARGEBACK / ESTORNO (BLOQUEIA IMEDIATAMENTE)
        refund_keywords = [
            "refund", "refunded", "reembolso", "reembolsado",
            "chargeback", "chargedback", "dispute", "disputa",
            "canceled", "cancelled", "cancelado", "cancelamento",
            "subscription_canceled", "expired", "bloqueado",
            "estorno", "estornado", "devolvido", "devolucao", "devolvida", "contestacao"
        ]
        is_refund = any(k in status_lower for k in refund_keywords)

        # Palavras-chave de COMPRA APROVADA / PAGA (LIBERA ACESSO IMEDIATAMENTE)
        paid_keywords = [
            "paid", "approved", "completed", "pago", "aprovado",
            "purchase_approved", "subscription_active", "active", "concluido",
            "liquidado", "autorizado"
        ]
        is_paid = any(k in status_lower for k in paid_keywords) and not is_refund

        if is_refund:
            user = self.deactivate_user(email)
            logger.warning(f"🚨 ACESSO BLOQUEADO POR REEMBOLSO/CANCELAMENTO: {email} (status: {status})")
            return {
                "status": "success",
                "action": "blocked_refund",
                "message": f"Acesso do e-mail {email} foi bloqueado imediatamente devido a reembolso/cancelamento.",
                "user": user
            }
        elif is_paid:
            user = self.activate_user(email, name=name, plan="vip", source=source)
            logger.info(f"✅ ACESSO VIP LIBERADO: {email} (status: {status})")
            return {
                "status": "success",
                "action": "activated",
                "message": f"Acesso VIP liberado com sucesso para {email}!",
                "user": user
            }
        else:
            return {
                "status": "received",
                "action": "ignored_status",
                "message": f"Status '{status}' registrado sem alteração de permissão.",
                "order_status": status
            }


sub_manager = SubscriptionManager()
