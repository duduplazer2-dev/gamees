import json
import logging
import os
import time
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("RaceGiftProcessor")


def normalize_text(text: str) -> str:
    """Normalizes string for robust matching: lowercase, trimmed, no accents."""
    if not text:
        return ""
    text = text.strip().lower()
    # Normalize unicode to remove diacritics
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


class GiftRule:
    def __init__(
        self,
        rule_id: str,
        name: str,
        aliases: List[str],
        target_team: str,
        meters_per_unit: float,
        boost_duration_sec: float = 3.0,
        speed_multiplier: float = 1.5,
        enabled: bool = True,
        description: str = "",
        emoji: str = "🎁",
        icon: str = "",
        percentage: str = "",
        votes: int = 0,
    ):
        self.rule_id = rule_id
        self.name = name
        self.raw_aliases = list(aliases) if aliases else []
        self.aliases = [normalize_text(a) for a in self.raw_aliases]
        self.aliases.append(normalize_text(name))
        self.aliases.append(normalize_text(rule_id))
        self.target_team = target_team.lower()  # "red", "green", "alternate"
        self.meters_per_unit = float(meters_per_unit)
        self.boost_duration_sec = float(boost_duration_sec)
        self.speed_multiplier = float(speed_multiplier)
        self.enabled = bool(enabled)
        self.description = description
        self.emoji = str(emoji or "🎁")
        self.icon = str(icon or "")
        self.percentage = str(percentage or "")
        self.votes = int(votes or 0)

    def matches(self, gift_identifier: str) -> bool:
        normalized = normalize_text(str(gift_identifier))
        return normalized in self.aliases

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.rule_id,
            "name": self.name,
            "aliases": self.raw_aliases,
            "target_team": self.target_team,
            "meters_per_unit": self.meters_per_unit,
            "boost_duration_sec": self.boost_duration_sec,
            "speed_multiplier": self.speed_multiplier,
            "enabled": self.enabled,
            "description": self.description,
            "emoji": self.emoji,
            "icon": self.icon,
            "percentage": self.percentage,
            "votes": self.votes,
        }


class GiftProcessor:
    def __init__(self, config_path: str = "config/gifts.json"):
        self.config_path = config_path
        self.rules: List[GiftRule] = []
        self.unknown_log_event: bool = True
        self.unknown_award_points: bool = True  # Nunca descarta presente: se TikTok enviar presente novo, pontua automaticamente!
        self.last_alternate_team: str = "green"  # Starts toggling to "red" first
        
        # Streak tracking: dict of (user, gift_id, streak_id) -> (last_count, last_update_time)
        self._streak_cache: Dict[str, Tuple[int, float]] = {}
        # Event deduplication cache: dict of event_hash -> timestamp
        self._dedup_cache: Dict[str, float] = {}
        
        self.load_config()

    def load_config(self) -> None:
        """Loads or reloads rules from gifts.json."""
        if not os.path.exists(self.config_path):
            logger.warning(f"Config file {self.config_path} not found. Using default rules.")
            self._load_fallback_rules()
            return

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            policy = data.get("unknown_gift_policy", {})
            self.unknown_log_event = policy.get("log_event", True)
            self.unknown_award_points = policy.get("award_points", False)

            loaded_rules = []
            for item in data.get("rules", []):
                rule = GiftRule(
                    rule_id=str(item.get("id", "")),
                    name=str(item.get("name", "")),
                    aliases=item.get("aliases", []),
                    target_team=str(item.get("target_team", "alternate")),
                    meters_per_unit=float(item.get("meters_per_unit", 5.0)),
                    boost_duration_sec=float(item.get("boost_duration_sec", 3.0)),
                    speed_multiplier=float(item.get("speed_multiplier", 1.5)),
                    enabled=bool(item.get("enabled", True)),
                    description=str(item.get("description", "")),
                    emoji=str(item.get("emoji", "🎁")),
                    icon=str(item.get("icon", "")),
                    percentage=str(item.get("percentage", "")),
                    votes=int(item.get("votes", 0)),
                )
                loaded_rules.append(rule)

            self.rules = loaded_rules
            logger.info(f"Loaded {len(self.rules)} gift rules from {self.config_path}")
        except Exception as e:
            logger.error(f"Error loading gift config from {self.config_path}: {e}. Fallback to default.")
            self._load_fallback_rules()

    def save_config(self, rules_data: List[Dict[str, Any]]) -> None:
        """Saves updated rules into gifts.json and reloads them."""
        payload = {
            "unknown_gift_policy": {
                "log_event": self.unknown_log_event,
                "award_points": self.unknown_award_points,
            },
            "rules": rules_data,
        }
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        self.load_config()

    def _load_fallback_rules(self) -> None:
        self.rules = [
            GiftRule("rose", "Rose", ["rosa", "rose", "5655"], "red", 5.0, 3.0, 1.5, True, "Equipe Vermelha", "🌹"),
            GiftRule("tiktok", "TikTok", ["tiktok", "logo tiktok", "5269"], "green", 5.0, 3.0, 1.5, True, "Equipe Verde", "🎵"),
            GiftRule("heart", "Heart", ["coracao", "coração", "heart"], "red", 20.0, 4.5, 2.0, True, "Equipe Vermelha Super", "💖"),
            GiftRule("cap", "Cap", ["bone", "boné", "cap"], "green", 20.0, 4.5, 2.0, True, "Equipe Verde Super", "🧢"),
            GiftRule("fireworks", "Fireworks", ["fogos", "fireworks"], "alternate", 50.0, 6.0, 2.5, True, "Alternado", "🎆"),
        ]

    def _cleanup_caches(self) -> None:
        now = time.time()
        # Clean dedup cache older than 300 seconds
        expired_dedup = [k for k, t in self._dedup_cache.items() if now - t > 300]
        for k in expired_dedup:
            del self._dedup_cache[k]

        # Clean streak cache older than 120 seconds
        expired_streaks = [k for k, (_, t) in self._streak_cache.items() if now - t > 120]
        for k in expired_streaks:
            del self._streak_cache[k]

    def find_rule(self, gift_identifier: str) -> Optional[GiftRule]:
        if not gift_identifier:
            return None
        norm = normalize_text(str(gift_identifier))
        clean = "".join(c for c in norm if c.isalnum())
        # 1. Exact match
        for rule in self.rules:
            if rule.matches(norm):
                return rule
        # 2. Substring or clean match
        if clean:
            for rule in self.rules:
                for alias in rule.aliases:
                    clean_alias = "".join(c for c in alias if c.isalnum())
                    if clean_alias and (clean_alias == clean or clean_alias in clean or clean in clean_alias):
                        return rule
        return None

    def process_event(
        self,
        gift_id: Any,
        gift_name: str,
        repeat_count: int = 1,
        user: str = "Anônimo",
        streak_id: Optional[str] = None,
        is_repeat_end: bool = False,
        event_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Processes a raw gift event, handles deduplication, streak counting,
        and returns the applied game result.
        """
        self._cleanup_caches()
        now = time.time()
        repeat_count = max(1, int(repeat_count))
        user_str = str(user).strip() or "Anônimo"
        gift_id_str = str(gift_id)
        gift_name_str = str(gift_name).strip()

        # Deduplication check by explicit event_id if provided
        if event_id:
            dedup_key = f"evt:{event_id}"
            if dedup_key in self._dedup_cache:
                logger.debug(f"Ignoring duplicate event by event_id: {event_id}")
                return {
                    "valid": False,
                    "reason": "duplicate_event_id",
                    "awarded_units": 0,
                    "meters": 0.0,
                    "target_team": None,
                }
            self._dedup_cache[dedup_key] = now

        # Streak / Combo deduplication calculation:
        # Streak / Combo deduplication calculation:
        # Prevents double-counting only when TikTok reports an ongoing combo streak (streak_id).
        # Regular gifts without streak_id are independent events and ALWAYS award units!
        units_to_award = 0

        if streak_id:
            combo_key = f"{user_str}:{gift_id_str or gift_name_str}:{streak_id}"
            if combo_key in self._streak_cache:
                last_count, last_time = self._streak_cache[combo_key]
                if repeat_count > last_count:
                    units_to_award = repeat_count - last_count
                    self._streak_cache[combo_key] = (repeat_count, now)
                else:
                    logger.debug(f"Streak {combo_key} count {repeat_count} <= {last_count}. Skipped duplicate repeat.")
                    return {
                        "valid": False,
                        "reason": "streak_already_counted",
                        "awarded_units": 0,
                        "meters": 0.0,
                        "target_team": None,
                    }
            else:
                units_to_award = repeat_count
                self._streak_cache[combo_key] = (repeat_count, now)
        else:
            # Independent gift (no streak_id): always award full count!
            units_to_award = repeat_count

        if units_to_award <= 0:
            return {
                "valid": False,
                "reason": "streak_already_counted",
                "awarded_units": 0,
                "meters": 0.0,
                "target_team": None,
            }

        # Look up rule
        rule = self.find_rule(gift_name_str)
        if not rule and gift_id_str:
            rule = self.find_rule(gift_id_str)

        if not rule:
            if self.unknown_log_event:
                logger.info(f"[UNKNOWN GIFT] Received '{gift_name_str}' (id: {gift_id_str}) from {user_str} x{units_to_award}.")
            if self.unknown_award_points:
                target_team = "red" if self.last_alternate_team == "green" else "green"
                self.last_alternate_team = target_team
                meters = 10.0 * units_to_award
                return {
                    "valid": True,
                    "reason": "applied",
                    "gift_id": gift_id_str or "custom",
                    "gift_name": gift_name_str or "Presente",
                    "user": user_str,
                    "awarded_units": units_to_award,
                    "total_streak_count": repeat_count,
                    "target_team": target_team,
                    "meters": meters,
                    "boost_duration_sec": 3.0,
                    "speed_multiplier": 1.6,
                    "timestamp": now,
                }
            return {
                "valid": False,
                "reason": "unknown_gift",
                "gift_name": gift_name_str,
                "gift_id": gift_id_str,
                "user": user_str,
                "awarded_units": 0,
                "meters": 0.0,
                "target_team": None,
            }

        if not rule.enabled:
            logger.info(f"[DISABLED RULE] Gift '{rule.name}' is disabled in configuration.")
            return {
                "valid": False,
                "reason": "rule_disabled",
                "gift_name": rule.name,
                "gift_id": gift_id_str,
                "user": user_str,
                "awarded_units": 0,
                "meters": 0.0,
                "target_team": None,
            }

        # Resolve target team (handle alternate)
        target_team = rule.target_team
        if target_team == "alternate":
            target_team = "red" if self.last_alternate_team == "green" else "green"
            self.last_alternate_team = target_team

        meters = rule.meters_per_unit * units_to_award

        logger.info(
            f"[GIFT APPLIED] {user_str} sent {rule.name} x{units_to_award} -> "
            f"Team: {target_team.upper()} (+{meters:.1f}m, boost {rule.speed_multiplier}x for {rule.boost_duration_sec}s)"
        )

        return {
            "valid": True,
            "reason": "applied",
            "gift_id": rule.rule_id,
            "gift_name": rule.name,
            "emoji": rule.emoji,
            "icon": rule.icon,
            "user": user_str,
            "awarded_units": units_to_award,
            "total_streak_count": repeat_count,
            "target_team": target_team,
            "meters": meters,
            "boost_duration_sec": rule.boost_duration_sec,
            "speed_multiplier": rule.speed_multiplier,
            "timestamp": now,
        }
