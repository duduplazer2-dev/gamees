import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger("RaceEngine")


class TeamState:
    def __init__(self, key: str, name: str):
        self.key = key  # "red" or "green"
        self.name = name
        self.distance: float = 0.0
        self.points: int = 0
        self.gifts_received: int = 0
        self.boost_remaining_sec: float = 0.0
        self.speed_multiplier: float = 1.0
        self.current_speed: float = 0.0
        self.last_gift: Optional[Dict[str, Any]] = None

    def reset(self) -> None:
        self.distance = 0.0
        self.points = 0
        self.gifts_received = 0
        self.boost_remaining_sec = 0.0
        self.speed_multiplier = 1.0
        self.current_speed = 0.0
        self.last_gift = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "name": self.name,
            "distance": round(self.distance, 2),
            "points": self.points,
            "gifts_received": self.gifts_received,
            "boost_remaining_sec": round(max(0.0, self.boost_remaining_sec), 1),
            "speed_multiplier": round(self.speed_multiplier, 2),
            "current_speed": round(self.current_speed, 2),
            "last_gift": self.last_gift,
        }


class RaceEngine:
    def __init__(
        self,
        target_distance: float = 1000.0,
        base_speed: float = 3.5,
        max_speed: float = 60.0,
        auto_restart_delay_sec: float = 10.0,
        timer_duration_sec: float = 480.0,
        chat_vote_meters: float = 4.0,
        finish_mode: str = "timer",
        red_team_name: str = "Lula",
        red_team_number: str = "13",
        green_team_name: str = "Bolsonaro",
        green_team_number: str = "22",
    ):
        self.target_distance = float(target_distance)
        self.base_speed = float(base_speed)
        self.max_speed = float(max_speed)
        self.auto_restart_delay_sec = float(auto_restart_delay_sec)
        self.timer_duration_sec = float(timer_duration_sec)
        self.timer_remaining_sec = float(timer_duration_sec)
        self.chat_vote_meters = float(chat_vote_meters)
        self.finish_mode = finish_mode  # "timer" (runs full time until 00:00) | "distance" (ends at target_distance)

        self.red_team_number = red_team_number
        self.green_team_number = green_team_number

        self.teams: Dict[str, TeamState] = {
            "red": TeamState("red", red_team_name),
            "green": TeamState("green", green_team_name),
        }

        self.state: str = "WAITING"  # "WAITING", "RACING", "PAUSED", "FINISHED"
        self.winner: Optional[str] = None
        self.start_time: Optional[float] = None
        self.finish_time: Optional[float] = None
        self.elapsed_time: float = 0.0
        self.auto_restart_countdown: Optional[float] = None

        self.recent_events: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        self._last_tick_time: float = time.time()

    async def start_race(self) -> Dict[str, Any]:
        async with self._lock:
            if self.state == "FINISHED":
                self._reset_internal()
            self.state = "RACING"
            self.start_time = time.time()
            self._last_tick_time = time.time()
            self.add_event({
                "type": "race_start",
                "message": "Corrida iniciada! Acelere com presentes na LIVE!",
                "timestamp": time.time(),
            })
            logger.info("Race started.")
            return self.get_snapshot()

    async def pause_race(self) -> Dict[str, Any]:
        async with self._lock:
            if self.state == "RACING":
                self.state = "PAUSED"
                self.add_event({
                    "type": "race_pause",
                    "message": "Corrida pausada.",
                    "timestamp": time.time(),
                })
                logger.info("Race paused.")
            return self.get_snapshot()

    async def resume_race(self) -> Dict[str, Any]:
        async with self._lock:
            if self.state == "PAUSED":
                self.state = "RACING"
                self._last_tick_time = time.time()
                self.add_event({
                    "type": "race_resume",
                    "message": "Corrida retomada!",
                    "timestamp": time.time(),
                })
                logger.info("Race resumed.")
            return self.get_snapshot()

    async def reset_race(self) -> Dict[str, Any]:
        async with self._lock:
            self._reset_internal()
            self.add_event({
                "type": "race_reset",
                "message": "Nova rodada pronta para começar!",
                "timestamp": time.time(),
            })
            logger.info("Race reset to initial state.")
            return self.get_snapshot()

    def _reset_internal(self) -> None:
        self.state = "WAITING"
        self.winner = None
        self.start_time = None
        self.finish_time = None
        self.elapsed_time = 0.0
        self.auto_restart_countdown = None
        self.timer_remaining_sec = self.timer_duration_sec
        for team in self.teams.values():
            team.reset()

    async def adjust_timer(self, delta_seconds: float) -> Dict[str, Any]:
        """Adds or subtracts seconds from the current round and default timer."""
        async with self._lock:
            delta = float(delta_seconds)
            self.timer_remaining_sec = max(10.0, self.timer_remaining_sec + delta)
            self.timer_duration_sec = max(30.0, self.timer_duration_sec + delta)
            logger.info(f"Race timer adjusted by {delta:+.0f}s. Remaining: {self.timer_remaining_sec:.0f}s, Total: {self.timer_duration_sec:.0f}s")
            return self.get_snapshot()

    async def set_timer_duration(self, seconds: float) -> Dict[str, Any]:
        """Sets the race duration in seconds."""
        async with self._lock:
            sec = max(30.0, float(seconds))
            self.timer_duration_sec = sec
            if self.state in ("WAITING", "FINISHED"):
                self.timer_remaining_sec = sec
            else:
                self.timer_remaining_sec = min(self.timer_remaining_sec, sec)
            logger.info(f"Race default timer set to {sec:.0f}s")
            return self.get_snapshot()

    async def set_finish_mode(self, mode: str) -> Dict[str, Any]:
        """Sets the race finish mode: 'timer' (full timer until 00:00) or 'distance' (ends at target distance)."""
        async with self._lock:
            if mode in ("timer", "distance"):
                self.finish_mode = mode
                logger.info(f"Race finish mode changed to: {mode}")
            return self.get_snapshot()

    async def set_target_distance(self, distance: float) -> Dict[str, Any]:
        """Sets the target distance in meters (e.g. 1000m, 3000m, 5000m, 10000m)."""
        async with self._lock:
            dist = max(100.0, float(distance))
            self.target_distance = dist
            logger.info(f"Race target distance changed to: {dist:.0f}m")
            return self.get_snapshot()

    async def apply_gift_boost(self, gift_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Applies a validated gift boost to the corresponding team.
        """
        async with self._lock:
            target_team_key = gift_data.get("target_team")
            if target_team_key not in self.teams:
                logger.warning(f"Invalid target team key: {target_team_key}")
                return None

            # If race is WAITING or FINISHED, receiving a gift immediately starts fresh round!
            if self.state in ("WAITING", "FINISHED"):
                if self.state == "FINISHED":
                    self._reset_internal()
                self.state = "RACING"
                self.start_time = time.time()
                self._last_tick_time = time.time()
                self.add_event({
                    "type": "race_start",
                    "message": "Corrida iniciada pelo presente!",
                    "timestamp": time.time(),
                })

            if self.state != "RACING":
                logger.info(f"Gift received but race is in state '{self.state}'. Boost queued or skipped.")
                return None

            team = self.teams[target_team_key]
            meters = float(gift_data.get("meters", 0.0))
            boost_dur = float(gift_data.get("boost_duration_sec", 3.0))
            multiplier = float(gift_data.get("speed_multiplier", 1.5))
            units = int(gift_data.get("awarded_units", 1))

            # Apply distance impulse (unclamped in timer mode so runners can compete for the full race duration!)
            if self.finish_mode == "timer":
                team.distance += meters
            else:
                team.distance = min(self.target_distance, team.distance + meters)
            team.points += units
            team.gifts_received += 1
            # Add or extend speed boost (bonus combo speed multiplier!)
            combo_mult = min(15.0, max(multiplier, 10.0 + max(0, units - 1) * 0.5))
            team.boost_remaining_sec = max(team.boost_remaining_sec + boost_dur * 0.75, boost_dur)
            team.speed_multiplier = max(team.speed_multiplier, combo_mult)
            team.last_gift = {
                "gift_name": gift_data.get("gift_name"),
                "user": gift_data.get("user"),
                "units": units,
                "meters": meters,
                "timestamp": time.time(),
            }

            event_entry = {
                "type": "gift_boost",
                "team": target_team_key,
                "team_name": team.name,
                "user": gift_data.get("user"),
                "gift_name": gift_data.get("gift_name"),
                "units": units,
                "meters": meters,
                "timestamp": time.time(),
            }
            self.add_event(event_entry)

            # Check if this gift crossed the finish line
            self._check_winner_under_lock()

            return event_entry

    async def apply_chat_vote(self, team_key: str, user: str = "Espectador", vote_count: int = 1) -> Optional[Dict[str, Any]]:
        """
        Applies a chat vote (e.g. typing '13' or '22' in chat).
        """
        async with self._lock:
            if team_key not in self.teams:
                return None

            if self.state in ("WAITING", "FINISHED"):
                if self.state == "FINISHED":
                    self._reset_internal()
                self.state = "RACING"
                self.start_time = time.time()
                self._last_tick_time = time.time()
                self.add_event({
                    "type": "race_start",
                    "message": "Corrida iniciada pelos votos do chat!",
                    "timestamp": time.time(),
                })

            if self.state != "RACING":
                return None

            team = self.teams[team_key]
            reps = max(1, min(10, int(vote_count)))
            gain_meters = self.chat_vote_meters * reps
            if self.finish_mode == "timer":
                team.distance += gain_meters
            else:
                team.distance = min(self.target_distance, team.distance + gain_meters)
            team.points += reps

            # Speed boost so the runner visibly accelerates to 30-40 KM/H when people vote in chat!
            team.boost_remaining_sec = min(5.0, max(1.6, 1.6 + 0.3 * (reps - 1)))
            team.speed_multiplier = min(10.0, 7.5 + (reps - 1) * 0.5)

            num = self.red_team_number if team_key == "red" else self.green_team_number
            event_entry = {
                "type": "chat_vote",
                "team": team_key,
                "team_name": team.name,
                "number": num,
                "user": user,
                "meters": gain_meters,
                "count": reps,
                "timestamp": time.time(),
            }
            self.add_event(event_entry)
            self._check_winner_under_lock()
            return event_entry

    def _check_winner_under_lock(self, time_up: bool = False) -> None:
        if self.state != "RACING":
            return

        red_team = self.teams["red"]
        green_team = self.teams["green"]

        if self.finish_mode == "timer":
            # In timer mode, the race runs for the ENTIRE duration and never ends early from distance!
            if not time_up:
                return

            if red_team.distance > green_team.distance:
                self.winner = "red"
            elif green_team.distance > red_team.distance:
                self.winner = "green"
            else:
                self.winner = "red" if red_team.points >= green_team.points else "green"
        else:
            red_finished = red_team.distance >= self.target_distance
            green_finished = green_team.distance >= self.target_distance

            if time_up:
                if red_team.distance > green_team.distance:
                    self.winner = "red"
                elif green_team.distance > red_team.distance:
                    self.winner = "green"
                else:
                    self.winner = "red" if red_team.points >= green_team.points else "green"
            elif red_finished and green_finished:
                if red_team.distance > green_team.distance:
                    self.winner = "red"
                elif green_team.distance > red_team.distance:
                    self.winner = "green"
                else:
                    self.winner = "red" if red_team.points >= green_team.points else "green"
            elif red_finished:
                self.winner = "red"
            elif green_finished:
                self.winner = "green"

        if self.winner:
            self.state = "FINISHED"
            self.finish_time = time.time()
            if self.start_time:
                self.elapsed_time = self.finish_time - self.start_time
            if self.auto_restart_delay_sec > 0:
                self.auto_restart_countdown = self.auto_restart_delay_sec

            winner_name = self.teams[self.winner].name
            reason = "Tempo esgotado!" if time_up else "Linha de chegada alcançada!"
            self.add_event({
                "type": "race_finish",
                "winner": self.winner,
                "winner_name": winner_name,
                "elapsed_time": round(self.elapsed_time, 1),
                "message": f"Vitória de {winner_name}! {reason}",
                "timestamp": time.time(),
            })
            logger.info(f"Race finished! Winner: {winner_name} ({reason}) in {self.elapsed_time:.1f}s")

    async def update(self, dt: float) -> Dict[str, Any]:
        async with self._lock:
            now = time.time()

            if self.state == "RACING":
                if self.start_time:
                    self.elapsed_time = now - self.start_time

                # Timer countdown
                time_up = False
                if self.timer_remaining_sec > 0:
                    self.timer_remaining_sec = max(0.0, self.timer_remaining_sec - dt)
                    if self.timer_remaining_sec <= 0:
                        time_up = True

                for team in self.teams.values():
                    if team.boost_remaining_sec > 0:
                        team.boost_remaining_sec -= dt
                        active_multiplier = team.speed_multiplier
                    else:
                        if team.speed_multiplier > 1.0:
                            team.speed_multiplier = max(1.0, team.speed_multiplier - dt * 4.0)
                        active_multiplier = team.speed_multiplier

                    team.current_speed = min(self.max_speed, self.base_speed * active_multiplier)
                    if self.finish_mode == "timer":
                        team.distance += (team.current_speed * dt)
                    else:
                        team.distance = min(self.target_distance, team.distance + (team.current_speed * dt))

                self._check_winner_under_lock(time_up=time_up)

            elif self.state == "FINISHED":
                if self.auto_restart_countdown is not None and self.auto_restart_countdown > 0:
                    self.auto_restart_countdown -= dt
                    if self.auto_restart_countdown <= 0:
                        logger.info("Auto-restart timer triggered. Starting fresh race.")
                        self._reset_internal()
                        self.state = "RACING"
                        self.start_time = time.time()
                        self._last_tick_time = time.time()
                        self.add_event({
                            "type": "race_start",
                            "message": "Nova rodada iniciada automaticamente!",
                            "timestamp": time.time(),
                        })

            return self.get_snapshot()

    def add_event(self, event_data: Dict[str, Any]) -> None:
        self.recent_events.append(event_data)
        if len(self.recent_events) > 20:
            self.recent_events.pop(0)

    def get_snapshot(self) -> Dict[str, Any]:
        red_team = self.teams["red"]
        green_team = self.teams["green"]
        target = max(100.0, self.target_distance)
        current_goal = max(target, max(red_team.distance, green_team.distance))
        red_progress = min(1.0, max(0.03, red_team.distance / current_goal))
        green_progress = min(1.0, max(0.03, green_team.distance / current_goal))

        # Rankings
        red_leads = red_team.distance >= green_team.distance
        red_rank = 1 if red_leads else 2
        green_rank = 2 if red_leads else 1

        # Format countdown timer MM:SS
        total_sec = int(self.timer_remaining_sec)
        timer_str = f"{total_sec // 60:02d}:{total_sec % 60:02d}"

        # Gap between racers
        gap = round(abs(red_team.distance - green_team.distance), 1)

        return {
            "state": self.state,
            "winner": self.winner,
            "finish_mode": self.finish_mode,
            "target_distance": self.target_distance,
            "elapsed_time": round(self.elapsed_time, 1),
            "timer_formatted": timer_str,
            "timer_remaining_sec": round(self.timer_remaining_sec, 1),
            "timer_duration_sec": round(self.timer_duration_sec, 1),
            "gap_meters": gap,
            "auto_restart_countdown": round(max(0.0, self.auto_restart_countdown or 0.0), 1) if self.auto_restart_countdown else None,
            "teams": {
                "red": {
                    **red_team.to_dict(),
                    "number": self.red_team_number,
                    "rank": red_rank,
                    "kmh": round(red_team.current_speed * 3.6),
                },
                "green": {
                    **green_team.to_dict(),
                    "number": self.green_team_number,
                    "rank": green_rank,
                    "kmh": round(green_team.current_speed * 3.6),
                },
            },
            "progress": {
                "red": round(red_progress * 100, 1),
                "green": round(green_progress * 100, 1),
            },
            "recent_events": list(self.recent_events[-10:]),
            "timestamp": time.time(),
        }
