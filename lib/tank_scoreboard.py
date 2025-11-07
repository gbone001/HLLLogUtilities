from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
import csv
import io
import os
import re

from lib import mappings
from lib.storage import LogLine
from utils import toTable

TEAM_KEYS = ("Allies", "Axis")
SCORE_PATTERN = re.compile(r"(-?\d+)\s*-\s*(-?\d+)")

TANK_ROLES = {
    "tank commander",
    "crewman",
    "gunner",
    "driver",
}


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class TankScoreConfig:
    tank_kill_points: float = 10.0
    veteran_threshold: int = 3
    veteran_bonus: float = 10.0
    ace_threshold: int = 5
    ace_bonus: float = 20.0
    mid_points_per_minute: float = 1.0
    fourth_points_per_minute: float = 1.5

    @classmethod
    def from_env(cls) -> "TankScoreConfig":
        return cls(
            tank_kill_points=_env_float("TANK_POINTS_TANK_KILL", 10.0),
            veteran_threshold=_env_int("TANK_POINTS_VETERAN_THRESHOLD", 3),
            veteran_bonus=_env_float("TANK_POINTS_VETERAN_BONUS", 10.0),
            ace_threshold=_env_int("TANK_POINTS_ACE_THRESHOLD", 5),
            ace_bonus=_env_float("TANK_POINTS_ACE_BONUS", 20.0),
            mid_points_per_minute=_env_float("TANK_POINTS_MID_PPM", 1.0),
            fourth_points_per_minute=_env_float("TANK_POINTS_FOURTH_PPM", 1.5),
        )


@dataclass
class CrewState:
    crew_id: str
    display_name: str = ""
    kills_this_life: int = 0
    veteran_awarded: bool = False
    ace_awarded: bool = False


@dataclass
class TankTeamScore:
    name: str
    tank_kills: int = 0
    veteran_crews: set[str] = field(default_factory=set)
    ace_crews: set[str] = field(default_factory=set)
    mid_seconds: float = 0.0
    fourth_seconds: float = 0.0

    def veteran_points(self, config: TankScoreConfig) -> float:
        return config.veteran_bonus * len(self.veteran_crews)

    def ace_points(self, config: TankScoreConfig) -> float:
        return config.ace_bonus * len(self.ace_crews)

    def mid_points(self, config: TankScoreConfig) -> float:
        return (self.mid_seconds / 60.0) * config.mid_points_per_minute

    def fourth_points(self, config: TankScoreConfig) -> float:
        return (self.fourth_seconds / 60.0) * config.fourth_points_per_minute

    def total_points(self, config: TankScoreConfig) -> float:
        return (
            config.tank_kill_points * self.tank_kills
            + self.veteran_points(config)
            + self.ace_points(config)
            + self.mid_points(config)
            + self.fourth_points(config)
        )

    def to_dict(self, config: TankScoreConfig) -> dict:
        return {
            "tank_kills": self.tank_kills,
            "veteran_crews": sorted(self.veteran_crews),
            "ace_crews": sorted(self.ace_crews),
            "mid_seconds": self.mid_seconds,
            "fourth_seconds": self.fourth_seconds,
            "points": self.total_points(config),
        }


@dataclass
class TankScoreboardResult:
    teams: Dict[str, TankTeamScore]
    config: TankScoreConfig
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def duration_minutes(self) -> float:
        return self.duration_seconds / 60.0

    def iter_team_stats(self) -> Iterable[TankTeamScore]:
        for key in TEAM_KEYS:
            yield self.teams.get(key, TankTeamScore(name=key))

    def to_dict(self) -> dict:
        return {
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_minutes": self.duration_minutes,
            "config": asdict(self.config),
            "teams": {
                team: stats.to_dict(self.config)
                for team, stats in self.teams.items()
            },
        }


def build_tank_scoreboard(
    logs: List[LogLine],
    *,
    config: TankScoreConfig | None = None,
) -> TankScoreboardResult:
    config = config or TankScoreConfig.from_env()
    teams = {team: TankTeamScore(name=team) for team in TEAM_KEYS}
    if not logs:
        return TankScoreboardResult(teams=teams, config=config)

    logs_sorted = sorted(logs, key=lambda log: log.event_time or datetime.min)
    start_time = logs_sorted[0].event_time
    end_time = logs_sorted[-1].event_time
    last_timestamp = start_time or end_time
    crew_states: Dict[str, CrewState] = {}

    mid_owner: Optional[str] = None
    fourth_owner: Optional[str] = None

    for log in logs_sorted:
        event_time = log.event_time or last_timestamp

        if last_timestamp and event_time:
            delta = (event_time - last_timestamp).total_seconds()
            if delta > 0:
                if mid_owner in teams:
                    teams[mid_owner].mid_seconds += delta
                if fourth_owner in teams:
                    teams[fourth_owner].fourth_seconds += delta
        last_timestamp = event_time

        etype = log.event_type
        if etype == "player_kill":
            _register_tank_kill(log, teams, crew_states, config)
            _register_death(log.player2_id, crew_states)
        elif etype == "player_teamkill":
            _register_death(log.player2_id, crew_states)
        elif etype == "player_suicide":
            _register_death(log.player_id, crew_states)
        elif etype == "team_capture_objective":
            allies_score, axis_score = _parse_score(log.message)
            if allies_score is None or axis_score is None:
                continue
            mid_owner = _mid_owner(allies_score, axis_score)
            fourth_owner = _fourth_owner(allies_score, axis_score)

    return TankScoreboardResult(
        teams=teams,
        config=config,
        start_time=start_time,
        end_time=end_time,
    )


def render_tank_scoreboard(
    result: TankScoreboardResult,
    *,
    config: TankScoreConfig | None = None,
) -> str:
    config = config or result.config
    rows = [
        ["TEAM", "Tank Kills", "Veteran Crews", "Ace Crews", "Hold Mid (min)", "Hold 4th (min)", "Points"],
    ]
    for stats in result.iter_team_stats():
        rows.append([
            stats.name,
            stats.tank_kills,
            len(stats.veteran_crews),
            len(stats.ace_crews),
            f"{stats.mid_seconds / 60.0:.2f}",
            f"{stats.fourth_seconds / 60.0:.2f}",
            f"{stats.total_points(config):.1f}",
        ])

    header = ["Tank Scoreboard"]
    if result.start_time and result.end_time:
        header.append(
            f"Window: {result.start_time.isoformat()} → {result.end_time.isoformat()} ({result.duration_minutes:.1f} min)"
        )

    details = []
    for stats in result.iter_team_stats():
        if stats.veteran_crews:
            details.append(f"{stats.name} Veteran Crews: {', '.join(sorted(stats.veteran_crews))}")
        if stats.ace_crews:
            details.append(f"{stats.name} Ace Crews: {', '.join(sorted(stats.ace_crews))}")

    scoring = (
        f"Scoring: Kill={config.tank_kill_points:+g}, "
        f"Veteran ({config.veteran_threshold}+ in life)={config.veteran_bonus:+g}, "
        f"Ace ({config.ace_threshold}+ in life)={config.ace_bonus:+g}, "
        f"Hold Mid={config.mid_points_per_minute:+g}/min, "
        f"Hold 4th={config.fourth_points_per_minute:+g}/min."
    )

    body = "\n\n".join(filter(None, [
        "\n".join(header),
        toTable(rows, title="TANK SCORING"),
        "\n".join(details) if details else "",
        scoring,
    ]))
    return body.strip()


def tank_scoreboard_to_csv(
    result: TankScoreboardResult,
    *,
    config: TankScoreConfig | None = None,
) -> str:
    config = config or result.config
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "team",
        "tank_kills",
        "veteran_crews",
        "ace_crews",
        "hold_mid_minutes",
        "hold_fourth_minutes",
        "points",
    ])
    for stats in result.iter_team_stats():
        writer.writerow([
            stats.name,
            stats.tank_kills,
            "; ".join(sorted(stats.veteran_crews)),
            "; ".join(sorted(stats.ace_crews)),
            f"{stats.mid_seconds / 60.0:.2f}",
            f"{stats.fourth_seconds / 60.0:.2f}",
            f"{stats.total_points(config):.1f}",
        ])
    return buffer.getvalue()


def _register_tank_kill(
    log: LogLine,
    teams: Dict[str, TankTeamScore],
    crew_states: Dict[str, CrewState],
    config: TankScoreConfig,
) -> None:
    team = (log.player_team or "").title()
    if team not in teams:
        return

    if not _is_tank_engagement(log):
        return

    teams[team].tank_kills += 1

    player_id = log.player_id
    if not player_id:
        return

    state = crew_states.setdefault(player_id, CrewState(crew_id=player_id))
    state.display_name = log.player_name or state.display_name or player_id
    state.kills_this_life += 1

    if state.kills_this_life >= config.veteran_threshold and not state.veteran_awarded:
        teams[team].veteran_crews.add(state.display_name)
        state.veteran_awarded = True

    if state.kills_this_life >= config.ace_threshold and not state.ace_awarded:
        teams[team].ace_crews.add(state.display_name)
        state.ace_awarded = True


def _register_death(player_id: Optional[str], crew_states: Dict[str, CrewState]) -> None:
    if not player_id:
        return
    state = crew_states.get(player_id)
    if state:
        state.kills_this_life = 0


def _is_tank_engagement(log: LogLine) -> bool:
    weapon = (log.weapon or "").strip()
    if weapon and weapon in mappings.VEHICLE_CLASSES:
        return True

    killer_role = (log.player_role or "").lower()
    victim_role = (log.player2_role or "").lower()
    if killer_role in TANK_ROLES or victim_role in TANK_ROLES:
        if log.player2_team and log.player_team and log.player2_team != log.player_team:
            return True

    if weapon:
        lower = weapon.lower()
        return any(hint in lower for hint in ("tank", "pz.", "sherman", "t-34", "tiger", "stug", "greyhound"))

    return False


def _parse_score(message: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not message:
        return None, None
    match = SCORE_PATTERN.search(message)
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _mid_owner(allies_score: int, axis_score: int) -> Optional[str]:
    # Owning mid means leading the race to the 3rd capture point.
    if allies_score >= 3 and allies_score > axis_score:
        return "Allies"
    if axis_score >= 3 and axis_score > allies_score:
        return "Axis"
    return None


def _fourth_owner(allies_score: int, axis_score: int) -> Optional[str]:
    if allies_score >= 4 and allies_score > axis_score:
        return "Allies"
    if axis_score >= 4 and axis_score > allies_score:
        return "Axis"
    return None
