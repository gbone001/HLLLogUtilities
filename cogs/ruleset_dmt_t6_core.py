# cogs/ruleset_dmt_t6_core.py
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any

@dataclass
class DmtT6Config:
    discord_channel_id: int
    precombat_hold_time: str  # "1:15:00"
    require_mode_warfare: bool
    suspect_heavy_weapons: List[str]
    technician_role_names: List[str]
    fourth_cap_illegal: bool
    commander_max_pstrikes: int
    allowed_commander_abilities: set[str]
    playable_maps_whitelist: List[str]

@dataclass
class Violation:
    code: str
    human: str
    evidence: Optional[str] = None
    severity: str = "major"  # info|minor|major|critical

@dataclass
class IngestContext:
    now: datetime
    match_start_time: Optional[datetime] = None
    mode: Optional[str] = None
    center_point: Optional[str] = None
    commander_ability_uses: Dict[str, int] = field(default_factory=dict)
    crew_combat_scores: Dict[str, int] = field(default_factory=dict)

class DmtScorecard:
    \"\"\"Computes unofficial DMT totals: 3x(sum of crew max CS) + Cmdr P-Strike CS + 0.5*mid_cap_seconds.\"\"\"
    def __init__(self):
        self._crew_max_by_crew: Dict[str, int] = {}
        self._cmdr_pstrike_cs: int = 0
        self._mid_hold_seconds: int = 0

    def ingest_event(self, e: Dict[str, Any], ctx: IngestContext):
        et = (e.get("event_type") or "").lower()
        # Example: track combat score peaks (you'll need to adapt to your actual score event schema)
        if et == "player_score_update" and (e.get("player_role","").lower() in ("tank commander","crewman","gunner","driver")):
            crew = e.get("team_name") or "UNK"
            try:
                cs = int(e.get("player_combat_score") or 0)
            except Exception:
                cs = 0
            self._crew_max_by_crew[crew] = max(self._crew_max_by_crew.get(crew, 0), cs)

        if et == "commander_ability" and (e.get("message","").startswith("precision_strike")):
            try:
                self._cmdr_pstrike_cs += int(e.get("new") or 0)
            except Exception:
                pass

        if et == "mid_hold_tick":
            try:
                self._mid_hold_seconds += int(e.get("new") or 1)
            except Exception:
                self._mid_hold_seconds += 1

    def render_unofficial(self) -> str:
        crew_sum = sum(self._crew_max_by_crew.values())
        total_combat = 3 * crew_sum + self._cmdr_pstrike_cs
        total = total_combat + int(self._mid_hold_seconds * 0.5)
        return (
            f\"DMT Interim\\n\"
            f\"  Crew highs (sum): {crew_sum}\\n\"
            f\"  Cmdr P-Strike CS: {self._cmdr_pstrike_cs}\\n\"
            f\"  TOTAL COMBAT = 3 * {crew_sum} + {self._cmdr_pstrike_cs} = {total_combat}\\n\"
            f\"  Mid hold secs = {self._mid_hold_seconds}  => +{self._mid_hold_seconds*0.5}\\n\"
            f\"  >>> DMT TOTAL TEAM SCORE (unofficial) = {total}\"
        )

class DmtT6Evaluator:
    def __init__(self, cfg: DmtT6Config):
        self.cfg = cfg

    def _parse_hhmmss(self, text: str) -> int:
        h, m, s = (int(x) for x in text.split(":"))
        return 3600*h + 60*m + s

    def ingest_event(self, e: Dict[str, Any], ctx: IngestContext) -> List[Violation]:
        out: List[Violation] = []
        et = (e.get("event_type") or "").lower()

        # Match start & mode capture
        if et in ("server_match_start","map_start"):
            ctx.match_start_time = datetime.fromisoformat(str(e.get("event_time"))) if e.get("event_time") else ctx.now
            msg = (e.get("message") or "").lower()
            if "warfare" in msg:
                ctx.mode = "warfare"

        # Mode must be warfare
        if et in ("map_layer_set","map_start") and self.cfg.require_mode_warfare:
            msg = (e.get("message") or "").lower()
            if "warfare" not in msg:
                out.append(Violation(
                    code="MODE",
                    human="Game mode is not WARFARE (DMT requires Warfare / mid-only).",
                    evidence=e.get("message","")
                ))

        # Early combat before 1:15:00 — any kill before that mark
        if et in ("kill","teamkill"):
            if ctx.match_start_time:
                try:
                    event_ts = datetime.fromisoformat(str(e.get("event_time")))
                except Exception:
                    event_ts = ctx.now
                delta = event_ts - ctx.match_start_time
                if delta.total_seconds() < self._parse_hhmmss(self.cfg.precombat_hold_time):
                    out.append(Violation(
                        code="EARLY_COMBAT",
                        human="Kill before 1:15:00 window.",
                        evidence=f\"{e.get('player_name')} -> {e.get('player2_name')} with {e.get('weapon')}\",
                        severity="major"
                    ))

        # Fourth point fully captured (illegal)
        if et == "sector_captured" and self.cfg.fourth_cap_illegal:
            msg = (e.get("message") or "").lower()
            if " 4th " in msg or "fourth" in msg or "sector 4" in msg:
                out.append(Violation(
                    code="FOURTH_FULL_CAP",
                    human="Team fully captured the 4th point (illegal in DMT).",
                    evidence=msg,
                    severity="major"
                ))

        # Technician role/class usage (banned)
        if et in ("player_role_change","player_connected"):
            role = (e.get("player_role") or "").lower()
            if any(r.lower() in role for r in self.cfg.technician_role_names):
                out.append(Violation(
                    code="TECHNICIAN",
                    human="Technician class/abilities are banned.",
                    evidence=f\"{e.get('player_name')} -> {e.get('player_role')}\",
                    severity="major"
                ))

        # Tank type limits — best-effort heavy suspicion via weapon names
        if et in ("kill","vehicle_kill"):
            wpn = (e.get("weapon") or "").lower()
            if any(sig.lower() in wpn for sig in self.cfg.suspect_heavy_weapons):
                out.append(Violation(
                    code="HEAVY_TANK_SUSPECTED",
                    human="Weapon signature suggests heavy tank use (DMT allows Medium & Recon only).",
                    evidence=f\"weapon={e.get('weapon')}\",
                    severity="minor"
                ))

        # Commander abilities — limit & whitelist
        if et == "commander_ability":
            msg = (e.get("message") or "").lower()
            ability = msg.split()[0] if msg else "unknown"
            cnt = 1 + ctx.commander_ability_uses.get(ability, 0)
            ctx.commander_ability_uses[ability] = cnt

            if ability not in self.cfg.allowed_commander_abilities:
                out.append(Violation(
                    code="CMD_ABILITY_FORBIDDEN",
                    human=f"Commander ability not permitted by DMT: {ability}",
                    evidence=msg,
                    severity="major"
                ))
            if ability == "precision_strike" and cnt > self.cfg.commander_max_pstrikes:
                out.append(Violation(
                    code="CMD_PSTRIKE_LIMIT",
                    human=f"Precision strike used {cnt} times (limit {self.cfg.commander_max_pstrikes}).",
                    evidence=msg,
                    severity="major"
                ))
            if ability == "precision_strike" and "hq" in msg:
                out.append(Violation(
                    code="CMD_PSTRIKE_HQ",
                    human="Precision strike on enemy HQ is forbidden.",
                    evidence=msg,
                    severity="major"
                ))

        return out
