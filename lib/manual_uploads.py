from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any

from lib.logs import LogLine
from lib.scores import MatchGroup
from lib.rcon.models import EventTypes


def infer_format(filename: str, content_type: str | None = None) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix == ".json":
        return "json"
    if suffix == ".csv":
        return "csv"
    if suffix == ".txt":
        return "text"
    if content_type == "application/json":
        return "json"
    if content_type == "text/csv":
        return "csv"
    return "text"


def parse_uploaded_logs(file_format: str, raw_text: str) -> list[LogLine]:
    if file_format == "json":
        return _parse_json_logs(raw_text)
    if file_format == "csv":
        return _parse_csv_logs(raw_text)
    return []


def summarize_matches(logs: list[LogLine]) -> list[dict[str, Any]]:
    if not logs:
        return []

    match_logs = _split_logs_into_matches(logs)
    matches: list[dict[str, Any]] = []
    for logs_for_match in match_logs:
        match = MatchGroup.from_logs(list(logs_for_match)).matches[0]
        matches.append({
            "map_name": match.map,
            "start_time": logs_for_match[0].event_time if logs_for_match else None,
            "end_time": logs_for_match[-1].event_time if logs_for_match else None,
            "duration_seconds": int(match.duration.total_seconds()),
            "allied_score": match.team1_score,
            "axis_score": match.team2_score,
            "player_count": len([player for player in match.players if player.player_id]),
        })
    return matches


def _parse_json_logs(raw_text: str) -> list[LogLine]:
    payload = json.loads(raw_text)
    logs_payload = payload.get("logs", payload)
    if not isinstance(logs_payload, list):
        raise ValueError("JSON upload must contain a top-level 'logs' array or be an array of logs.")

    logs = [LogLine(**item) for item in logs_payload]
    return sorted(logs, key=lambda log: log.event_time)


def _parse_csv_logs(raw_text: str) -> list[LogLine]:
    reader = csv.DictReader(io.StringIO(raw_text))
    logs: list[LogLine] = []
    for row in reader:
        normalized = {
            key: value
            for key, value in row.items()
            if value not in ("", None)
        }
        logs.append(LogLine(**normalized))
    return sorted(logs, key=lambda log: log.event_time)


def _split_logs_into_matches(logs: list[LogLine]) -> list[list[LogLine]]:
    matches: list[list[LogLine]] = []
    current: list[LogLine] = []

    for log in logs:
        if log.event_type == EventTypes.server_match_start.name and current:
            matches.append(current)
            current = []
        current.append(log)

    if current:
        matches.append(current)

    return matches
