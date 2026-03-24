#!/usr/bin/env python3
import argparse
import json
import mimetypes
import os
import re
import secrets
import unicodedata
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse


HOME = Path.home()
DEFAULT_GROUP_DIR = HOME / ".openclaw" / "data" / "movies" / "group-film-night"
GROUP_DIR = Path(os.environ.get("GROUP_FILM_NIGHT_DIR", str(DEFAULT_GROUP_DIR))).expanduser()
STATE_PATH = Path(
    os.environ.get(
        "GROUP_FILM_NIGHT_STATE_PATH",
        str(GROUP_DIR / "group-state.json"),
    )
).expanduser()
DASHBOARD_PATH = Path(
    os.environ.get(
        "GROUP_FILM_NIGHT_DASHBOARD_PATH",
        str(GROUP_DIR / "dashboard" / "index.html"),
    )
).expanduser()
PERSONAL_DIR = Path(
    os.environ.get(
        "GROUP_FILM_NIGHT_PERSONAL_DIR",
        str(HOME / ".openclaw" / "data" / "movies" / "movie-chooser"),
    )
).expanduser()
STREAMING_STATE_PATH = Path(
    os.environ.get(
        "GROUP_FILM_NIGHT_STREAMING_STATE_PATH",
        str(PERSONAL_DIR / "streaming-state.json"),
    )
).expanduser()
METADATA_CACHE_PATH = Path(
    os.environ.get(
        "GROUP_FILM_NIGHT_METADATA_CACHE_PATH",
        str(PERSONAL_DIR / "metadata-cache.json"),
    )
).expanduser()
RT_CACHE_PATH = Path(
    os.environ.get(
        "GROUP_FILM_NIGHT_RT_CACHE_PATH",
        str(PERSONAL_DIR / "rt-cache.json"),
    )
).expanduser()
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = int(os.environ.get("PORT", "8048"))
MAX_HISTORY = 12
VALID_REACTIONS = {"neutral", "like", "dislike", "seen"}
SNACK_CATALOG = [
    {"id": "nachos", "label": "Nachos"},
    {"id": "chips", "label": "Chips"},
    {"id": "schokolade", "label": "Schokolade"},
    {"id": "eis", "label": "Eis"},
    {"id": "bier", "label": "Bier"},
    {"id": "sonstiges", "label": "Sonstiges"},
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_session_date(value: str) -> Optional[str]:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError("Bitte ein gueltiges Datum fuer den Filmabend angeben.") from exc


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_value.strip().lower()
    lowered = re.sub(r"[^a-z0-9]+", "-", lowered)
    return lowered.strip("-") or "item"


def compact_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_value.strip().lower()
    lowered = re.sub(r"\[[^\]]+\]", "", lowered)
    lowered = re.sub(r"\(\d{4}\)", "", lowered)
    lowered = lowered.replace("’", "").replace("'", "")
    lowered = re.sub(r"[^a-z0-9]+", "", lowered)
    return lowered or "item"


def service_label(service: str) -> str:
    labels = {
        "prime_video": "Prime Video",
        "disney_plus": "Disney+",
        "manual": "Manuell",
        "unknown": "Unbekannt",
    }
    return labels.get(service, service.replace("_", " ").title())


def type_label(value: str) -> str:
    labels = {
        "movie": "Film",
        "show": "Serie",
        "unknown": "Unklar",
    }
    return labels.get(value or "unknown", "Unklar")


def normalize_title_for_display(title: str) -> str:
    cleaned = (title or "").strip()
    cleaned = re.sub(r"\[[^\]]+\]", "", cleaned)
    cleaned = re.sub(r"\(\d{4}\)", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" -:")


def search_title(title: str) -> str:
    cleaned = normalize_title_for_display(title)
    cleaned = re.sub(r"\b(?:dt\.?/ov|ov|original version)\b", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" -:")


def normalize_type(value: str) -> str:
    lowered = (value or "").strip().lower()
    if lowered in {"movie", "film"}:
        return "movie"
    if lowered in {"show", "series", "serie", "tv"}:
        return "show"
    return "unknown"


def infer_type_from_title(title: str) -> str:
    value = (title or "").lower()
    if re.search(r"\b(staffel|season|episode|folge|series|serie)\b", value):
        return "show"
    return "unknown"


def parse_percent(value: Any) -> Optional[int]:
    if value is None:
        return None
    digits = re.sub(r"[^0-9]", "", str(value))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return json.loads(json.dumps(default))
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=True), encoding="utf-8")


def load_metadata_entries() -> Dict[str, Any]:
    return load_json(METADATA_CACHE_PATH, {"entries": {}}).get("entries", {})


def load_rt_entries() -> Dict[str, Any]:
    return load_json(RT_CACHE_PATH, {"entries": {}}).get("entries", {})


def rt_payload_for_title(title: str, content_type: str, rt_entries: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title_key = compact_key(search_title(title))
    candidates = [
        f"{normalize_type(content_type)}::{title_key}",
        f"movie::{title_key}",
        f"show::{title_key}",
        f"unknown::{title_key}",
    ]
    for key in candidates:
        entry = rt_entries.get(key)
        if not entry:
            continue
        critics = entry.get("critics") or {}
        percent = parse_percent(critics.get("percent") or critics.get("score"))
        if percent is None:
            continue
        return {
            "percent": percent,
            "label": critics.get("label") or "Tomatometer",
            "url": entry.get("url") or "",
        }
    return None


def resolved_type_from_rt(rt_payload: Optional[Dict[str, Any]]) -> Optional[str]:
    if not rt_payload:
        return None
    url = (rt_payload.get("url") or "").lower()
    if "/m/" in url:
        return "movie"
    if "/tv/" in url:
        return "show"
    return None


def build_film_id(service: str, title: str) -> str:
    return f"{service}::{compact_key(search_title(title))}"


def merge_watchlist_item(existing: Optional[Dict[str, Any]], incoming: Dict[str, Any]) -> Dict[str, Any]:
    if not existing:
        return incoming
    merged = dict(existing)
    for key in ["title", "searchTitle", "service", "serviceLabel", "url", "image", "source"]:
        if incoming.get(key):
            merged[key] = incoming[key]
    existing_type = normalize_type(merged.get("type"))
    incoming_type = normalize_type(incoming.get("type"))
    type_source = merged.get("typeSource") or "imported"
    incoming_type_source = incoming.get("typeSource") or "imported"
    if type_source != "manual":
        if incoming_type == "movie":
            merged["type"] = "movie"
        elif incoming_type == "show" and existing_type != "movie":
            merged["type"] = "show"
        elif incoming_type == "unknown" and existing_type == "show" and not existing.get("manual"):
            merged["type"] = "unknown"
        elif existing_type == "unknown" and incoming_type != "unknown":
            merged["type"] = incoming_type
        merged["typeSource"] = incoming_type_source
    if not merged.get("rt") and incoming.get("rt"):
        merged["rt"] = incoming["rt"]
    merged["manual"] = bool(existing.get("manual") or incoming.get("manual"))
    merged["typeLabel"] = type_label(merged.get("type"))
    merged["updatedAt"] = utc_now()
    merged["importedAt"] = incoming.get("importedAt") or existing.get("importedAt") or utc_now()
    if existing.get("addedAt"):
        merged["addedAt"] = existing["addedAt"]
    else:
        merged["addedAt"] = incoming.get("addedAt") or utc_now()
    return merged


def import_watchlist(existing_watchlist: Optional[List[Dict[str, Any]]] = None) -> Tuple[List[Dict[str, Any]], int]:
    if not STREAMING_STATE_PATH.exists():
        ordered = sorted(
            [dict(item) for item in (existing_watchlist or [])],
            key=lambda item: (
                service_label(item.get("service", "")).lower(),
                item.get("title", "").lower(),
            ),
        )
        for item in ordered:
            item["type"] = normalize_type(item.get("type"))
            item["typeLabel"] = type_label(item.get("type"))
        return ordered, 0
    streaming_state = load_json(STREAMING_STATE_PATH, {"services": {}})
    metadata_entries = load_metadata_entries()
    rt_entries = load_rt_entries()
    items_by_id: Dict[str, Dict[str, Any]] = {}
    imported_count = 0

    for item in existing_watchlist or []:
        items_by_id[item["id"]] = dict(item)

    for service_name, service_data in (streaming_state.get("services") or {}).items():
        for entry in service_data.get("watchlist") or []:
            raw_title = entry.get("title") or ""
            display_title = normalize_title_for_display(raw_title)
            if not display_title:
                continue
            film_id = build_film_id(service_name, display_title)
            metadata = metadata_entries.get(f"{service_name}::{entry.get('url', '')}", {})
            inferred_type = normalize_type(metadata.get("type") or entry.get("type") or infer_type_from_title(display_title))
            rt_payload = rt_payload_for_title(display_title, inferred_type, rt_entries)
            rt_resolved_type = resolved_type_from_rt(rt_payload)
            title_hint_type = infer_type_from_title(display_title)
            if rt_resolved_type:
                content_type = rt_resolved_type
            elif inferred_type == "show" and title_hint_type != "show":
                content_type = "unknown"
            else:
                content_type = inferred_type
            imported = {
                "id": film_id,
                "title": display_title,
                "searchTitle": search_title(display_title),
                "service": service_name,
                "serviceLabel": service_label(service_name),
                "type": content_type,
                "typeLabel": type_label(content_type),
                "url": entry.get("url") or "",
                "image": metadata.get("image") or "",
                "rt": rt_payload,
                "source": "personal_watchlist",
                "manual": False,
                "typeSource": "imported",
                "addedAt": utc_now(),
                "importedAt": utc_now(),
                "updatedAt": utc_now(),
            }
            items_by_id[film_id] = merge_watchlist_item(items_by_id.get(film_id), imported)
            imported_count += 1

    ordered = sorted(
        items_by_id.values(),
        key=lambda item: (
            service_label(item.get("service", "")).lower(),
            item.get("title", "").lower(),
        ),
    )
    for item in ordered:
        item["type"] = normalize_type(item.get("type"))
        item["typeLabel"] = type_label(item.get("type"))
    return ordered, imported_count


def default_state() -> Dict[str, Any]:
    watchlist, imported_count = import_watchlist()
    now = utc_now()
    return {
        "version": 1,
        "updatedAt": now,
        "lastImportAt": now if imported_count else None,
        "source": {
            "streamingStatePath": str(STREAMING_STATE_PATH),
            "metadataCachePath": str(METADATA_CACHE_PATH),
            "rtCachePath": str(RT_CACHE_PATH),
        },
        "participants": [],
        "watchlist": watchlist,
        "preferences": {},
        "currentSession": None,
        "sessionHistory": [],
        "comments": [],
    }


def ensure_state() -> Dict[str, Any]:
    state = load_json(STATE_PATH, None)
    if state is None:
        state = default_state()
        write_json(STATE_PATH, state)
        return state
    state.setdefault("version", 1)
    state.setdefault("updatedAt", utc_now())
    state.setdefault("lastImportAt", None)
    state.setdefault("participants", [])
    state.setdefault("watchlist", [])
    state.setdefault("preferences", {})
    state.setdefault("currentSession", None)
    state.setdefault("sessionHistory", [])
    state.setdefault("comments", [])
    for participant in state.get("participants", []):
        ensure_participant_token(participant)
    for item in state.get("watchlist", []):
        if normalize_type(item.get("type")) == "unknown":
            item["type"] = "movie"
            item["typeLabel"] = type_label("movie")
            item["typeSource"] = item.get("typeSource") or "manual"
    if not state["watchlist"]:
        state["watchlist"], _ = import_watchlist(state["watchlist"])
    write_json(STATE_PATH, state)
    return state


def save_state(state: Dict[str, Any]) -> None:
    state["updatedAt"] = utc_now()
    write_json(STATE_PATH, state)


def participant_name_map(state: Dict[str, Any]) -> Dict[str, str]:
    return {participant["id"]: participant["name"] for participant in state.get("participants", [])}


def participant_by_id(state: Dict[str, Any], participant_id: str) -> Optional[Dict[str, Any]]:
    return next((item for item in state.get("participants", []) if item["id"] == participant_id), None)


def ensure_participant_token(participant: Dict[str, Any]) -> str:
    token = (participant.get("joinToken") or "").strip()
    if token:
        return token
    token = secrets.token_urlsafe(18)
    participant["joinToken"] = token
    return token


def participant_by_token(state: Dict[str, Any], token: str) -> Optional[Dict[str, Any]]:
    needle = (token or "").strip()
    if not needle:
        return None
    for participant in state.get("participants", []):
        if (participant.get("joinToken") or "").strip() == needle:
            return participant
    return None


def viewer_context(state: Dict[str, Any], token: str) -> Optional[Dict[str, Any]]:
    participant = participant_by_token(state, token)
    if not participant:
        return None
    return {
        "participant": participant,
        "is_host": slugify(participant.get("name") or "") in {"marius", "kathi"},
    }


def require_host_viewer(state: Dict[str, Any], token: str) -> Dict[str, Any]:
    viewer = viewer_context(state, token)
    if not viewer:
        raise ValueError("Bitte zuerst den eigenen Namen auswaehlen oder einen Host-Link oeffnen.")
    if not viewer.get("is_host"):
        raise ValueError("Nur Hosts duerfen das machen.")
    return viewer


def require_viewer_identity(state: Dict[str, Any], token: str, participant_id: str) -> Optional[Dict[str, Any]]:
    viewer = viewer_context(state, token)
    if not viewer:
        raise ValueError("Bitte zuerst den eigenen Namen auswaehlen.")
    if viewer["participant"]["id"] != participant_id:
        raise ValueError("Dieser persoenliche Link darf nur fuer die eigene Auswahl verwendet werden.")
    return viewer


def watchlist_by_id(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {item["id"]: item for item in state.get("watchlist", [])}


def reaction_for(state: Dict[str, Any], participant_id: str, film_id: str) -> str:
    prefs = state.get("preferences", {}).get(participant_id, {})
    reaction = (prefs.get(film_id) or {}).get("reaction")
    return reaction if reaction in VALID_REACTIONS else "neutral"


def create_participant(state: Dict[str, Any], name: str, membership: str = "member") -> Dict[str, Any]:
    trimmed = (name or "").strip()
    if not trimmed:
        raise ValueError("Bitte einen Namen eingeben.")
    base = slugify(trimmed)
    existing_ids = {participant["id"] for participant in state.get("participants", [])}
    participant_id = base
    suffix = 2
    while participant_id in existing_ids:
        participant_id = f"{base}-{suffix}"
        suffix += 1
    participant = {
        "id": participant_id,
        "name": trimmed,
        "membership": "guest" if membership == "guest" else "member",
        "createdAt": utc_now(),
    }
    ensure_participant_token(participant)
    state.setdefault("participants", []).append(participant)
    state["participants"] = sorted(state["participants"], key=lambda item: item["name"].lower())
    save_state(state)
    return participant


def set_preference(state: Dict[str, Any], participant_id: str, film_id: str, reaction: str) -> None:
    if not participant_by_id(state, participant_id):
        raise ValueError("Teilnehmer nicht gefunden.")
    if film_id not in watchlist_by_id(state):
        raise ValueError("Film nicht gefunden.")
    normalized = reaction if reaction in VALID_REACTIONS else "neutral"
    state.setdefault("preferences", {}).setdefault(participant_id, {})
    if normalized == "neutral":
        state["preferences"][participant_id].pop(film_id, None)
        if not state["preferences"][participant_id]:
            state["preferences"].pop(participant_id, None)
    else:
        state["preferences"][participant_id][film_id] = {
            "reaction": normalized,
            "updatedAt": utc_now(),
        }
    save_state(state)


def add_watchlist_item(state: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    title = normalize_title_for_display(payload.get("title") or "")
    if not title:
        raise ValueError("Bitte mindestens einen Filmtitel angeben.")
    service = (payload.get("service") or "manual").strip() or "manual"
    content_type = normalize_type(payload.get("type") or "movie")
    film_id = build_film_id(service, title)
    metadata = {
        "id": film_id,
        "title": title,
        "searchTitle": search_title(title),
        "service": service,
        "serviceLabel": service_label(service),
        "type": content_type,
        "typeLabel": type_label(content_type),
        "url": (payload.get("url") or "").strip(),
        "image": (payload.get("image") or "").strip(),
        "rt": None,
        "source": "manual",
        "manual": True,
        "typeSource": "manual",
        "addedAt": utc_now(),
        "updatedAt": utc_now(),
    }
    watchlist = watchlist_by_id(state)
    watchlist[film_id] = merge_watchlist_item(watchlist.get(film_id), metadata)
    state["watchlist"] = sorted(
        watchlist.values(),
        key=lambda item: (service_label(item.get("service", "")).lower(), item.get("title", "").lower()),
    )
    save_state(state)
    return watchlist[film_id]


def set_watchlist_type(state: Dict[str, Any], film_id: str, content_type: str) -> Dict[str, Any]:
    normalized = normalize_type(content_type)
    films = watchlist_by_id(state)
    if film_id not in films:
        raise ValueError("Film nicht gefunden.")
    film = dict(films[film_id])
    film["type"] = normalized
    film["typeLabel"] = type_label(normalized)
    film["typeSource"] = "manual"
    film["updatedAt"] = utc_now()
    films[film_id] = film
    state["watchlist"] = sorted(
        films.values(),
        key=lambda item: (service_label(item.get("service", "")).lower(), item.get("title", "").lower()),
    )
    save_state(state)
    return film


def remove_watchlist_item(state: Dict[str, Any], film_id: str) -> Dict[str, Any]:
    films = watchlist_by_id(state)
    if film_id not in films:
        raise ValueError("Film nicht gefunden.")
    removed = films.pop(film_id)

    for participant_id, film_preferences in list((state.get("preferences") or {}).items()):
        if film_id in film_preferences:
            film_preferences.pop(film_id, None)
            if not film_preferences:
                state["preferences"].pop(participant_id, None)

    session = state.get("currentSession")
    if session:
        session["candidates"] = [film for film in session.get("candidates", []) if film.get("id") != film_id]
        session["revealed"] = [film for film in session.get("revealed", []) if film.get("id") != film_id]
        if session.get("ballots"):
            for ballot in session["ballots"].values():
                ballot["ranking"] = [entry for entry in ballot.get("ranking", []) if entry.get("filmId") != film_id]

    state["watchlist"] = sorted(
        films.values(),
        key=lambda item: (service_label(item.get("service", "")).lower(), item.get("title", "").lower()),
    )
    save_state(state)
    return removed


def refresh_watchlist_from_personal(state: Dict[str, Any]) -> Dict[str, Any]:
    merged_watchlist, imported_count = import_watchlist(state.get("watchlist") or [])
    state["watchlist"] = merged_watchlist
    state["lastImportAt"] = utc_now()
    save_state(state)
    return {
        "watchlistCount": len(state["watchlist"]),
        "importedRows": imported_count,
        "lastImportAt": state["lastImportAt"],
    }


def candidate_reason(candidate: Dict[str, Any]) -> str:
    parts: List[str] = []
    if candidate.get("consensusPick"):
        parts.append("Konsens-Pick")
    likes = candidate.get("likes") or 0
    seen = candidate.get("seen") or 0
    rt = candidate.get("rtPercent")
    if likes:
        parts.append(f"{likes} Likes")
    if seen:
        parts.append(f"{seen} schon gesehen")
    if rt is not None:
        parts.append(f"{rt}% RT")
    parts.append(candidate.get("serviceLabel") or service_label(candidate.get("service", "")))
    return " · ".join(parts)


def shortlist_candidates(state: Dict[str, Any], attendee_ids: List[str], limit: int = 5) -> List[Dict[str, Any]]:
    watchlist = state.get("watchlist") or []
    if not watchlist:
        return []

    scored: List[Dict[str, Any]] = []
    for film in watchlist:
        if normalize_type(film.get("type")) != "movie":
            continue
        reactions = [reaction_for(state, participant_id, film["id"]) for participant_id in attendee_ids]
        dislikes = reactions.count("dislike")
        seen = reactions.count("seen")
        likes = reactions.count("like")
        if dislikes:
            continue
        base_score = 52
        base_score += 20
        rt_percent = parse_percent((film.get("rt") or {}).get("percent"))
        if rt_percent is not None:
            base_score += int(rt_percent / 3)
        if film.get("manual"):
            base_score += 4
        base_score += likes * 26
        base_score -= seen * 15
        entry = {
            "filmId": film["id"],
            "title": film["title"],
            "service": film["service"],
            "serviceLabel": film.get("serviceLabel") or service_label(film["service"]),
            "url": film.get("url") or "",
            "image": film.get("image") or "",
            "type": film.get("type") or "unknown",
            "typeLabel": film.get("typeLabel") or type_label(film.get("type")),
            "rt": film.get("rt"),
            "rtPercent": rt_percent,
            "likes": likes,
            "seen": seen,
            "consensusPick": likes == len(attendee_ids) and len(attendee_ids) > 0,
            "shortlistScore": base_score,
        }
        entry["reason"] = candidate_reason(entry)
        scored.append(entry)

    explicit_movies = list(scored)
    explicit_movies.sort(
        key=lambda item: (
            -int(bool(item.get("consensusPick"))),
            -item["shortlistScore"],
            -(item.get("rtPercent") or 0),
            item["title"].lower(),
        )
    )

    selected: List[Dict[str, Any]] = []
    service_counts: Dict[str, int] = {}

    def try_take(items: List[Dict[str, Any]]) -> None:
        for item in items:
            if len(selected) >= limit:
                return
            if item["filmId"] in {chosen["filmId"] for chosen in selected}:
                continue
            service_total = service_counts.get(item["service"], 0)
            if service_total >= 3:
                continue
            selected.append(item)
            service_counts[item["service"]] = service_total + 1

    consensus_candidates = [item for item in explicit_movies if item.get("consensusPick")]
    if consensus_candidates:
        try_take(consensus_candidates[:1])
    try_take(explicit_movies)
    if len(selected) < limit:
        remaining = scored
        for item in remaining:
            if len(selected) >= limit:
                break
            if item["filmId"] not in {chosen["filmId"] for chosen in selected}:
                selected.append(item)

    return selected[:limit]


def build_auto_ranking(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranking: List[Dict[str, Any]] = []
    for position, candidate in enumerate(candidates, start=1):
        ranking.append(
            {
                "position": position,
                "filmId": candidate["filmId"],
                "points": candidate.get("shortlistScore", 0),
                "firstPlaceVotes": candidate.get("likes", 0),
                "voteCount": 0,
            }
        )
    return ranking


def archive_current_session(state: Dict[str, Any]) -> None:
    session = state.get("currentSession")
    if not session:
        return
    history_entry = {
        "id": session.get("id"),
        "createdAt": session.get("createdAt"),
        "attendeeIds": session.get("attendeeIds", []),
        "candidateIds": [candidate["filmId"] for candidate in session.get("candidates", [])],
        "revealedCount": session.get("revealedCount", 0),
        "ranking": session.get("ranking", []),
        "scheduledFor": session.get("scheduledFor"),
    }
    state.setdefault("sessionHistory", []).insert(0, history_entry)
    state["sessionHistory"] = state["sessionHistory"][:MAX_HISTORY]


def create_session(state: Dict[str, Any], attendee_ids: List[str], scheduled_for: Optional[str] = None) -> Dict[str, Any]:
    unique_ids = []
    seen_ids = set()
    for attendee_id in attendee_ids:
        if attendee_id not in seen_ids:
            unique_ids.append(attendee_id)
            seen_ids.add(attendee_id)
    if not unique_ids:
        raise ValueError("Bitte mindestens einen Teilnehmer auswählen.")
    for attendee_id in unique_ids:
        if not participant_by_id(state, attendee_id):
            raise ValueError(f"Unbekannter Teilnehmer: {attendee_id}")
    candidates = shortlist_candidates(state, unique_ids, limit=5)
    if not candidates:
        raise ValueError("Aktuell gibt es fuer diese Runde keinen passenden Film. Fuegt mehr Titel hinzu oder setzt mehr Likes/Neutrals fuer die heutigen Teilnehmer.")
    archive_current_session(state)
    session = {
        "id": f"session-{uuid.uuid4().hex[:10]}",
        "createdAt": utc_now(),
        "scheduledFor": normalize_session_date(scheduled_for or ""),
        "attendeeIds": unique_ids,
        "candidates": candidates,
        "snackAssignments": {},
        "votes": {},
        "ranking": build_auto_ranking(candidates),
        "revealedCount": 0,
    }
    state["currentSession"] = session
    save_state(state)
    return session


def validate_ranking(session: Dict[str, Any], ranking: List[str]) -> List[str]:
    expected_ids = [candidate["filmId"] for candidate in session.get("candidates", [])]
    if len(ranking) != len(expected_ids):
        raise ValueError("Bitte alle Kandidaten genau einmal einsortieren.")
    if set(ranking) != set(expected_ids):
        raise ValueError("Die Abstimmung muss genau die aktuellen Kandidaten enthalten.")
    if len(ranking) != len(set(ranking)):
        raise ValueError("Jeder Kandidat darf nur einmal vorkommen.")
    return ranking


def compute_session_ranking(session: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = {candidate["filmId"]: candidate for candidate in session.get("candidates", [])}
    points = {film_id: 0 for film_id in candidates}
    first_places = {film_id: 0 for film_id in candidates}
    vote_count = len(session.get("votes", {}))
    candidate_count = len(candidates)
    for vote in session.get("votes", {}).values():
        ranking = vote.get("ranking") or []
        for index, film_id in enumerate(ranking):
            points[film_id] += candidate_count - index
            if index == 0:
                first_places[film_id] += 1
    ordered = sorted(
        candidates.values(),
        key=lambda candidate: (
            -points[candidate["filmId"]],
            -first_places[candidate["filmId"]],
            -candidate.get("shortlistScore", 0),
            candidate["title"].lower(),
        ),
    )
    ranking = []
    for position, candidate in enumerate(ordered, start=1):
        ranking.append(
            {
                "position": position,
                "filmId": candidate["filmId"],
                "points": points[candidate["filmId"]],
                "firstPlaceVotes": first_places[candidate["filmId"]],
                "voteCount": vote_count,
            }
        )
    session["ranking"] = ranking
    return ranking


def submit_vote(state: Dict[str, Any], participant_id: str, ranking: List[str]) -> Dict[str, Any]:
    session = state.get("currentSession")
    if not session:
        raise ValueError("Es gibt aktuell keine aktive Session.")
    if participant_id not in session.get("attendeeIds", []):
        raise ValueError("Dieser Teilnehmer ist heute nicht Teil der Session.")
    validated = validate_ranking(session, ranking)
    session.setdefault("votes", {})[participant_id] = {
        "ranking": validated,
        "submittedAt": utc_now(),
    }
    if len(session["votes"]) == len(session.get("attendeeIds", [])):
        compute_session_ranking(session)
    save_state(state)
    return session


def reveal_next(state: Dict[str, Any]) -> Dict[str, Any]:
    session = state.get("currentSession")
    if not session:
        raise ValueError("Es gibt aktuell keine aktive Session.")
    ranking = session.get("ranking") or build_auto_ranking(session.get("candidates", []))
    if session.get("revealedCount", 0) >= len(ranking):
        raise ValueError("Alle Plätze wurden bereits revealed.")
    session["revealedCount"] = session.get("revealedCount", 0) + 1
    save_state(state)
    return session


def reset_reveal(state: Dict[str, Any]) -> Dict[str, Any]:
    session = state.get("currentSession")
    if not session:
        raise ValueError("Es gibt aktuell keine aktive Session.")
    session["revealedCount"] = 0
    save_state(state)
    return session


def reset_session(state: Dict[str, Any]) -> None:
    archive_current_session(state)
    state["currentSession"] = None
    save_state(state)


def set_snack_assignment(state: Dict[str, Any], participant_id: str, snack_id: str) -> Dict[str, Any]:
    session = state.get("currentSession")
    if not session:
        raise ValueError("Es gibt aktuell keine aktive Session.")
    if participant_id not in session.get("attendeeIds", []):
        raise ValueError("Dieser Teilnehmer ist heute nicht Teil der Session.")
    snack_ids = {item["id"] for item in SNACK_CATALOG}
    if snack_id not in snack_ids:
        raise ValueError("Unbekannter Snack.")
    assignments = session.setdefault("snackAssignments", {})
    current_owners = list(assignments.get(snack_id) or [])
    if participant_id in current_owners:
        current_owners = [owner_id for owner_id in current_owners if owner_id != participant_id]
    else:
        current_owners.append(participant_id)
    if current_owners:
        assignments[snack_id] = current_owners
    else:
        assignments.pop(snack_id, None)
    save_state(state)
    return session


def add_comment(state: Dict[str, Any], participant_id: str, text: str) -> Dict[str, Any]:
    participant = participant_by_id(state, participant_id)
    if not participant:
        raise ValueError("Teilnehmer nicht gefunden.")
    body = (text or "").strip()
    if not body:
        raise ValueError("Bitte einen Kommentar eingeben.")
    if len(body) > 280:
        raise ValueError("Kommentar bitte auf maximal 280 Zeichen begrenzen.")
    comment = {
        "id": f"comment-{uuid.uuid4().hex[:10]}",
        "participantId": participant_id,
        "text": body,
        "createdAt": utc_now(),
        "sessionId": (state.get("currentSession") or {}).get("id"),
    }
    state.setdefault("comments", []).insert(0, comment)
    state["comments"] = state["comments"][:40]
    save_state(state)
    return comment


def session_payload(state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    session = state.get("currentSession")
    if not session:
        return None
    participants = participant_name_map(state)
    candidates_by_id = {candidate["filmId"]: candidate for candidate in session.get("candidates", [])}
    revealed_entries = []
    ranking = session.get("ranking") or []
    for entry in ranking[: session.get("revealedCount", 0)]:
        candidate = candidates_by_id.get(entry["filmId"])
        if not candidate:
            continue
        revealed_entries.append(
            {
                "position": entry["position"],
                "points": entry["points"],
                "firstPlaceVotes": entry["firstPlaceVotes"],
                "film": candidate,
            }
        )
    snack_names = {item["id"]: item["label"] for item in SNACK_CATALOG}
    snack_assignments = []
    for snack_id, owner_ids in (session.get("snackAssignments") or {}).items():
        owner_list = owner_ids if isinstance(owner_ids, list) else [owner_ids]
        snack_assignments.append(
            {
                "snackId": snack_id,
                "snackLabel": snack_names.get(snack_id, snack_id),
                "participantIds": owner_list,
                "participantNames": [participants.get(owner_id, owner_id) for owner_id in owner_list],
            }
        )
    return {
        "id": session.get("id"),
        "createdAt": session.get("createdAt"),
        "scheduledFor": session.get("scheduledFor"),
        "attendeeIds": session.get("attendeeIds", []),
        "attendees": [
            {"id": attendee_id, "name": participants.get(attendee_id, attendee_id)}
            for attendee_id in session.get("attendeeIds", [])
        ],
        "candidates": session.get("candidates", []),
        "selection": session.get("candidates", [None])[0] if session.get("candidates") else None,
        "votesLocked": len(session.get("votes", {})),
        "votesNeeded": len(session.get("attendeeIds", [])),
        "voteStatus": [
            {
                "participantId": attendee_id,
                "name": participants.get(attendee_id, attendee_id),
                "submitted": attendee_id in session.get("votes", {}),
            }
            for attendee_id in session.get("attendeeIds", [])
        ],
        "allVotesSubmitted": True,
        "revealedCount": session.get("revealedCount", 0),
        "revealed": revealed_entries,
        "hasMoreReveals": session.get("revealedCount", 0) < len(session.get("candidates", [])),
        "snackCatalog": SNACK_CATALOG,
        "snackAssignments": snack_assignments,
    }


def history_payload(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    participants = participant_name_map(state)
    films = watchlist_by_id(state)
    out = []
    for entry in state.get("sessionHistory", [])[:6]:
        winner = None
        ranking = entry.get("ranking") or []
        if ranking:
            winner_film = films.get(ranking[0].get("filmId"))
            if winner_film:
                winner = {
                    "title": winner_film.get("title"),
                    "serviceLabel": winner_film.get("serviceLabel") or service_label(winner_film.get("service", "")),
                }
        out.append(
            {
                "id": entry.get("id"),
                "createdAt": entry.get("createdAt"),
                "scheduledFor": entry.get("scheduledFor"),
                "attendees": [participants.get(attendee_id, attendee_id) for attendee_id in entry.get("attendeeIds", [])],
                "winner": winner,
            }
        )
    return out


def comments_payload(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    participants = participant_name_map(state)
    out = []
    for entry in state.get("comments", [])[:12]:
        out.append(
            {
                "id": entry.get("id"),
                "participantId": entry.get("participantId"),
                "participantName": participants.get(entry.get("participantId"), entry.get("participantId")),
                "text": entry.get("text", ""),
                "createdAt": entry.get("createdAt"),
                "sessionId": entry.get("sessionId"),
            }
        )
    return out


def state_payload(state: Dict[str, Any], viewer_token: str = "") -> Dict[str, Any]:
    viewer = viewer_context(state, viewer_token)
    viewer_participant = viewer["participant"] if viewer else None
    include_join_links = not viewer_participant or bool(viewer and viewer.get("is_host"))
    public_participants = [
        {
            "id": participant.get("id"),
            "name": participant.get("name"),
            "membership": participant.get("membership", "member"),
            "createdAt": participant.get("createdAt"),
        }
        for participant in state.get("participants", [])
    ]
    return {
        "updatedAt": state.get("updatedAt"),
        "lastImportAt": state.get("lastImportAt"),
        "participants": public_participants,
        "watchlist": state.get("watchlist", []),
        "preferences": state.get("preferences", {}),
        "currentSession": session_payload(state),
        "sessionHistory": history_payload(state),
        "comments": comments_payload(state),
        "viewer": {
            "participantId": viewer_participant.get("id") if viewer_participant else None,
            "participantName": viewer_participant.get("name") if viewer_participant else None,
            "isHost": bool(viewer and viewer.get("is_host")),
            "hasLink": bool(viewer_participant),
        },
        "joinLinks": [] if not include_join_links else [
            {
                "participantId": participant.get("id"),
                "participantName": participant.get("name"),
                "path": f"/?viewer={participant.get('joinToken')}",
            }
            for participant in state.get("participants", [])
            if participant.get("joinToken")
        ],
        "stats": {
            "participantCount": len(state.get("participants", [])),
            "watchlistCount": len(state.get("watchlist", [])),
            "movieCount": len([item for item in state.get("watchlist", []) if normalize_type(item.get("type")) == "movie"]),
        },
    }


def read_json_body(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0"))
    if length <= 0:
        return {}
    raw = handler.rfile.read(length).decode("utf-8")
    if not raw.strip():
        return {}
    return json.loads(raw)


class GroupFilmNightHandler(BaseHTTPRequestHandler):
    server_version = "GroupFilmNight/0.1"

    def _send_json(self, payload: Dict[str, Any], status: int = HTTPStatus.OK, include_body: bool = True) -> None:
        data = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if include_body:
            self.wfile.write(data)

    def _send_html(self, html: str, status: int = HTTPStatus.OK, include_body: bool = True) -> None:
        data = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if include_body:
            self.wfile.write(data)

    def _send_file(self, path: Path, status: int = HTTPStatus.OK, include_body: bool = True) -> None:
        data = path.read_bytes()
        content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "public, max-age=3600")
        self.end_headers()
        if include_body:
            self.wfile.write(data)

    def _send_error_json(self, message: str, status: int = HTTPStatus.BAD_REQUEST) -> None:
        self._send_json({"ok": False, "error": message}, status=status)

    def _handle_get_like(self, include_body: bool) -> None:
        parsed = urlparse(self.path)
        viewer_token = (parse_qs(parsed.query).get("viewer") or [""])[0]
        if parsed.path in {"/", "/index.html"}:
            if not DASHBOARD_PATH.exists():
                self._send_error_json("Dashboard-Datei nicht gefunden.", status=HTTPStatus.NOT_FOUND)
                return
            self._send_html(DASHBOARD_PATH.read_text(encoding="utf-8"), include_body=include_body)
            return
        if parsed.path.startswith("/assets/"):
            relative = parsed.path.removeprefix("/assets/").strip("/")
            asset_path = (GROUP_DIR / relative).resolve()
            group_root = GROUP_DIR.resolve()
            if group_root not in asset_path.parents and asset_path != group_root:
                self._send_error_json("Asset nicht gefunden.", status=HTTPStatus.NOT_FOUND)
                return
            if not asset_path.exists() or not asset_path.is_file():
                self._send_error_json("Asset nicht gefunden.", status=HTTPStatus.NOT_FOUND)
                return
            self._send_file(asset_path, include_body=include_body)
            return
        if parsed.path == "/api/state":
            state = ensure_state()
            self._send_json({"ok": True, "state": state_payload(state, viewer_token)}, include_body=include_body)
            return
        if parsed.path == "/health":
            self._send_json({"ok": True, "status": "ok"}, include_body=include_body)
            return
        self._send_error_json("Nicht gefunden.", status=HTTPStatus.NOT_FOUND)

    def do_GET(self) -> None:
        self._handle_get_like(include_body=True)

    def do_HEAD(self) -> None:
        self._handle_get_like(include_body=False)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        api_path = parsed.path
        viewer_token = (parse_qs(parsed.query).get("viewer") or [""])[0]
        try:
            payload = read_json_body(self)
        except json.JSONDecodeError:
            self._send_error_json("Ungueltiges JSON.")
            return

        state = ensure_state()
        try:
            if api_path == "/api/participants":
                viewer = viewer_context(state, viewer_token)
                bootstrap = bool(payload.get("bootstrap"))
                self_serve = bool(payload.get("selfServe"))
                if viewer and not viewer.get("is_host"):
                    raise ValueError("Nur Hosts koennen neue Teilnehmer anlegen.")
                if not viewer and state.get("participants") and not (bootstrap or self_serve):
                    raise ValueError("Bitte zuerst den eigenen Namen auswaehlen oder einen Host-Link oeffnen.")
                participant = create_participant(
                    state,
                    payload.get("name") or "",
                    membership=(payload.get("membership") or "member"),
                )
                self._send_json(
                    {
                        "ok": True,
                        "participant": participant,
                        "joinPath": f"/?viewer={participant.get('joinToken')}",
                        "state": state_payload(state, viewer_token),
                    }
                )
                return

            if api_path == "/api/preferences":
                require_viewer_identity(
                    state,
                    viewer_token,
                    (payload.get("participantId") or "").strip(),
                )
                set_preference(
                    state,
                    participant_id=(payload.get("participantId") or "").strip(),
                    film_id=(payload.get("filmId") or "").strip(),
                    reaction=(payload.get("reaction") or "neutral").strip(),
                )
                self._send_json({"ok": True, "state": state_payload(state, viewer_token)})
                return

            if api_path == "/api/watchlist/add":
                if state.get("participants"):
                    viewer_context_required = viewer_context(state, viewer_token)
                    if not viewer_context_required:
                        raise ValueError("Bitte zuerst den eigenen Namen auswaehlen.")
                item = add_watchlist_item(state, payload)
                self._send_json({"ok": True, "item": item, "state": state_payload(state, viewer_token)})
                return

            if api_path == "/api/watchlist/set-type":
                item = set_watchlist_type(
                    state,
                    film_id=(payload.get("filmId") or "").strip(),
                    content_type=(payload.get("type") or "unknown").strip(),
                )
                self._send_json({"ok": True, "item": item, "state": state_payload(state, viewer_token)})
                return

            if api_path == "/api/watchlist/delete":
                require_host_viewer(state, viewer_token)
                item = remove_watchlist_item(
                    state,
                    film_id=(payload.get("filmId") or "").strip(),
                )
                self._send_json({"ok": True, "item": item, "state": state_payload(state, viewer_token)})
                return

            if api_path == "/api/watchlist/import":
                require_host_viewer(state, viewer_token)
                result = refresh_watchlist_from_personal(state)
                self._send_json({"ok": True, "result": result, "state": state_payload(state, viewer_token)})
                return

            if api_path == "/api/session/create":
                require_host_viewer(state, viewer_token)
                session = create_session(
                    state,
                    payload.get("attendeeIds") or [],
                    payload.get("scheduledFor") or "",
                )
                self._send_json({"ok": True, "sessionId": session["id"], "state": state_payload(state, viewer_token)})
                return

            if api_path == "/api/session/vote":
                require_viewer_identity(
                    state,
                    viewer_token,
                    (payload.get("participantId") or "").strip(),
                )
                session = submit_vote(
                    state,
                    participant_id=(payload.get("participantId") or "").strip(),
                    ranking=payload.get("ranking") or [],
                )
                self._send_json(
                    {
                        "ok": True,
                        "votesLocked": len(session.get("votes", {})),
                        "votesNeeded": len(session.get("attendeeIds", [])),
                        "state": state_payload(state, viewer_token),
                    }
                )
                return

            if api_path == "/api/session/snacks":
                require_viewer_identity(
                    state,
                    viewer_token,
                    (payload.get("participantId") or "").strip(),
                )
                session = set_snack_assignment(
                    state,
                    participant_id=(payload.get("participantId") or "").strip(),
                    snack_id=(payload.get("snackId") or "").strip(),
                )
                self._send_json({"ok": True, "sessionId": session.get("id"), "state": state_payload(state, viewer_token)})
                return

            if api_path == "/api/session/reveal-next":
                require_host_viewer(state, viewer_token)
                session = reveal_next(state)
                self._send_json({"ok": True, "revealedCount": session.get("revealedCount", 0), "state": state_payload(state, viewer_token)})
                return

            if api_path == "/api/session/reset-reveal":
                require_host_viewer(state, viewer_token)
                session = reset_reveal(state)
                self._send_json({"ok": True, "revealedCount": session.get("revealedCount", 0), "state": state_payload(state, viewer_token)})
                return

            if api_path == "/api/session/reset":
                require_host_viewer(state, viewer_token)
                reset_session(state)
                self._send_json({"ok": True, "state": state_payload(state, viewer_token)})
                return

            if api_path == "/api/comments":
                require_viewer_identity(
                    state,
                    viewer_token,
                    (payload.get("participantId") or "").strip(),
                )
                comment = add_comment(
                    state,
                    participant_id=(payload.get("participantId") or "").strip(),
                    text=(payload.get("text") or ""),
                )
                self._send_json({"ok": True, "comment": comment, "state": state_payload(state, viewer_token)})
                return

            self._send_error_json("Unbekannter API-Endpunkt.", status=HTTPStatus.NOT_FOUND)
        except ValueError as exc:
            self._send_error_json(str(exc), status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._send_error_json(f"Interner Fehler: {exc}", status=HTTPStatus.INTERNAL_SERVER_ERROR)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local server for the group film night MVP.")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Host to bind to. Default: {DEFAULT_HOST}")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Port to bind to. Default: {DEFAULT_PORT}")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    ensure_state()
    server = ThreadingHTTPServer((args.host, args.port), GroupFilmNightHandler)
    print(f"Group Film Night dashboard: http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
