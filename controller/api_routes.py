"""
API routes for PitBox Controller web UI (simplified - no presets).
"""
# -----------------------------------------------------------------------------
# How Content Manager gets online server information (for reference; PitBox does
# not implement the lobby client; we only consume CM favourites and query servers).
# -----------------------------------------------------------------------------
# Content Manager does NOT maintain its own online server database. It gets
# online server information from the Assetto Corsa master lobby / lobby service
# that public AC servers register with.
#
# Flow:
#   - Dedicated Assetto Corsa servers start up and register themselves with the
#     official AC lobby / master server.
#   - Content Manager requests the server list from that lobby service.
#   - The lobby response provides basic server data, e.g.:
#       server IP / host, port, server name, track, car list, player count / slots
#   - After receiving the list, Content Manager can then ping or query individual
#     servers directly for fresh status (latency, availability, etc.).
#
# Important distinction:
#   - Content Manager is only a client/browser for the lobby list.
#   - It is not the original source of server discovery.
#   - The source of discovery is the Assetto Corsa lobby / master server.
#
# For local/private setups, servers may also be found by:
#   - LAN discovery
#   - Direct IP connection
#   - Favourites / saved servers (e.g. Favourites.txt)
#
# PitBox supports: (1) preset servers from disk, (2) CM Favourites.txt.
# -----------------------------------------------------------------------------

import asyncio
import copy
import hashlib
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

# Build id for X-PitBox-Build header (set at import time).
BUILD_ID = time.strftime("%Y%m%d%H%M%S", time.gmtime())

import psutil
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from controller.agent_poller import get_status_cache, send_agent_command
from controller.kiosk import (
    create_pair_data,
    create_session,
    get_controller_state,
    get_kiosk_agent_state,
    get_session,
    get_session_for_agent,
    invalidate_session,
    set_assigned_state,
    set_kiosk_agent_state,
    verify_token as kiosk_verify_token,
)
from controller.security import get_registry, require_agent
from controller.operator_auth import (
    EMPLOYEE_COOKIE,
    get_employee_password_optional,
    require_employee,
    require_operator,
    require_operator_if_password_configured,
    sanitize_employee_login_next,
)
from pitbox_common.safe_inputs import validate_steering_shifting_preset_basename
from controller.config import (
    get_config,
    get_config_path,
    load_config,
    save_config,
    UpdateChannelConfig,
    get_controller_http_url,
    get_ac_server_root,
    get_ac_server_presets_root,
    get_preset_dir,
    list_server_preset_ids,
)
from controller.config import ControllerConfig, AgentInfo
from controller.ac_paths import (
    _car_cache_dir,
    _cars_dir,
    _cfg_dir_for_server,
    _content_root,
    _server_config_paths_for_read,
    _track_cache_dir,
    _tracks_dir,
)
from controller.discovery import get_discovered
from controller.ini_io import read_ini, write_ini, write_ini_atomic, _ini_value
from controller.enrollment import get_state as get_enrollment_state, is_enabled as enrollment_is_enabled, start_enrollment, stop_enrollment, verify_secret as enrollment_verify_secret
from controller.enrolled_rigs import add as enrolled_add, add_cm as enrolled_add_cm, get as enrolled_get, get_all_ordered as enrolled_get_all_ordered, get_agent_id_by_display_name, get_display_name_for_rig, load as load_enrolled_rigs, remove as enrolled_remove
from controller.enrollment_broadcast import set_controller_url_provider, start as start_enrollment_broadcast
from controller.common.event_log import LogCategory as EventLogCategory, LogLevel as EventLogLevel, make_event as make_log_event
from controller.service.event_store import append_event as event_store_append
from controller.telemetry_models import AgentStatusBody, TelemetryTickBody
from controller.telemetry_store import ingest_telemetry, ingest_status, build_timing_snapshot
from controller.updater import (
    apply_controller_update,
    clear_update_cache,
    get_update_status,
    get_updater_status,
    run_unified_installer_update,
)
from controller.cm_favourites import get_favourites_debug_info, load_favourites_servers
from controller.server_preset_helpers import (
    PRESETS_DIR_DEBUG,
    STFOLDER_NAME,
    discover_presets,
    get_live_server_info,
    get_merged_server_ids,
    parse_ac_server_cfg,
    _build_favourite_server_cfg_snapshot,
    _get_car_display_name,
    _get_cached_preset_disk_state,
    _get_favourite_by_id,
    _get_server_preset_dir_safe,
    _invalidate_preset_disk_state_cache,
    _normalize_track_id_from_preset,
    _parse_ui_car_json,
    _prettify_car_id,
    _preset_ini_paths,
    _set_cached_preset_disk_state,
    _get_server_join_host,
    _valid_server_id,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")

# In-memory sim -> server assignment (agent_id -> server_id). Persisted to sim_assignments.json.
_assignments: dict[str, str] = {}

_ASSIGNMENTS_FILENAME = "sim_assignments.json"


def _assignments_file_path() -> Path:
    """Path to sim_assignments.json (same dir as controller config, or cwd)."""
    try:
        cp = get_config_path()
        if cp:
            return Path(cp).resolve().parent / _ASSIGNMENTS_FILENAME
    except Exception:
        pass
    return Path(os.getcwd()) / _ASSIGNMENTS_FILENAME


def load_sim_assignments() -> None:
    """Load agent_id -> server_id from sim_assignments.json. Call on startup."""
    global _assignments
    path = _assignments_file_path()
    if not path.is_file():
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            _assignments = {str(k): str(v).strip() for k, v in data.items() if v}
            logger.info("Loaded %d sim display assignment(s) from %s", len(_assignments), path)
    except Exception as e:
        logger.warning("Could not load sim assignments from %s: %s", path, e)


def _save_sim_assignments() -> None:
    """Write _assignments to sim_assignments.json."""
    path = _assignments_file_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(_assignments, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning("Could not save sim assignments to %s: %s", path, e)


def _car_availability_for_server(server_id: str, summary: dict, cache: dict) -> dict[str, dict[str, int]]:
    """
    Per-car availability for a server: total slots, taken (by sims on this server), left.
    Used so sim display can show "N left" and gray out cars with 0 left.
    """
    slots = summary.get("slots") or []
    cars_list = summary.get("cars") or []
    total_by_car: dict[str, int] = {}
    for s in slots:
        car = (s.get("car") or "").strip()
        if car:
            total_by_car[car] = total_by_car.get(car, 0) + 1
    for c in cars_list:
        c = (c or "").strip()
        if c and c not in total_by_car:
            total_by_car[c] = 0
    taken_by_car: dict[str, int] = {}
    sid = (server_id or "").strip()
    for aid, srv_id in _assignments.items():
        if (srv_id or "").strip() != sid:
            continue
        car_id = None
        status = cache.get(aid) if cache else None
        if status and getattr(status, "last_session", None):
            ls = status.last_session
            car_id = (ls.get("car_id") or ls.get("car") or "").strip()
        if not car_id:
            st = get_controller_state(aid)
            sel = st.assigned_selection or {}
            car_id = (sel.get("car_id") or ((sel.get("car") or {}).get("car_id")) or "").strip()
        if car_id:
            taken_by_car[car_id] = taken_by_car.get(car_id, 0) + 1
    out: dict[str, dict[str, int]] = {}
    for car in total_by_car:
        total = total_by_car[car]
        taken = taken_by_car.get(car, 0)
        out[car] = {"total": total, "taken": taken, "left": max(0, total - taken) if total > 0 else 999}
    return out


def _resolve_assignment(agent_id: str) -> tuple[str | None, str | None]:
    """
    Return (canonical_agent_id, server_id) for server-display.
    Uses case-insensitive match so ?agent_id=sim5 matches stored Sim5.
    """
    if not agent_id or not (agent_id := agent_id.strip()):
        return None, None
    if agent_id in _assignments:
        return agent_id, _assignments[agent_id]
    aid_lower = agent_id.lower()
    for key, sid in _assignments.items():
        if key.lower() == aid_lower:
            return key, sid
    return None, None


def _validate_steering_shifting_name_http(name: str) -> str:
    try:
        return validate_steering_shifting_preset_basename(name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


# Cache for server summary: server_id -> (cached_at_timestamp, payload)
_server_summary_cache: dict[str, tuple[float, dict]] = {}
_SERVER_SUMMARY_CACHE_TTL_SEC = 30.0

# Catalog caches (cars, tracks, assists, servers) - TTL seconds
_CATALOG_CACHE_TTL = 60.0
_catalog_cars_cache: Optional[tuple[float, list]] = None
_catalog_tracks_cache: Optional[tuple[float, list]] = None
_catalog_assists_cache: Optional[tuple[float, list]] = None
_catalog_servers_cache: Optional[tuple[float, list]] = None


# Request/Response models
class StartStopRequest(BaseModel):
    sim_ids: Optional[list[str]] = None
    all: bool = False
    steering_presets: Optional[dict[str, str]] = None  # sim_id -> preset_name (applied before launch)


class StartStopResponse(BaseModel):
    results: dict[str, dict]
    ok: bool = True
    dispatched: list[dict] = Field(default_factory=list)  # [{ id, ok, error? }]


class StatusResponse(BaseModel):
    agents: list[dict]


# ---- Agent registry (for UI) and agent-facing heartbeat ----
@router.get("/agents/registry")
async def get_agents_registry(_: None = Depends(require_operator)):
    """Return current agent registry (agent_id -> record) for UI display."""
    registry = get_registry()
    return registry.all_records()


@router.get("/agents/discovered")
async def get_agents_discovered(_: None = Depends(require_operator)):
    """Return agents discovered on LAN via UDP beacon (for enrollment/pairing)."""
    return {"agents": get_discovered()}


@router.post("/heartbeat")
async def heartbeat(agent_id: str = Depends(require_agent)):
    """Agent-facing: register/update IP and validate token. Call with X-Agent-Id and X-Agent-Token."""
    return {"status": "ok", "agent_id": agent_id}


@router.post("/agents/status")
async def agents_status(body: AgentStatusBody, agent_id: str = Depends(require_agent)):
    """Accept agent_status from agents (boot and every 5s). Store for timing/snapshot context."""
    canonical = (agent_id or "").strip() or (body.agent_id or "").strip()
    if canonical:
        ingest_status(canonical, body)
    return {"status": "ok", "agent_id": canonical}


@router.post("/agents/telemetry")
async def agents_telemetry(body: TelemetryTickBody, agent_id: str = Depends(require_agent)):
    """Accept telemetry_tick from agents. Store last per agent; ignore stale/out-of-order (seq, ts_ms)."""
    canonical = (agent_id or "").strip() or (body.agent_id or "").strip()
    if not canonical:
        raise HTTPException(status_code=400, detail="agent_id required")
    stored = ingest_telemetry(canonical, body)
    return {"status": "ok", "agent_id": canonical, "stored": stored}


@router.get("/timing/snapshot")
async def timing_snapshot(_: None = Depends(require_operator_if_password_configured)):
    """Return fused timing_snapshot from last telemetry per agent (1–5 Hz polling)."""
    snap = build_timing_snapshot()
    return snap.model_dump()


# ---- Enrollment mode + auto-pair ----

class EnrollmentSetBody(BaseModel):
    enabled: bool = False


@router.get("/enrollment")
async def get_enrollment(_: None = Depends(require_operator_if_password_configured)):
    """
    Enrollment mode state for the operator UI (toggle + countdown + secret display).

    **Payload (from ``get_enrollment_state``):**
    - ``enabled`` (bool): operational metadata — whether the enrollment window is active.
    - ``until_ts`` (float): operational metadata — Unix expiry time.
    - ``seconds_remaining`` (int): operational metadata — countdown.
    - ``secret`` (str): **credential-like** — current enrollment shared secret; must not be
      readable by unauthenticated clients when ``employee_password`` is set.

    **Classification:** operational metadata plus a live secret. Gated with
    ``require_operator_if_password_configured`` so LAN stays open without password (legacy),
    but remote/anonymous clients cannot harvest the secret when login is enabled.
    """
    return get_enrollment_state()


@router.post("/enrollment")
async def set_enrollment(body: EnrollmentSetBody, _: None = Depends(require_operator)):
    """Turn enrollment mode ON (10 min) or OFF."""
    if body.enabled:
        secret = start_enrollment()
        start_enrollment_broadcast()
        try:
            event_store_append(make_log_event(EventLogLevel.INFO, EventLogCategory.SYSTEM, "Controller", "Enrollment started", details={"seconds_remaining": 600}))
        except Exception:
            pass
        return {"enabled": True, "seconds_remaining": 600, "secret": secret}
    stop_enrollment()
    try:
        event_store_append(make_log_event(EventLogLevel.INFO, EventLogCategory.SYSTEM, "Controller", "Enrollment stopped"))
    except Exception:
        pass
    return {"enabled": False, "seconds_remaining": 0}


class PairEnrollBody(BaseModel):
    device_id: str
    hostname: Optional[str] = None
    agent_port: int = 9631
    enrollment_secret: str = ""
    host: Optional[str] = None  # agent IP; if omitted, use request client


@router.post("/pair/enroll")
async def pair_enroll(request: Request, body: PairEnrollBody):
    """Accept enrollment from an unpaired agent. Only when enrollment mode is ON and secret matches."""
    if not enrollment_is_enabled():
        raise HTTPException(status_code=403, detail="Enrollment mode is off or expired")
    if not enrollment_verify_secret(body.enrollment_secret or ""):
        raise HTTPException(status_code=403, detail="Invalid enrollment secret")
    device_id = (body.device_id or "").strip()
    if not device_id:
        raise HTTPException(status_code=400, detail="device_id required")
    port = int(body.agent_port) if body.agent_port else 9631
    if not (1 <= port <= 65535):
        raise HTTPException(status_code=400, detail="agent_port must be 1-65535")
    host = (body.host or "").strip()
    if not host:
        host = request.client.host if request.client else ""
    if not host:
        raise HTTPException(status_code=400, detail="Could not determine agent host")
    try:
        token = enrolled_add(device_id, host, port, body.hostname)
    except ValueError as e:
        logger.warning("Enrollment rejected for device_id=%r: %s", device_id, e)
        raise HTTPException(status_code=400, detail=str(e))
    logger.info("Enrolled rig %s at %s:%s", device_id, host, port)
    try:
        event_store_append(make_log_event(EventLogLevel.INFO, EventLogCategory.RIG, "Controller", "Rig enrolled", rig_id=device_id, details={"host": host, "port": port}))
    except Exception:
        pass
    return {"success": True, "token": token, "agent_id": device_id}


@router.post("/pair/unenroll")
async def pair_unenroll(agent_id: str = Depends(require_agent)):
    """Agent unenrolls itself (e.g. when user unpairs). Removes rig from controller; sim card will disappear after refresh."""
    canonical = (agent_id or "").strip()
    if not canonical:
        raise HTTPException(status_code=400, detail="agent_id required")
    if enrolled_remove(canonical):
        logger.info("Rig unenrolled (agent requested): %s", canonical)
        try:
            event_store_append(make_log_event(EventLogLevel.INFO, EventLogCategory.RIG, "Controller", "Rig unenrolled (agent requested)", rig_id=canonical))
        except Exception:
            pass
        return {"success": True, "message": "Unenrolled"}
    raise HTTPException(status_code=404, detail="Agent not enrolled")


@router.delete("/agents/{agent_id}")
async def remove_agent(agent_id: str, _: None = Depends(require_operator)):
    """Remove a rig from the controller (admin). Sim card will disappear from UI. agent_id can be display name (e.g. Sim5) or device_id."""
    canonical = _canonical_agent_id(agent_id)
    if not canonical:
        raise HTTPException(status_code=404, detail="Unknown agent_id")
    if enrolled_remove(canonical):
        logger.info("Rig removed from controller UI: %s", canonical)
        try:
            event_store_append(make_log_event(EventLogLevel.INFO, EventLogCategory.RIG, "Controller", "Rig removed", rig_id=canonical))
        except Exception:
            pass
        return {"ok": True, "agent_id": canonical}
    raise HTTPException(status_code=404, detail="Agent not found")


class AddCmRigBody(BaseModel):
    """Add a rig that uses Content Manager remote control (no PitBox Agent on sim)."""
    agent_id: str
    host: str
    cm_port: int = 11777
    cm_password: Optional[str] = ""
    display_name: Optional[str] = None


@router.post("/rigs/cm")
async def add_cm_rig(body: AddCmRigBody, _: None = Depends(require_operator)):
    """Add or update a rig that uses Content Manager remote control API. Sim control only (presets, single-player sessions)."""
    agent_id = (body.agent_id or "").strip()
    host = (body.host or "").strip()
    if not agent_id:
        raise HTTPException(status_code=400, detail="agent_id required")
    if not host:
        raise HTTPException(status_code=400, detail="host required")
    try:
        enrolled_add_cm(
            agent_id=agent_id,
            host=host,
            cm_port=body.cm_port or 11777,
            cm_password=body.cm_password or "",
            display_name=(body.display_name or "").strip() or None,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    try:
        event_store_append(make_log_event(EventLogLevel.INFO, EventLogCategory.RIG, "Controller", "CM rig added/updated", rig_id=agent_id, details={"host": host, "cm_port": body.cm_port}))
    except Exception:
        pass
    return {"ok": True, "agent_id": agent_id, "message": "CM rig added or updated"}


# ---- Employee Control (mobile): login + hotkey ----
class EmployeeLoginBody(BaseModel):
    """Employee login request."""
    password: str = ""
    next: str | None = None


@router.post("/employee/login")
async def employee_login(request: Request, response: Response, body: EmployeeLoginBody):
    """Validate password and set session cookie. Returns 200 on success."""
    pw = get_employee_password_optional()
    if pw is None:
        raise HTTPException(status_code=403, detail="Employee Control is disabled")
    if (body.password or "").strip() != pw:
        raise HTTPException(status_code=401, detail="Invalid password")
    response.set_cookie(
        key=EMPLOYEE_COOKIE,
        value="1",
        path="/",
        max_age=86400 * 7,  # 7 days
        httponly=True,
        samesite="lax",
    )
    return {"ok": True, "redirect": sanitize_employee_login_next(body.next)}


@router.post("/employee/logout")
async def employee_logout(response: Response):
    """Clear employee session cookie."""
    response.delete_cookie(key=EMPLOYEE_COOKIE, path="/")
    return {"ok": True}


@router.get("/employee/session")
async def employee_session(request: Request):
    """Whether operator (employee) login is configured and whether this browser has a session cookie."""
    pw = get_employee_password_optional()
    logged_in = request.cookies.get(EMPLOYEE_COOKIE) == "1"
    enabled = pw is not None
    effective_logged_in = logged_in if enabled else False
    return {
        "employee_login_enabled": enabled,
        "logged_in": effective_logged_in,
        "login_required_for_control": enabled and not effective_logged_in,
        "require_cookie_for_remote_control": enabled,
        # When no password is set all LAN clients have open access (not localhost-only).
        "localhost_only_without_password": False,
    }


class HotkeyBody(BaseModel):
    """Hotkey action for employee control."""
    action: str  # "toggle_manual" | "back_to_pits"


@router.post("/agents/{agent_id}/hotkey")
async def agent_hotkey(agent_id: str, body: HotkeyBody, _: None = Depends(require_employee)):
    """Send Ctrl+G (toggle AUTO/MANUAL) or Ctrl+P (back to pits) to the agent. Employee session required."""
    canonical = _canonical_agent_id(agent_id)
    if not canonical:
        raise HTTPException(status_code=404, detail="Unknown agent_id")
    action = (body.action or "").strip().lower()
    if action not in ("toggle_manual", "back_to_pits"):
        raise HTTPException(status_code=400, detail="action must be toggle_manual or back_to_pits")
    result = await send_agent_command(canonical, "hotkey", {"action": action})
    if not result.get("success"):
        raise HTTPException(status_code=502, detail=result.get("message", "Agent request failed"))
    return result


def _enrich_last_session(ls: dict | None) -> dict | None:
    """Add car_name and track_name from content ui_car.json / ui_track.json for UI display."""
    if not ls or not isinstance(ls, dict):
        return ls
    out = dict(ls)
    car_id = (ls.get("car") or "").strip()
    track_id = (ls.get("track") or "").strip()
    layout = (ls.get("layout") or "").strip() or "default"
    if car_id and car_id != "—":
        out["car_name"] = _get_car_display_name(car_id) or car_id
    else:
        out["car_name"] = ls.get("car_name") or "—"
    if track_id and track_id != "—":
        out["track_name"] = _get_track_display_name(track_id, layout) or track_id
    else:
        out["track_name"] = ls.get("track_name") or "—"
    return out


def _sim_order_key(agent: dict) -> tuple:
    """
    Sort key so sim cards show as Sim 1, Sim 2, ... Sim 5, Sim 10 (natural order).
    Extracts number from display_name or agent_id (e.g. 'Sim 5' -> 5, 'Sim5' -> 5); fallback to string.
    """
    dn = (agent.get("display_name") or "").strip()
    aid = (agent.get("agent_id") or "").strip()
    for s in (dn, aid):
        if not s:
            continue
        m = re.search(r"sim\s*[-_]?\s*(\d+)", s, re.IGNORECASE)
        if m:
            return (int(m.group(1)), dn, aid)
        if s.isdigit():
            return (int(s), dn, aid)
    return (999, dn, aid)


# Endpoints
@router.get("/status")
async def get_status(_: None = Depends(require_operator_if_password_configured)):
    """
    Rig grid + assignments + running AC instances + poll hint.

    **Auth:** `require_operator_if_password_configured` — public on LAN only when `employee_password`
    is unset; when set, same as operator (cookie or localhost-only without password).

    **Payload (all operational; justified only while unauthenticated LAN is allowed without password):**
    - **agents[]**: per-rig UI (connect targets, presets, session thumbnails, online state). Includes
      host/port (enrollment targets), last_session (track/car for cards), heartbeat/AC snippets.
    - **server_ids**: merged preset + CM favourite ids for dropdowns.
    - **assignments**: sim → server_id for display/join.
    - **servers**: running acServer rows for dashboard.
    - **poll_interval_sec**: UI poll tuning (non-secret scalar).

    When `employee_password` is configured, unauthenticated clients do not receive this payload.
    """
    cache = get_status_cache()
    config = get_config()
    enrolled = enrolled_get_all_ordered()
    agents = []
    for i, rig in enumerate(enrolled):
        agent_id = (rig.get("agent_id") or "").strip()
        host = rig.get("host") or ""
        backend = (rig.get("backend") or "agent").strip().lower()
        port = int(rig.get("port") or rig.get("cm_port") or 0)
        display_name = get_display_name_for_rig(rig, i)
        agent_status = cache.get(agent_id)
        if agent_status:
            last_session = getattr(agent_status, "last_session", None)
            last_session = _enrich_last_session(last_session)
            agents.append({
                "agent_id": agent_status.agent_id,
                "host": host,
                "port": port,
                "backend": backend,
                "online": agent_status.online,
                "error": agent_status.error,
                "ac_running": agent_status.ac_running,
                "pid": agent_status.pid,
                "uptime_sec": agent_status.uptime_sec,
                "last_check": agent_status.last_check.isoformat(),
                "steering_presets": getattr(agent_status, "steering_presets", []) or [],
                "shifting_presets": getattr(agent_status, "shifting_presets", []) or [],
                "display_name": display_name,
                "ts": getattr(agent_status, "ts", None),
                "heartbeat": getattr(agent_status, "heartbeat", None),
                "ac": getattr(agent_status, "ac", None),
                "server": getattr(agent_status, "server", None) or {"state": "UNAVAILABLE", "source": "UNKNOWN"},
                "last_session": last_session,
                "control_mode": getattr(agent_status, "control_mode", None) or "AUTO",
            })
        else:
            agents.append({
                "agent_id": agent_id,
                "host": host,
                "port": port,
                "backend": backend,
                "online": False,
                "error": "UNREACHABLE",
                "ac_running": False,
                "pid": None,
                "uptime_sec": None,
                "last_check": None,
                "steering_presets": [],
                "shifting_presets": [],
                "display_name": display_name,
                "ts": None,
                "heartbeat": None,
                "ac": None,
                "server": {"state": "UNAVAILABLE", "source": "UNKNOWN"},
                "last_session": None,
                "control_mode": "AUTO",
            })

    # Order sim cards by sim number (Sim 1, Sim 2, ... Sim 5, Sim 10), not enrollment order
    agents.sort(key=_sim_order_key)

    # Presets + Content Manager favourites (merged, deduped by ip:port).
    server_ids = get_merged_server_ids()
    running_servers = _get_running_servers_list()
    # Public-safe UI tuning only (no paths, secrets, or agent structure). Same source as controller config.
    try:
        poll_interval_sec = float(getattr(config, "poll_interval_sec", 1.5) or 1.5)
    except (TypeError, ValueError):
        poll_interval_sec = 1.5
    if poll_interval_sec <= 0 or poll_interval_sec > 3600:
        poll_interval_sec = 1.5
    return {
        "agents": agents,
        "server_ids": server_ids,
        "assignments": dict(_assignments),
        "servers": running_servers,
        "poll_interval_sec": poll_interval_sec,
    }


# ---- Sim assignment and server display ----

def _read_full_server_cfg(server_id: str) -> tuple[dict[str, dict[str, str]] | None, Path | None]:
    """Read full server_cfg.ini from preset. Returns (parsed_dict, sc_path) or (None, None) if missing."""
    if not server_id or not _valid_server_id(server_id):
        return None, None
    try:
        preset_dir = _get_server_preset_dir_safe(server_id)
        sc_path, _ = _preset_ini_paths(preset_dir)
        if not sc_path.exists():
            return None, None
        data = read_ini(sc_path)
        return data, sc_path
    except Exception as e:
        logger.debug("_read_full_server_cfg: server_id=%s e=%s", server_id, e)
        return None, None


def _build_server_summary(server_id: str, *, skip_cache: bool = False) -> dict:
    """
    Build server summary from preset dir (server_cfg.ini + entry_list.ini).
    Shared by GET /api/servers/{server_id}/summary and GET /api/sims/{agent_id}/server-display.
    Uses Option B path layout: primary preset root, fallback preset/cfg/.
    Raises HTTPException on malformed INI or missing preset.
    When skip_cache=True (e.g. for sim display), always read from disk so track/cars stay in sync with assignment.
    """
    now = time.time()
    if not skip_cache and server_id in _server_summary_cache:
        cached_at, payload = _server_summary_cache[server_id]
        if now - cached_at < _SERVER_SUMMARY_CACHE_TTL_SEC:
            return payload
    preset_dir = _get_server_preset_dir_safe(server_id)
    sc_path, el_path = _preset_ini_paths(preset_dir)
    if not sc_path.exists() and not el_path.exists():
        # Preset folder exists but no config yet: return minimal summary so sim UI still shows the server
        logger.info(
            "[server-summary] requested=%s resolved=%s preset_path=%s track_raw=(no config) config_raw=(no config)",
            server_id, server_id, preset_dir,
        )
        return {
            "server_id": server_id,
            "name": "",
            "track": {"id": "", "config": "", "name": ""},
            "mode": "pickup",
            "cars": [],
            "cars_with_skins": [],
            "slots": [],
            "updated_at": "",
        }
    try:
        server_ini = read_ini(sc_path) if sc_path.exists() else {}
    except Exception as e:
        logger.exception("Failed to parse server_cfg.ini at %s: %s", sc_path, e)
        raise HTTPException(
            status_code=500,
            detail=f"Malformed server_cfg.ini: {e!s}",
        )
    try:
        entry_ini = read_ini(el_path) if el_path.exists() else {}
    except Exception as e:
        logger.exception("Failed to parse entry_list.ini at %s: %s", el_path, e)
        raise HTTPException(
            status_code=500,
            detail=f"Malformed entry_list.ini: {e!s}",
        )
    # SERVER section (case-insensitive)
    server_opts = {}
    for sect, opts in server_ini.items():
        if sect.upper() == "SERVER":
            server_opts = {k.upper(): (v or "").strip() for k, v in opts.items()}
            break
    name = server_opts.get("NAME", "")
    track_id_raw = server_opts.get("TRACK", "").strip()
    config_raw = server_opts.get("CONFIG_TRACK", "").strip()
    logger.info(
        "[server-summary] requested=%s resolved=%s preset_path=%s track_raw=%s config_raw=%s",
        server_id, server_id, preset_dir, track_id_raw or "(empty)", config_raw or "(empty)",
    )
    cars_raw = server_opts.get("CARS", "").strip()
    cars_list = [c.strip() for c in cars_raw.split(";") if c.strip()]
    track_payload = _build_safe_track_payload(server_id, track_id_raw, config_raw)
    # Skins per car (for sim display); dedupe by car_id
    cars_with_skins: list[dict[str, Any]] = []
    try:
        cars_dir = _cars_dir()
        seen: set[str] = set()
        for car_id in cars_list:
            if car_id in seen:
                continue
            seen.add(car_id)
            car_folder = cars_dir / car_id
            skins = _list_skins_for_car(car_folder)
            cars_with_skins.append({"car": car_id, "skins": skins})
    except Exception as e:
        logger.debug("Could not list skins for cars: %s", e)
    # Slots from entry_list.ini [CAR_0], [CAR_1], ...
    slots: list[dict[str, Any]] = []
    car_sections = [s for s in entry_ini if s.upper().startswith("CAR_")]
    for sect in sorted(car_sections, key=lambda s: (len(s), s)):
        opts = entry_ini[sect]
        try:
            slot_num = int(sect.replace("CAR_", "").strip()) if "CAR_" in sect.upper() else len(slots) + 1
        except ValueError:
            slot_num = len(slots) + 1
        car_model = (opts.get("MODEL") or "").strip()
        skin = (opts.get("SKIN") or "").strip()
        slots.append({"slot": slot_num, "car": car_model, "skin": skin})
    mode = "slots" if slots else "pickup"
    # updated_at: max mtime of both files, ISO8601
    updated_ts = 0.0
    for p in (sc_path, el_path):
        if p.exists():
            try:
                updated_ts = max(updated_ts, p.stat().st_mtime)
            except OSError:
                pass
    updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(updated_ts)) if updated_ts else ""
    payload = {
        "server_id": server_id,
        "name": name,
        "track": track_payload,
        "mode": mode,
        "cars": cars_list,
        "cars_with_skins": cars_with_skins,
        "slots": slots,
        "updated_at": updated_at,
    }
    _server_summary_cache[server_id] = (now, payload)
    return payload


@router.get("/assignments/{agent_id}")
async def get_assignment(agent_id: str, _: None = Depends(require_operator_if_password_configured)):
    """Return the server assignment for an agent. 404 if not assigned."""
    canonical, server_id = _resolve_assignment(agent_id)
    if not server_id:
        raise HTTPException(status_code=404, detail="No server assigned")
    return {"agent_id": canonical or agent_id, "server_id": server_id}


class AssignmentSetBody(BaseModel):
    server_id: str


@router.post("/assignments/{agent_id}")
async def set_assignment(agent_id: str, body: AssignmentSetBody, _: None = Depends(require_operator)):
    """Set or clear the server assignment for an agent (control UI). Empty server_id clears."""
    sid = (body.server_id or "").strip()
    # Use canonical key if we already have this agent (case-insensitive) so we don't duplicate keys
    canonical, _ = _resolve_assignment(agent_id)
    key = (canonical or agent_id.strip()) if agent_id else ""
    if not key:
        raise HTTPException(status_code=400, detail="Invalid agent_id")
    if not sid:
        if canonical:
            _assignments.pop(canonical, None)
        _save_sim_assignments()
        logger.info("[assignment] agent_id=%s server_id=(cleared)", key)
        return {"ok": True, "agent_id": key, "server_id": None}
    if not _valid_server_id(sid):
        raise HTTPException(status_code=400, detail="Invalid server_id")
    _assignments[key] = sid
    _save_sim_assignments()
    logger.info("[assignment] agent_id=%s server_id=%s", key, sid)
    return {"ok": True, "agent_id": key, "server_id": sid}


# ---- Kiosk mode: pairing, session, command, display ----

def _canonical_agent_id(agent_id: str) -> Optional[str]:
    """Return canonical agent_id from enrolled rigs. Matches by agent_id (device_id) or by display name (e.g. Sim5, Sim 5)."""
    if not agent_id or not (agent_id := agent_id.strip()):
        return None
    for r in enrolled_get_all_ordered():
        aid = (r.get("agent_id") or "").strip()
        if aid.lower() == agent_id.lower():
            return aid
    by_display = get_agent_id_by_display_name(agent_id)
    if by_display:
        return by_display
    return None


class KioskClaimBody(BaseModel):
    """Body for POST /api/kiosk/claim (from QR scan)."""
    agent_id: str
    nonce: str
    token: str


@router.get("/kiosk/pair-info")
async def kiosk_pair_info(agent_id: str = ""):
    """Return agent_id, nonce, token for QR. Sim display builds qr_url from origin + /kiosk?agent_id=...&nonce=...&token=..."""
    canonical = _canonical_agent_id(agent_id)
    if not canonical:
        raise HTTPException(status_code=400, detail="Invalid or unknown agent_id")
    data = create_pair_data(canonical)
    if not data:
        raise HTTPException(status_code=500, detail="Could not create pair data")
    return data


@router.post("/kiosk/claim")
async def kiosk_claim(body: KioskClaimBody):
    """Validate QR token and create kiosk session. Returns session_id for subsequent commands."""
    canonical = _canonical_agent_id(body.agent_id)
    if not canonical:
        raise HTTPException(status_code=400, detail="Invalid or unknown agent_id")
    if not kiosk_verify_token(canonical, body.nonce, body.token):
        raise HTTPException(status_code=403, detail="Invalid or expired pair token")
    session = create_session(canonical)
    if not session:
        raise HTTPException(status_code=500, detail="Could not create session")
    state = get_kiosk_agent_state(canonical)
    state.status = "paired"
    return {
        "session_id": session.session_id,
        "agent_id": canonical,
        "expires_at": session.expires_at,
    }


@router.get("/sims/{agent_id}/kiosk-display")
async def get_sim_kiosk_display(agent_id: str):
    """One-call for sim display: when idle return qr payload; when paired return status + selection (read-only)."""
    canonical = _canonical_agent_id(agent_id)
    if not canonical:
        raise HTTPException(status_code=404, detail="Unknown agent_id")
    state = get_kiosk_agent_state(canonical)
    session = get_session_for_agent(canonical)
    if not session:
        # Idle: show QR
        pair_data = create_pair_data(canonical)
        return JSONResponse(
            content={
                "agent_id": canonical,
                "status": "idle",
                "qr": pair_data,
                "selection": None,
                "applied": None,
                "warnings": [],
                "errors": [],
            },
            headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
        )
    return JSONResponse(
        content={
            "agent_id": canonical,
            "status": state.status,
            "qr": None,
            "selection": state.selection,
            "applied": state.applied,
            "warnings": state.warnings or [],
            "errors": state.errors or [],
        },
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


# Per-agent last results signature we sent navigate_to_results for (one-time per completed race).
_sim_results_navigated: dict[str, str] = {}


@router.get("/sims/{agent_id}/state")
async def get_sim_state(agent_id: str):
    """Sim display state: assigned (controller), detected (agent last_session), process (acs_running, pid).
    When not paired: includes server_display (track, cars) for idle kiosk. paired=true when QR session exists."""
    canonical = _canonical_agent_id(agent_id)
    if not canonical:
        raise HTTPException(status_code=404, detail="Unknown agent_id")
    ctrl = get_controller_state(canonical)
    cache = get_status_cache()
    agent_status = cache.get(canonical)
    assigned = {
        "status": ctrl.assigned_status,
        "updated_at": datetime.fromtimestamp(ctrl.updated_at, tz=timezone.utc).isoformat() if ctrl.updated_at else None,
        "selection": ctrl.assigned_selection,
    }
    detected = {
        "last_session": agent_status.last_session if agent_status else None,
        "updated_at": agent_status.last_check.isoformat() if (agent_status and getattr(agent_status, "last_check", None)) else None,
    }
    process = {
        "acs_running": agent_status.ac_running if agent_status else False,
        "pid": agent_status.pid if agent_status else None,
    }
    qr = create_pair_data(canonical)
    paired = get_session_for_agent(canonical) is not None
    # Server assignment from dropdown (sim_assignments) — kiosk screen reads server from here
    _, assignment_server_id = _resolve_assignment(canonical)
    assignment = None
    summary = None
    if assignment_server_id:
        try:
            summary = _build_server_summary(assignment_server_id, skip_cache=False)
            assignment = {
                "server_id": assignment_server_id,
                "server_name": (summary.get("name") or "").strip() or assignment_server_id,
            }
        except Exception:
            assignment = {"server_id": assignment_server_id, "server_name": assignment_server_id}
    server_display = None
    # When kiosk is disabled, always show race setup when assigned; when enabled, only when not paired
    kiosk_enabled = getattr(get_config(), "kiosk_mode_enabled", False)
    if assignment and summary and (not paired or not kiosk_enabled):
        try:
            track_safe = _sanitize_track_for_response(summary.get("track"))
            car_availability = _car_availability_for_server(assignment_server_id, summary, cache)
            server_display = {
                "agent_id": canonical,
                "server_id": summary["server_id"],
                "track": track_safe,
                "mode": summary["mode"],
                "cars": summary["cars"],
                "cars_with_skins": summary.get("cars_with_skins", []),
                "slots": summary.get("slots", []),
                "updated_at": summary.get("updated_at", ""),
                "car_availability": car_availability,
            }
        except HTTPException:
            pass
    payload = {
        "agent_id": canonical,
        "paired": paired,
        "assigned": assigned,
        "detected": detected,
        "process": process,
        "assignment": assignment,
        "qr": qr,
        "server_display": server_display,
        "kiosk_enabled": kiosk_enabled,
    }
    if agent_status:
        if getattr(agent_status, "race_results", None) and isinstance(agent_status.race_results, list):
            payload["race_results"] = agent_status.race_results
        if getattr(agent_status, "race_track_name", None) and isinstance(agent_status.race_track_name, str):
            payload["race_track_name"] = agent_status.race_track_name
    # Signal frontend to switch to results screen: race finished and sim has backed out (one-time per race)
    has_results = bool(payload.get("race_results") and len(payload["race_results"]) > 0)
    acs_running = (payload.get("process") or {}).get("acs_running") is True
    navigate = False
    if has_results and not acs_running:
        try:
            sig = hashlib.md5(
                json.dumps(payload["race_results"], sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            if _sim_results_navigated.get(canonical) != sig:
                _sim_results_navigated[canonical] = sig
                navigate = True
        except Exception:
            navigate = True
    payload["navigate_to_results"] = navigate
    return JSONResponse(
        content=payload,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


@router.get("/sims/{agent_id}/presets")
async def get_sim_presets(agent_id: str):
    """Steering and shifting preset lists for this sim (kiosk UI — same source as sim cards)."""
    canonical = _canonical_agent_id(agent_id)
    if not canonical:
        raise HTTPException(status_code=404, detail="Unknown agent_id")
    cache = get_status_cache()
    agent_status = cache.get(canonical)
    steering = list(getattr(agent_status, "steering_presets", None) or []) if agent_status else []
    shifting = list(getattr(agent_status, "shifting_presets", None) or []) if agent_status else []
    return JSONResponse(
        content={"agent_id": canonical, "steering_presets": steering, "shifting_presets": shifting},
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


class KioskCommandEnvelope(BaseModel):
    """Unified kiosk envelope (schema_version, source.session_id, target.agent_id, intent, selection)."""
    schema_version: Optional[int] = 1
    request_id: Optional[str] = None
    timestamp_utc: Optional[str] = None
    source: Optional[dict] = None  # { type: "kiosk", session_id: "..." }
    target: Optional[dict] = None  # { agent_id: "Sim5" }
    intent: Optional[dict] = None  # { action: "apply_and_launch" | "preview_only" | "apply_only" | "stop_session" }
    selection: Optional[dict] = None


@router.post("/kiosk/command")
async def kiosk_command(envelope: KioskCommandEnvelope):
    """Accept unified payload from phone; validate kiosk session; forward to agent POST /apply; return agent response and update sim display state."""
    session_id = (envelope.source or {}).get("session_id") or ""
    if not session_id:
        raise HTTPException(status_code=401, detail="Missing source.session_id")
    session = get_session(session_id)
    if not session:
        raise HTTPException(status_code=401, detail="Invalid or expired kiosk session")
    agent_id = session.agent_id
    # Optional: ensure target.agent_id matches session
    target_agent = (envelope.target or {}).get("agent_id") or ""
    if target_agent and target_agent.strip().lower() != agent_id.lower():
        raise HTTPException(status_code=403, detail="Session does not match target agent")
    # Build payload and enrich server.join_addr + server_cfg_snapshot for online so agent can sync race.ini
    payload = envelope.model_dump(exclude_none=True)
    selection = payload.get("selection") or {}
    mode = selection.get("mode") or {}
    if (mode.get("kind") or "").strip().lower() == "online":
        server = selection.get("server") or {}
        server_id = (server.get("server_id") or "").strip()
        if server_id:
            resolved_host, resolved_port, _ = _resolve_server_address(server_id)
            enrich = dict(selection.get("server") or {})
            if resolved_host is not None and resolved_port is not None:
                enrich["join_addr"] = f"{resolved_host}:{resolved_port}"
            sc_data, _ = _read_full_server_cfg(server_id)
            if not sc_data:
                raise HTTPException(
                    status_code=400,
                    detail=f"Preset server_cfg.ini not found or unreadable for server_id={server_id!r}. Required for online join.",
                )
            enrich["server_cfg_snapshot"] = sc_data
            enrich["preset_name"] = server_id
            pwd = get_config().global_server_password
            enrich["global_server_password"] = (pwd if pwd is not None else None)
            payload.setdefault("selection", {})["server"] = enrich
    # Forward to agent
    result = await send_agent_command(agent_id, "apply", payload)
    # Update kiosk display state from agent response
    status = result.get("status") or "idle"
    set_kiosk_agent_state(
        agent_id,
        status=status,
        selection=envelope.selection,
        applied=result.get("applied"),
        warnings=result.get("warnings") or [],
        errors=result.get("errors") or [],
    )
    if (envelope.intent or {}).get("action") == "stop_session":
        invalidate_session(session_id)
    return result


# ---- Catalogs (cached) for kiosk UI ----

def _catalogs_dir() -> Path:
    """Directory for catalog JSON files (same parent as controller config)."""
    try:
        cp = get_config_path()
        if cp:
            return Path(cp).resolve().parent / "catalogs"
    except Exception:
        pass
    return Path(os.getcwd()) / "catalogs"


def _load_catalog_json(name: str) -> Optional[dict]:
    """Load optional catalog JSON (e.g. assists.json, servers.json). Returns None if missing."""
    path = _catalogs_dir() / f"{name}.json"
    if not path.is_file():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.warning("Could not load catalog %s: %s", path, e)
        return None


def _build_catalog_cars() -> list[dict]:
    """Build Car list for catalog from content/cars (cached)."""
    raw = _list_cars()
    out = []
    for c in raw:
        car_id = (c.get("car_id") or "").strip()
        if not car_id or ".." in car_id:
            continue
        cls = (c.get("class") or "").strip()
        class_ids = [cls] if cls else []
        skins = []
        for s in (c.get("skins") or []):
            skin_id = (s.get("folder") or s.get("skin_id") or "").strip()
            skins.append({"skin_id": skin_id or "default", "name": s.get("name") or skin_id, "preview": ""})
        if not skins:
            skins = [{"skin_id": "default", "name": "Default", "preview": ""}]
        preview = (c.get("preview_url") or "").strip()
        images = {"card": preview, "hero": preview} if preview else {}
        out.append({
            "car_id": car_id,
            "name": c.get("name") or car_id,
            "brand": cls or "",
            "class_ids": class_ids,
            "skins": skins,
            "images": images,
            "tags": [],
            "disabled": False,
        })
    return out


def _build_catalog_tracks() -> list[dict]:
    """Build Track list with layouts from content/tracks (cached)."""
    tracks_dir = _tracks_dir()
    if not tracks_dir.is_dir():
        return []
    out = []
    for track_path in sorted(tracks_dir.iterdir()):
        if not track_path.is_dir():
            continue
        track_id = track_path.name
        if ".." in track_id or not track_id or track_id == STFOLDER_NAME:
            continue
        ui_dir = track_path / "ui"
        layouts = []
        if ui_dir.is_dir():
            for layout_path in sorted(ui_dir.iterdir()):
                if not layout_path.is_dir():
                    continue
                layout_id = layout_path.name
                if ".." in layout_id or layout_id == STFOLDER_NAME:
                    continue
                name = _get_track_display_name(track_id, layout_id) or layout_id or "default"
                layouts.append({
                    "layout_id": layout_id,
                    "name": name,
                    "ac_track": track_id,
                    "ac_config": layout_id,
                    "images": {"hero": "", "map": ""},
                })
        if not layouts:
            layouts = [{
                "layout_id": "default",
                "name": _get_track_display_name(track_id, None) or track_id,
                "ac_track": track_id,
                "ac_config": "default",
                "images": {"hero": "", "map": ""},
            }]
        first_layout = (layouts[0].get("layout_id") or "default") if layouts else "default"
        ui_info = _get_track_ui_info(track_id, first_layout)
        country = (ui_info.get("country") or "").strip() or "Unknown"
        categories = list(ui_info.get("categories") or [])
        out.append({
            "track_id": track_id,
            "name": _get_track_display_name(track_id, None) or track_id,
            "layouts": layouts,
            "country": country,
            "categories": categories,
            "disabled": False,
        })
    return out


def _build_catalog_assists() -> list[dict]:
    """Build assists preset list from catalog file or default."""
    data = _load_catalog_json("assists")
    if data and isinstance(data.get("presets"), list):
        return list(data["presets"])
    return [
        {"assist_preset_id": "race", "name": "Race", "apply_method": "agent_files", "agent_refs": {"savedsetup_file": "Race.ini"}, "policy": {"allow_custom": False}},
        {"assist_preset_id": "drift", "name": "Drift", "apply_method": "agent_files", "agent_refs": {"savedsetup_file": "Drift.ini"}, "policy": {"allow_custom": False}},
        {"assist_preset_id": "kids", "name": "Kids Mode", "apply_method": "agent_files", "agent_refs": {"savedsetup_file": "Kids.ini"}, "policy": {"allow_custom": False, "limits": {"abs": "allowed", "tc": "allowed"}}},
    ]


def _build_catalog_servers() -> list[dict]:
    """Build Server list from presets + Content Manager favourites + optional servers.json overlay."""
    overlay = _load_catalog_json("servers")
    preset_list = list_server_preset_ids() if not PRESETS_DIR_DEBUG.is_dir() else discover_presets(PRESETS_DIR_DEBUG)
    if not preset_list:
        preset_list = ["SERVER_01"]
    overlay_servers = (overlay or {}).get("servers") if isinstance(overlay, dict) else None
    if isinstance(overlay_servers, dict):
        preset_list = sorted(set(preset_list) | set(overlay_servers.keys()))
    result = []
    preset_addrs: set[tuple[str, int]] = set()
    for server_id in preset_list:
        try:
            summary = _build_server_summary(server_id)
        except Exception:
            summary = {"server_id": server_id, "name": server_id, "track": {}, "cars": []}
        track = summary.get("track") or {}
        addr = {"host": "", "port": 9600}
        if overlay and isinstance(overlay.get("servers"), dict) and server_id in overlay["servers"]:
            o = overlay["servers"][server_id]
            if isinstance(o.get("address"), dict):
                addr = {**addr, **o["address"]}
        preset_dir = _get_server_preset_dir_safe(server_id)
        sc_path, _ = _preset_ini_paths(preset_dir)
        if sc_path.exists():
            try:
                server_ini = read_ini(sc_path)
                server_opts, _ = _parse_server_section(server_ini)
                if server_opts:
                    addr["host"] = (server_opts.get("LISTEN_IP") or server_opts.get("IP") or addr.get("host") or "").strip()
                    if not addr["host"]:
                        addr["host"] = _get_server_join_host()
                    tcp_s = (server_opts.get("TCP_PORT") or "").strip()
                    udp_s = (server_opts.get("UDP_PORT") or "").strip()
                    port_s = tcp_s or udp_s
                    if port_s.isdigit():
                        addr["port"] = int(port_s)
                    if addr.get("host") and addr.get("port"):
                        preset_addrs.add((str(addr["host"]), int(addr["port"])))
            except Exception:
                pass
        cars = summary.get("cars") or []
        o = (overlay or {}).get("servers") or {} if isinstance(overlay, dict) else {}
        o2 = o.get(server_id) if isinstance(o, dict) else None
        caps = (o2.get("capabilities") or {}) if isinstance(o2, dict) else {}
        constraints = (o2.get("constraints") or {}) if isinstance(o2, dict) else {}
        if not constraints and cars:
            constraints = {"allowed_car_ids": cars}
        result.append({
            "server_id": server_id,
            "name": (summary.get("name") or "").strip() or server_id,
            "address": addr,
            "capabilities": {"join_method": "aclink", "car_selection": "best_effort", "supports_spectator": True, **caps},
            "constraints": constraints,
            "tags": list((o2.get("tags") or [])) if isinstance(o2, dict) else [],
            "disabled": bool(o2.get("disabled")) if isinstance(o2, dict) else False,
            "source": "preset",
        })
    for f in load_favourites_servers():
        addr_key = (f.get("ip") or "", int(f.get("port") or 0))
        if addr_key in preset_addrs:
            continue
        result.append({
            "server_id": f["server_id"],
            "name": (f.get("name") or "").strip() or f["server_id"],
            "address": {"host": f.get("ip") or "", "port": f.get("port") or 9600},
            "capabilities": {"join_method": "aclink", "car_selection": "best_effort", "supports_spectator": True},
            "constraints": {},
            "tags": ["favorite"],
            "disabled": False,
            "source": "favorite",
        })
    return result


@router.get("/catalogs/cars")
async def get_catalog_cars():
    """
    Cached catalog from installed content (``content/cars``).

    **Returns** ``{ "cars": [ ... ] }``. Each car: ``car_id``, ``name``, ``brand``, ``class_ids``,
    ``skins`` (``skin_id``, ``name``, ``preview``), ``images`` (card/hero URLs), ``tags``, ``disabled``.

    **Classification:** mostly **harmless public content** for pickers (reveals which cars are installed
    on the controller host — a mild content-inventory footprint). Intentionally public for kiosk / LAN UI.
    """
    global _catalog_cars_cache
    now = time.time()
    if _catalog_cars_cache and (now - _catalog_cars_cache[0]) < _CATALOG_CACHE_TTL:
        return {"cars": _catalog_cars_cache[1]}
    cars = _build_catalog_cars()
    _catalog_cars_cache = (now, cars)
    return {"cars": cars}


@router.get("/catalogs/tracks")
async def get_catalog_tracks():
    """
    Cached catalog from installed content (``content/tracks``).

    **Returns** ``{ "tracks": [ ... ] }``. Each track: ``track_id``, ``name``, ``layouts``
    (``layout_id``, ``name``, ``ac_track``, ``ac_config``, ``images``), ``country``, ``categories``, ``disabled``.

    **Classification:** **harmless public content** for pickers (reveals installed track mods). Public for kiosk / LAN UI.
    """
    global _catalog_tracks_cache
    now = time.time()
    if _catalog_tracks_cache and (now - _catalog_tracks_cache[0]) < _CATALOG_CACHE_TTL:
        return {"tracks": _catalog_tracks_cache[1]}
    tracks = _build_catalog_tracks()
    _catalog_tracks_cache = (now, tracks)
    return {"tracks": tracks}


@router.get("/catalogs/assists")
async def get_catalog_assists(_: None = Depends(require_operator_if_password_configured)):
    """
    Assist presets from ``catalogs/assists.json`` or built-in defaults.

    **Returns** ``{ "assists": [ ... ] }``. Each preset: ``assist_preset_id``, ``name``,
    ``apply_method``, ``agent_refs`` (e.g. ``savedsetup_file`` → ``Race.ini``), ``policy`` (limits, etc.).

    **Classification:** **configuration-derived** (file names and application policy). Gated when
    ``employee_password`` is set. Kiosk tolerates 401 and falls back to default assist options in UI.
    """
    global _catalog_assists_cache
    now = time.time()
    if _catalog_assists_cache and (now - _catalog_assists_cache[0]) < _CATALOG_CACHE_TTL:
        return {"assists": _catalog_assists_cache[1]}
    assists = _build_catalog_assists()
    _catalog_assists_cache = (now, assists)
    return {"assists": assists}


@router.get("/catalogs/servers")
async def get_catalog_servers(_: None = Depends(require_operator_if_password_configured)):
    """
    Server join catalog: preset IDs, ``server_cfg.ini`` listen addresses, Content Manager favourites,
    plus optional ``catalogs/servers.json`` overlay (capabilities, constraints, tags).

    **Returns** ``{ "servers": [ ... ] }``. Each entry: ``server_id``, ``name``, ``address`` (``host``, ``port``
    from INI or overlay), ``capabilities``, ``constraints`` (e.g. ``allowed_car_ids``), ``tags``, ``disabled``, ``source``.

    **Classification:** **operational metadata** and **configuration-derived** (join targets, car allowlists).
    Gated when ``employee_password`` is set. Kiosk step 2 uses ``GET /servers`` for cards; catalog list may be empty when unauthenticated.
    """
    global _catalog_servers_cache
    now = time.time()
    if _catalog_servers_cache and (now - _catalog_servers_cache[0]) < _CATALOG_CACHE_TTL:
        return {"servers": _catalog_servers_cache[1]}
    servers = _build_catalog_servers()
    _catalog_servers_cache = (now, servers)
    return {"servers": servers}


def _cars_for_server(server_id: str) -> list[str]:
    """
    Return allowed car ids for a server: from entry_list.ini unique MODEL if present;
    else from server_cfg.ini CARS=; else [].
    Uses Option B path layout: primary preset root, fallback preset/cfg/.
    """
    preset_dir = _get_server_preset_dir_safe(server_id)
    sc_path, el_path = _preset_ini_paths(preset_dir)
    entry_ini: dict = {}
    server_ini: dict = {}
    if el_path.exists():
        try:
            entry_ini = read_ini(el_path)
        except Exception:
            pass
    if sc_path.exists():
        try:
            server_ini = read_ini(sc_path)
        except Exception:
            pass
    # From entry_list [CAR_N] MODEL
    car_sections = [s for s in entry_ini if s.upper().startswith("CAR_")]
    if car_sections:
        seen: set[str] = set()
        out: list[str] = []
        for sect in sorted(car_sections, key=lambda s: (len(s), s)):
            opts = entry_ini[sect]
            model = (opts.get("MODEL") or "").strip()
            if model and model not in seen:
                seen.add(model)
                out.append(model)
        if out:
            return out
    # Fallback: server_cfg SERVER CARS=
    for sect, opts in server_ini.items():
        if sect.upper() == "SERVER":
            cars_raw = (opts.get("CARS") or "").strip()
            return [c.strip() for c in cars_raw.split(";") if c.strip()]
    return []


@router.get("/servers")
async def list_servers(_: None = Depends(require_operator_if_password_configured)):
    """Return list of server summaries for all merged server ids (presets + CM favourites). One entry per id with id, name, ip, port, join_addr, source."""
    server_ids = get_merged_server_ids()
    result: list[dict[str, Any]] = []
    for sid in server_ids:
        fav = _get_favourite_by_id(sid)
        if fav:
            ip_str = (fav.get("ip") or "").strip()
            port_val = int(fav.get("port") or 0)
            join_addr = f"{ip_str}:{port_val}" if ip_str else ""
            item = {
                "id": fav["server_id"],
                "server_id": fav["server_id"],
                "name": (fav.get("name") or "").strip() or fav["server_id"],
                "ip": ip_str or None,
                "port": port_val,
                "trackLabel": "—",
                "playerCount": None,
                "tcp_port": port_val,
                "udp_port": port_val,
                "http_port": None,
                "join_addr": join_addr,
                "source": "favorite",
            }
            result.append(item)
            continue
        try:
            summary = _build_server_summary(sid)
            track = summary.get("track") or {}
            track_label = (track.get("name") or "").strip() or (track.get("id") or "") + ("/" + (track.get("config") or "default") if track.get("config") else "")
            name = (summary.get("name") or "").strip() or sid
            item = {
                "id": sid,
                "server_id": sid,
                "name": name,
                "trackLabel": track_label or "—",
                "playerCount": None,
                "source": "preset",
            }
            try:
                preset_dir = _get_server_preset_dir_safe(sid)
                sc_path, _ = _preset_ini_paths(preset_dir)
                parsed = parse_ac_server_cfg(sc_path) if sc_path.exists() else None
                if parsed:
                    tcp_port = parsed["tcp_port"]
                    udp_port = parsed["udp_port"]
                    http_port = parsed.get("http_port")
                    logger.info(
                        "[server-net] preset='%s' cfg_path='%s' tcp=%s udp=%s http=%s",
                        sid, sc_path, tcp_port, udp_port, http_port,
                    )
                    item["tcp_port"] = tcp_port
                    item["udp_port"] = udp_port
                    item["http_port"] = http_port
                    host = parsed.get("ip") or _get_server_join_host()
                    join_addr = f"{host}:{tcp_port}"
                    item["join_addr"] = join_addr
                    item["ip"] = (parsed.get("ip") or "").strip() or None
                    item["port"] = tcp_port
                    if not parsed.get("ip"):
                        logger.info("[server-addr] preset='%s' join_addr='%s'", sid, join_addr)
                else:
                    item["tcp_port"] = None
                    item["udp_port"] = None
                    item["http_port"] = None
                    item["join_addr"] = None
                    item["ip"] = None
                    item["port"] = None
            except Exception:
                item["tcp_port"] = None
                item["udp_port"] = None
                item["http_port"] = None
                item["join_addr"] = None
                item["ip"] = None
                item["port"] = None
            result.append(item)
        except HTTPException:
            result.append({
                "id": sid, "server_id": sid, "name": sid, "trackLabel": "—", "playerCount": None,
                "tcp_port": None, "udp_port": None, "http_port": None, "join_addr": None,
                "ip": None, "port": None, "source": "preset",
            })
        except Exception:
            result.append({
                "id": sid, "server_id": sid, "name": sid, "trackLabel": "—", "playerCount": None,
                "tcp_port": None, "udp_port": None, "http_port": None, "join_addr": None,
                "ip": None, "port": None, "source": "preset",
            })
    return result


@router.get("/servers/presets-info")
async def get_presets_info(_: None = Depends(require_operator)):
    """Return the presets path and merged server list (presets + CM favourites) the controller is using.
    Registered before /servers/{server_id} so 'presets-info' is not captured as a server_id."""
    presets_root = _get_presets_root()
    server_ids = get_merged_server_ids()
    return {
        "presets_root": str(presets_root),
        "presets_root_exists": presets_root.is_dir(),
        "server_ids": server_ids,
        "favourites_debug": get_favourites_debug_info(),
    }


@router.get("/servers/{server_id}")
async def get_server_details(server_id: str, _: None = Depends(require_operator_if_password_configured)):
    """Return server details including cars list for online join. Handles preset (from disk) and CM favourite."""
    if not _valid_server_id(server_id):
        raise HTTPException(status_code=400, detail="Invalid server_id")
    fav = _get_favourite_by_id(server_id)
    if fav:
        port = int(fav.get("port") or 0)
        host = (fav.get("ip") or "").strip()
        join_addr = f"{host}:{port}" if host else None
        live = await asyncio.to_thread(get_live_server_info, host or "", port)
        cars_list = list(live.get("cars") or [])
        track_id = (live.get("track") or {}).get("id") or live.get("track_id") or None
        track_config = (live.get("track") or {}).get("config") or live.get("layout") or None
        cars = [{"id": c, "displayName": c, "thumbnailUrl": f"/api/cars/{c}/preview", "isTaken": None} for c in cars_list]
        return {
            "id": fav["server_id"],
            "name": (fav.get("name") or live.get("name") or "").strip() or fav["server_id"],
            "ip": host or None,
            "port": port,
            "tcp_port": port,
            "udp_port": port,
            "http_port": None,
            "join_addr": join_addr,
            "joinDisplayText": join_addr,
            "trackId": track_id,
            "trackConfig": track_config,
            "cars": cars,
            "source": "favorite",
        }
    try:
        summary = _build_server_summary(server_id)
    except HTTPException:
        raise
    summary = dict(summary)
    track = summary.get("track") or {}
    track_id = (track.get("id") or "").strip()
    track_config = (track.get("config") or "").strip() or None
    preset_dir = _get_server_preset_dir_safe(server_id)
    sc_path, _ = _preset_ini_paths(preset_dir)
    tcp_port = None
    udp_port = None
    http_port = None
    join_addr = None
    join_display = None
    ip = None
    if sc_path.exists():
        parsed = parse_ac_server_cfg(sc_path)
        if parsed:
            tcp_port = parsed["tcp_port"]
            udp_port = parsed["udp_port"]
            http_port = parsed.get("http_port")
            logger.info(
                "[server-net] preset='%s' cfg_path='%s' tcp=%s udp=%s http=%s",
                server_id, sc_path, tcp_port, udp_port, http_port,
            )
            host = parsed.get("ip") or _get_server_join_host()
            join_addr = f"{host}:{tcp_port}"
            if parsed.get("ip"):
                join_display = join_addr
            else:
                join_display = f"Admin PC (LAN IP) : {tcp_port}"
                logger.info("[server-addr] preset='%s' join_addr='%s'", server_id, join_addr)
            ip = parsed.get("ip")
    car_ids = _cars_for_server(server_id)
    cars = [
        {"id": cid, "displayName": cid, "thumbnailUrl": f"/api/cars/{cid}/preview", "isTaken": None}
        for cid in car_ids
    ]
    return {
        "id": server_id,
        "name": (summary.get("name") or "").strip() or server_id,
        "ip": ip,
        "port": tcp_port,
        "tcp_port": tcp_port,
        "udp_port": udp_port,
        "http_port": http_port,
        "join_addr": join_addr,
        "joinDisplayText": join_display,
        "trackId": track_id or None,
        "trackConfig": track_config,
        "cars": cars,
        "source": "preset",
    }


@router.get("/servers/{server_id}/current_config")
async def get_server_current_config(server_id: str, _: None = Depends(require_operator_if_password_configured)):
    """
    Return current TRACK and CONFIG_TRACK from the preset's server_cfg.ini (read from disk).
    Used by the track picker modal to pre-select the server's current track/layout.
    """
    cfg_dir = _cfg_dir_for_server(server_id)
    sc_path, _ = _server_config_paths_for_read(cfg_dir)
    server_cfg_raw = read_ini(sc_path) if sc_path.exists() else {}
    server_cfg = {}
    for sect, opts in server_cfg_raw.items():
        s_key = sect.upper()
        if s_key not in server_cfg:
            server_cfg[s_key] = {k.upper(): v for k, v in opts.items()}
    srv = server_cfg.get("SERVER", {})
    track_raw = (srv.get("TRACK") or "").strip()
    layout_raw = (srv.get("CONFIG_TRACK") or "").strip()
    return {
        "preset_id": server_id,
        "track": track_raw,
        "layout": layout_raw,
    }


def _compute_preset_disk_state(preset_id: str) -> dict[str, Any]:
    """
    Read server_cfg.ini and entry_list.ini from disk for the given preset (source of truth).
    For CM favourite preset_id (ip:port), returns minimal state with server_join and empty cars.
    """
    fav = _get_favourite_by_id(preset_id)
    if fav:
        host = (fav.get("ip") or "").strip() or "127.0.0.1"
        seed_port = int(fav.get("port") or 0)
        live = get_live_server_info(host, seed_port)
        game_port = live.get("game_port") if live.get("game_port") is not None else seed_port
        cars_list = list(live.get("cars") or [])
        track = live.get("track") or {}
        track_id = _normalize_track_id_from_preset((track.get("id") or live.get("track_id") or "").strip()) or (track.get("id") or live.get("track_id") or "").strip()
        layout_id = (track.get("config") or live.get("layout") or "").strip()
        entry_list_cars = [{"slot": i + 1, "MODEL": c, "SKIN": "", "GUID": ""} for i, c in enumerate(cars_list)]
        cars_source = "live" if cars_list else "none"
        server_section: dict[str, str] = {
            "NAME": (fav.get("name") or live.get("name") or "").strip() or preset_id,
            "TCP_PORT": str(game_port),
            "UDP_PORT": str(game_port),
            "IP": host,
            "CARS": ";".join(cars_list),
            "TRACK": track_id,
        }
        if live.get("http_port") is not None:
            server_section["HTTP_PORT"] = str(live["http_port"])
        if layout_id:
            server_section["CONFIG_TRACK"] = layout_id
        return {
            "preset_id": preset_id,
            "paths": {"server_cfg_ini": "", "entry_list_ini": ""},
            "server_cfg": {"SERVER": server_section},
            "track": {"track_id": track_id or "", "layout_id": layout_id or ""},
            "cars": cars_list,
            "cars_source": cars_source,
            "entry_list": {"cars": entry_list_cars},
            "server_join": {"host": host, "port": game_port},
            "source": "favorite",
        }
    preset_dir = _get_server_preset_dir_safe(preset_id)
    sc_path, el_path = _preset_ini_paths(preset_dir)

    paths = {
        "server_cfg_ini": str(sc_path),
        "entry_list_ini": str(el_path),
    }

    server_cfg: dict[str, dict[str, str]] = {}
    if sc_path.exists():
        try:
            raw = read_ini(sc_path)
            for sect, opts in raw.items():
                s_key = sect.upper()
                if s_key not in server_cfg:
                    server_cfg[s_key] = {str(k).upper(): (v or "").strip() for k, v in opts.items()}
        except Exception as e:
            logger.warning("[disk_state] Failed to read server_cfg.ini at %s: %s", sc_path, e)
    srv = server_cfg.get("SERVER", {})
    track_id_raw = (srv.get("TRACK") or "").strip()
    layout_id_raw = (srv.get("CONFIG_TRACK") or "").strip()
    track_id_safe = _safe_track_id(_normalize_track_id_from_preset(track_id_raw)) if track_id_raw else ""
    layout_id_safe = _safe_config(layout_id_raw) if layout_id_raw else ""

    entry_list_cars: list[dict[str, Any]] = []
    if el_path.exists():
        try:
            entry_ini = read_ini(el_path)
            car_sections = [s for s in entry_ini if s.upper().startswith("CAR_")]
            for sect in sorted(car_sections, key=lambda s: (len(s), s)):
                opts = entry_ini[sect]
                try:
                    slot_num = int(sect.replace("CAR_", "").strip()) if "CAR_" in sect.upper() else len(entry_list_cars)
                except ValueError:
                    slot_num = len(entry_list_cars)
                model = (opts.get("MODEL") or "").strip()
                skin = (opts.get("SKIN") or "").strip()
                guid = (opts.get("GUID") or "").strip()
                entry_list_cars.append({"slot": slot_num, "MODEL": model, "SKIN": skin, "GUID": guid})
        except Exception as e:
            logger.warning("[disk_state] Failed to read entry_list.ini at %s: %s", el_path, e)
    else:
        logger.info("[disk_state] entry_list.ini missing for preset_id=%s path=%s", preset_id, el_path)

    # Top-level cars: string[] and cars_source for UI (entry_list.ini first, else server_cfg.ini CARS=)
    entry_models: list[str] = []
    seen_models: set[str] = set()
    for c in entry_list_cars:
        m = (c.get("MODEL") or "").strip()
        if m and m not in seen_models:
            seen_models.add(m)
            entry_models.append(m)
    cars_raw = (srv.get("CARS") or "").strip()
    cars_from_cfg: list[str] = [x.strip() for x in cars_raw.split(";") if x.strip()]
    if entry_models:
        cars_top: list[str] = entry_models
        cars_source = "entry_list.ini"
    elif cars_from_cfg:
        cars_top = cars_from_cfg
        cars_source = "server_cfg.ini:CARS"
    else:
        cars_top = []
        cars_source = "none"

    port: Optional[int] = None
    if srv.get("TCP_PORT") and str(srv.get("TCP_PORT", "")).strip().isdigit():
        port = int(str(srv["TCP_PORT"]).strip())
    elif srv.get("UDP_PORT") and str(srv.get("UDP_PORT", "")).strip().isdigit():
        port = int(str(srv["UDP_PORT"]).strip())
    host = (srv.get("LISTEN_IP") or srv.get("IP") or "").strip() or _get_server_join_host()

    return {
        "preset_id": preset_id,
        "paths": paths,
        "server_cfg": server_cfg,
        "track": {"track_id": track_id_safe or "", "layout_id": layout_id_safe or "default"},
        "cars": cars_top,
        "cars_source": cars_source,
        "entry_list": {"cars": entry_list_cars},
        "server_join": {"host": host or "127.0.0.1", "port": port},
    }


def _disk_state_payload_for_id(preset_id: str) -> Optional[dict[str, Any]]:
    """Resolve disk_state for one preset using TTL cache; None if preset_id invalid."""
    if not _valid_server_id(preset_id):
        return None
    hit = _get_cached_preset_disk_state(preset_id)
    if hit is not None:
        return copy.deepcopy(hit)
    payload = _compute_preset_disk_state(preset_id)
    _set_cached_preset_disk_state(preset_id, payload)
    return copy.deepcopy(payload)


_PRESETS_DISK_STATE_BATCH_MAX = 64


@router.get("/preset/{preset_id}/disk_state")
async def get_preset_disk_state(preset_id: str, _: None = Depends(require_operator_if_password_configured)):
    """Same as _compute_preset_disk_state with short TTL cache."""
    out = await asyncio.to_thread(_disk_state_payload_for_id, preset_id)
    if out is None:
        raise HTTPException(status_code=400, detail="Invalid preset_id")
    return out


@router.get("/presets/disk_state")
async def get_presets_disk_state(ids: str = "", _: None = Depends(require_operator_if_password_configured)):
    """
    Batch disk_state for multiple presets (comma-separated ids).
    Reuses the same 5s per-preset cache as GET /preset/{id}/disk_state.
    Runs all preset lookups concurrently so a slow favourite-server live-fetch
    on one entry does not delay the others.
    """
    raw = [x.strip() for x in (ids or "").split(",") if x.strip()]
    seen: set[str] = set()
    ordered: list[str] = []
    for x in raw:
        if x in seen:
            continue
        seen.add(x)
        ordered.append(x)
    if len(ordered) > _PRESETS_DISK_STATE_BATCH_MAX:
        ordered = ordered[:_PRESETS_DISK_STATE_BATCH_MAX]
    payloads = await asyncio.gather(
        *[asyncio.to_thread(_disk_state_payload_for_id, pid) for pid in ordered]
    )
    results: dict[str, dict[str, Any]] = {
        pid: payload
        for pid, payload in zip(ordered, payloads)
        if payload is not None
    }
    return {"results": results}


@router.get("/debug/presets")
async def get_debug_presets(_: None = Depends(require_operator)):
    """Debug: presets_dir (hardcoded), exists, discovered list, count, sample paths."""
    presets_dir = PRESETS_DIR_DEBUG.resolve()
    exists = presets_dir.is_dir()
    discovered = discover_presets(presets_dir) if exists else []
    sample = {sid: str(presets_dir / sid) for sid in discovered}
    return {
        "presets_dir": str(presets_dir),
        "exists": exists,
        "discovered": discovered,
        "count": len(discovered),
        "sample": sample,
    }


@router.get("/debug/favourites")
async def get_debug_favourites(_: None = Depends(require_operator)):
    """Debug: resolved Favourites.txt path, exists, parsed count, first 10 entries, cache status."""
    favourites = load_favourites_servers()
    info = get_favourites_debug_info()
    info["count"] = len(favourites)
    info["first_10"] = favourites[:10]
    return info


# --- PitBox online servers (add by IP:port; no lobby) ---

@router.get("/servers/{server_id}/summary")
async def get_server_summary(server_id: str, _: None = Depends(require_operator_if_password_configured)):
    """Return server preset summary (track, cars, slots). For online (favourite) uses live server metadata when available."""
    fav = _get_favourite_by_id(server_id)
    if fav:
        host = (fav.get("ip") or "").strip()
        port = int(fav.get("port") or 0)
        live = await asyncio.to_thread(get_live_server_info, host, port)
        cars_list = list(live.get("cars") or [])
        track = live.get("track") or {}
        if not track and (live.get("track_id") or live.get("layout")):
            track = {"id": live.get("track_id") or "", "config": (live.get("layout") or "").strip(), "name": live.get("track_id") or ""}
        return {
            "server_id": server_id,
            "name": (fav.get("name") or live.get("name") or "").strip() or server_id,
            "track": track,
            "cars": cars_list,
            "source": "favorite",
        }
    summary = _build_server_summary(server_id)
    summary = dict(summary)
    summary["track"] = _sanitize_track_for_response(summary.get("track"))
    return summary


@router.get("/sims/{agent_id}/server-display")
async def get_sim_server_display(agent_id: str, _: None = Depends(require_operator_if_password_configured)):
    """One-call endpoint for sim display: assignment + server summary. 404 if no assignment."""
    canonical, server_id = _resolve_assignment(agent_id)
    if not server_id:
        logger.info("[assignment-miss] agent_id=%s using_fallback=none (404)", agent_id)
        raise HTTPException(
            status_code=404,
            detail="No server assigned",
        )
    preset_dir = _get_server_preset_dir_safe(server_id)
    summary = _build_server_summary(server_id, skip_cache=False)
    track_safe = _sanitize_track_for_response(summary.get("track"))
    returned_server = summary["server_id"]
    logger.info(
        "[server-display] agent_id=%s assigned=%s returned_server=%s track_id=%s config=%s",
        canonical or agent_id, server_id, returned_server,
        track_safe.get("id") or "", track_safe.get("config") or "",
    )
    body = {
        "agent_id": canonical or agent_id,
        "server_id": returned_server,
        "track": track_safe,
        "mode": summary["mode"],
        "cars": summary["cars"],
        "cars_with_skins": summary.get("cars_with_skins", []),
        "slots": summary.get("slots", []),
        "updated_at": summary["updated_at"],
        "preset_path": str(preset_dir),
    }
    return JSONResponse(
        content=body,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


@router.get("/version")
async def get_version():
    """Return current PitBox version (single source of truth)."""
    from pitbox_common.version import __version__
    return {"version": __version__}


@router.get("/update/status")
async def get_update_status_route(refresh: bool = False, _: None = Depends(require_operator_if_password_configured)):
    """
    Read C:\\PitBox\\updates\\status.json from external updater.
    If missing or state is idle: return idle + release info (current_version, update_available, etc.).
    If updater is running: return state, message, percent. Never cached.
    Use ?refresh=true to bypass cache and fetch latest release from GitHub.
    """
    if refresh:
        clear_update_cache()
    updater = get_updater_status()
    release = get_update_status()
    out = {"state": "idle", "message": "", "percent": 0}
    out.update(release)
    # Always merge updater status (state, message, percent) so release info is not lost
    # when an update is downloading/done/error.
    if updater.get("state") and updater["state"] != "idle":
        out.update(updater)
    return out


class UpdateApplyBody(BaseModel):
    """Request body for update apply (optional). No body or {} defaults to controller."""
    target: str = "controller"


@router.post("/update/apply")
async def post_update_apply(request: Request, _: None = Depends(require_operator)):
    """
    Spawn external updater (pitbox_updater.exe) with latest release ZIP URL. Return immediately.
    Accepts no body, empty JSON {}, or {"target": "controller"}. Default: apply controller update.
    """
    target = "controller"
    try:
        body_bytes = await request.body()
        if body_bytes and body_bytes.strip():
            data = json.loads(body_bytes)
            if isinstance(data, dict) and data.get("target"):
                target = data["target"]
    except (json.JSONDecodeError, TypeError):
        pass
    if target != "controller":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only target=controller supported")
    ok, msg = apply_controller_update()
    if not ok:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)
    return {"ok": True, "message": "Updater started"}


@router.post("/update/run-installer")
async def post_update_run_installer(_: None = Depends(require_operator)):
    """
    Run the unified installer update (update_pitbox.ps1 -Force). This is the preferred
    update method when PitBoxInstaller_*.exe is present in the release.
    """
    ok, msg = run_unified_installer_update()
    if not ok:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=msg)
    return {"ok": True, "message": msg}


@router.get("/config")
async def get_controller_config(_: None = Depends(require_operator)):
    """Return current controller config and path (tokens masked for display). Operator or localhost-only per require_operator."""
    config = get_config()
    path = get_config_path()
    # Export as dict with tokens masked for UI display
    data = config.model_dump()
    for agent in data.get("agents", []):
        if agent.get("token"):
            agent["token"] = "***"
    return {"config_path": path, "config": data}


class AddAgentBody(BaseModel):
    """Enrollment: add a discovered agent to controller config. Does not modify agent_config.json (see ENROLLMENT.md)."""
    id: str
    host: str
    port: int
    token: str

    @field_validator("id")
    @classmethod
    def id_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Agent ID cannot be empty")
        return v.strip()

    @field_validator("host")
    @classmethod
    def host_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Host cannot be empty")
        return v.strip()

    @field_validator("port")
    @classmethod
    def port_valid(cls, v: int) -> int:
        if v < 1 or v > 65535:
            raise ValueError("Port must be 1-65535")
        return v

    @field_validator("token")
    @classmethod
    def token_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Token cannot be empty")
        return v.strip()


@router.post("/config/agents")
async def add_agent_to_config(body: AddAgentBody, _: None = Depends(require_operator)):
    """Add or update an agent (upsert). Writes to enrolled_rigs.json (sim card store) and controller_config.json."""
    path = get_config_path()
    if not path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No controller config file loaded; create or open controller_config.json first",
        )
    config = get_config()
    existing_in_config = any(a.id == body.id for a in config.agents)
    updated = existing_in_config

    # Always upsert into enrolled_rigs.json (the live sim-card store)
    enrolled_add(body.id, body.host, body.port, hostname=None)

    # Sync controller_config.json: update existing entry or append new one
    if existing_in_config:
        new_agents = [
            AgentInfo(id=a.id, host=body.host if a.id == body.id else a.host,
                      port=body.port if a.id == body.id else a.port,
                      token=body.token if a.id == body.id else a.token)
            for a in config.agents
        ]
    else:
        new_agent = AgentInfo(id=body.id, host=body.host, port=body.port, token=body.token)
        new_agents = list(config.agents) + [new_agent]

    new_config = ControllerConfig(
        ui_host=config.ui_host,
        ui_port=config.ui_port,
        allow_lan_ui=config.allow_lan_ui,
        poll_interval_sec=config.poll_interval_sec,
        agents=new_agents,
        ac_server_root=config.ac_server_root,
        ac_server_presets_root=config.ac_server_presets_root,
        ac_server_cfg_path=config.ac_server_cfg_path,
        ac_servers=config.ac_servers,
        ac_content_root=config.ac_content_root,
        ac_cars_path=config.ac_cars_path,
        server_host=config.server_host,
        global_server_password=config.global_server_password,
        kiosk_secret=config.kiosk_secret,
        employee_password=config.employee_password,
        update_channel=config.update_channel,
    )
    save_config(Path(path), new_config)
    return {"ok": True, "agent_id": body.id, "updated": updated}


class ConfigPutBody(BaseModel):
    """Partial controller config update (merge with existing). Restricted to update_channel when from non-localhost."""
    ui_host: Optional[str] = None
    ui_port: Optional[int] = None
    allow_lan_ui: Optional[bool] = None
    ac_server_cfg_path: Optional[str] = None
    update_channel: Optional[UpdateChannelConfig] = None


def _is_localhost(request: Request) -> bool:
    """True if request is from localhost (127.0.0.1 or ::1)."""
    host = (request.client.host if request.client else "").strip()
    return host in ("127.0.0.1", "::1", "localhost")


@router.put("/config")
async def put_controller_config(request: Request, body: ConfigPutBody, _: None = Depends(require_operator)):
    """
    Merge partial config updates. Restricted to localhost for security.
    When from localhost: allows ui_host, ui_port, allow_lan_ui, ac_server_cfg_path, update_channel.
    Logs remote addr and changed keys.
    """
    if not _is_localhost(request):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Config updates allowed only from localhost",
        )
    path = get_config_path()
    if not path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No controller config file loaded",
        )
    config = get_config()
    updates = body.model_dump(exclude_unset=True)
    if not updates:
        return {"ok": True, "message": "No changes"}
    new_data = config.model_dump()
    for k, v in updates.items():
        if k in ("ui_host", "ui_port", "allow_lan_ui", "ac_server_cfg_path", "update_channel"):
            if v is not None or k in ("ac_server_cfg_path", "update_channel"):
                new_data[k] = v
    changed_keys = [k for k in updates if new_data.get(k) != config.model_dump().get(k)]
    logger.info(
        "PUT /config from %s: changed_keys=%s",
        request.client.host if request.client else "?",
        changed_keys,
    )
    new_config = ControllerConfig(**new_data)
    path_obj = Path(path)
    tmp = path_obj.with_suffix(path_obj.suffix + ".tmp")
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        json.dump(new_config.model_dump(), f, indent=2, ensure_ascii=False)
    try:
        tmp.replace(path_obj)
    except OSError as e:
        logger.exception("Config save failed: %s", e)
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Config save failed")
    load_config(path_obj)
    return {"ok": True, "message": "Config saved"}


# Per-sim timeout for batch start/stop: short so dispatch returns fast; agent responds immediately.
_BATCH_CONNECT_TIMEOUT_SEC = 2.0
_BATCH_READ_TIMEOUT_SEC = 6.0
_BATCH_START_STOP_TIMEOUT_SEC = _BATCH_CONNECT_TIMEOUT_SEC + _BATCH_READ_TIMEOUT_SEC  # for asyncio.wait_for


async def _send_start_or_stop_one(
    sim_id: str, action: str, body: dict, timeout_sec: float = _BATCH_START_STOP_TIMEOUT_SEC
) -> dict:
    """
    Send start or stop to one agent with short timeout. Each call uses its own HTTP connection.
    Agent returns 200 immediately (work done in background); this just waits for that response.
    Returns dict with success, message, pid (start), status ('ok'|'failed'), ms, error.
    """
    rig = enrolled_get(sim_id)
    host = (rig.get("host") or "").strip() if rig else ""
    port = int(rig.get("port") or 0) if rig else 0
    t0 = time.monotonic()
    logger.info("[batch] sim_id=%s host=%s port=%s action=%s start", sim_id, host, port, action)
    try:
        result = await asyncio.wait_for(
            send_agent_command(sim_id, action, body, timeout=_BATCH_READ_TIMEOUT_SEC),
            timeout=timeout_sec,
        )
    except asyncio.TimeoutError:
        ms = int((time.monotonic() - t0) * 1000)
        logger.warning("[batch] sim_id=%s action=%s end status=timeout ms=%s", sim_id, action, ms)
        return {
            "success": False,
            "message": "timeout",
            "status": "failed",
            "ms": ms,
            "error": "timeout",
        }
    except Exception as e:
        ms = int((time.monotonic() - t0) * 1000)
        err_msg = str(e)
        logger.warning("[batch] sim_id=%s action=%s end status=failed error=%s ms=%s", sim_id, action, err_msg, ms)
        return {
            "success": False,
            "message": err_msg,
            "status": "failed",
            "ms": ms,
            "error": err_msg,
        }
    ms = int((time.monotonic() - t0) * 1000)
    ok = bool(result.get("success"))
    logger.info(
        "[batch] sim_id=%s action=%s end status=%s ms=%s",
        sim_id, action, "ok" if ok else "failed", ms,
    )
    out = {
        **result,
        "status": "ok" if ok else "failed",
        "ms": ms,
        "error": result.get("message") if not ok else None,
    }
    return out


@router.post("/start", response_model=StartStopResponse)
async def start_sims(request: StartStopRequest, _: None = Depends(require_operator)):
    """Start one or more sims in parallel. Returns immediately with dispatch summary; progress via acs_running poll."""
    if request.all:
        sim_ids = [r.get("agent_id") for r in enrolled_get_all_ordered() if r.get("agent_id")]
    elif request.sim_ids:
        sim_ids = request.sim_ids
    else:
        raise HTTPException(status_code=400, detail="Must specify sim_ids or all=true")

    presets = request.steering_presets or {}
    for _sid, pname in presets.items():
        if pname and str(pname).strip():
            _validate_steering_shifting_name_http(str(pname))
    tasks = [
        _send_start_or_stop_one(
            sim_id,
            "start",
            {"steering_preset": (presets.get(sim_id) or "").strip()} if presets.get(sim_id) else {},
            _BATCH_START_STOP_TIMEOUT_SEC,
        )
        for sim_id in sim_ids
    ]
    t0 = time.monotonic()
    gathered = await asyncio.gather(*tasks, return_exceptions=True)
    total_ms = int((time.monotonic() - t0) * 1000)
    logger.info("[batch] start dispatch complete sim_count=%s total_ms=%s", len(sim_ids), total_ms)

    results = {}
    dispatched = []
    for sim_id, raw in zip(sim_ids, gathered):
        if isinstance(raw, Exception):
            ms = int(_BATCH_START_STOP_TIMEOUT_SEC * 1000)
            err_msg = str(raw)
            logger.warning("[batch] sim_id=%s action=start end status=failed error=%s", sim_id, err_msg)
            results[sim_id] = {
                "success": False,
                "message": err_msg,
                "status": "failed",
                "ms": ms,
                "error": err_msg,
            }
            dispatched.append({"id": sim_id, "ok": False, "error": err_msg})
        else:
            results[sim_id] = raw
            dispatched.append({
                "id": sim_id,
                "ok": bool(raw.get("success")),
                "error": raw.get("error") if not raw.get("success") else None,
            })
            if raw.get("pid"):
                set_assigned_state(sim_id, status="running", selection=None)
    return StartStopResponse(results=results, ok=True, dispatched=dispatched)


@router.post("/stop", response_model=StartStopResponse)
async def stop_sims(request: StartStopRequest, _: None = Depends(require_operator)):
    """Stop one or more sims in parallel. Returns immediately with dispatch summary; progress via acs_running poll."""
    if request.all:
        sim_ids = [r.get("agent_id") for r in enrolled_get_all_ordered() if r.get("agent_id")]
    elif request.sim_ids:
        sim_ids = request.sim_ids
    else:
        raise HTTPException(status_code=400, detail="Must specify sim_ids or all=true")

    tasks = [
        _send_start_or_stop_one(sim_id, "stop", {}, _BATCH_START_STOP_TIMEOUT_SEC)
        for sim_id in sim_ids
    ]
    t0 = time.monotonic()
    gathered = await asyncio.gather(*tasks, return_exceptions=True)
    total_ms = int((time.monotonic() - t0) * 1000)
    logger.info("[batch] stop dispatch complete sim_count=%s total_ms=%s", len(sim_ids), total_ms)

    results = {}
    dispatched = []
    for sim_id, raw in zip(sim_ids, gathered):
        if isinstance(raw, Exception):
            ms = int(_BATCH_START_STOP_TIMEOUT_SEC * 1000)
            err_msg = str(raw)
            logger.warning("[batch] sim_id=%s action=stop end status=failed error=%s", sim_id, err_msg)
            results[sim_id] = {
                "success": False,
                "message": err_msg,
                "status": "failed",
                "ms": ms,
                "error": err_msg,
            }
            dispatched.append({"id": sim_id, "ok": False, "error": err_msg})
        else:
            results[sim_id] = raw
            dispatched.append({
                "id": sim_id,
                "ok": bool(raw.get("success")),
                "error": raw.get("error") if not raw.get("success") else None,
            })
    return StartStopResponse(results=results, ok=True, dispatched=dispatched)


class ApplySteeringRequest(BaseModel):
    sim_id: str
    preset_name: str


@router.post("/apply-steering")
async def apply_steering(request: ApplySteeringRequest, _: None = Depends(require_operator)):
    """Apply steering preset to one sim (forwards to agent apply_steering_preset)."""
    _validate_steering_shifting_name_http(request.preset_name)
    result = await send_agent_command(request.sim_id, "apply_steering_preset", {"name": request.preset_name})
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("message", "Agent request failed"))
    return {
        "success": True,
        "message": result.get("message", "Applied"),
        "requires_restart": result.get("requires_restart", False),
    }


class ApplyShiftingRequest(BaseModel):
    sim_id: str
    preset_name: str


@router.post("/apply-shifting")
async def apply_shifting(request: ApplyShiftingRequest, _: None = Depends(require_operator)):
    """Apply shifting/assists preset to one sim (overwrites assists.ini from .cmpreset)."""
    _validate_steering_shifting_name_http(request.preset_name)
    result = await send_agent_command(
        request.sim_id, "apply_shifting_preset", {"name": request.preset_name}
    )
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("message", "Agent request failed"))
    return {
        "success": True,
        "message": result.get("message", "Applied"),
        "requires_restart": result.get("requires_restart", False),
    }


class SetDriverNameRequest(BaseModel):
    sim_id: str
    driver_name: str


class UpdateRaceSelectionRequest(BaseModel):
    """Write server and/or car selection to sim's race.ini (no launch)."""
    server_id: Optional[str] = None
    car_id: Optional[str] = None
    skin_id: Optional[str] = None


class ResetRigRequest(BaseModel):
    sim_id: str
    steering_preset: Optional[str] = "Race"
    shifting_preset: Optional[str] = "H-Pattern"
    display_name: Optional[str] = None


@router.post("/reset-rig")
async def reset_rig(request: ResetRigRequest, _: None = Depends(require_operator)):
    """Reset rig to defaults on one sim (steering, shifting, display name). Does not launch AC. Returns requires_restart if AC was running."""
    _validate_steering_shifting_name_http(request.steering_preset or "Race")
    _validate_steering_shifting_name_http(request.shifting_preset or "H-Pattern")
    body = {
        "steering_preset": request.steering_preset or "Race",
        "shifting_preset": request.shifting_preset or "H-Pattern",
        "display_name": (request.display_name or "").strip() or None,
    }
    result = await send_agent_command(request.sim_id, "reset_rig", body)
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("message", "Agent request failed"))
    return {
        "success": True,
        "message": result.get("message", "Rig reset applied"),
        "requires_restart": result.get("requires_restart", False),
    }


class LaunchOnlineRequest(BaseModel):
    """Payload for online join: server + car + presets."""
    server_id: Optional[str] = None
    server_ip: Optional[str] = None
    server_port: Optional[int] = None
    car_id: Optional[str] = None
    preset_id: Optional[str] = None
    shifter_mode: Optional[str] = None
    sim_display: Optional[str] = None
    max_running_time_minutes: Optional[int] = None  # Session time limit in minutes (0 = no limit); writes time_limited_test.ini


def _launch_online_validation_error(
    status_code: int,
    error: str,
    detail: str,
    hint: str = "",
) -> HTTPException:
    """Return HTTPException with JSON body for launch_online validation/not-found errors."""
    body = {"error": error, "detail": detail}
    if hint:
        body["hint"] = hint
    return HTTPException(status_code=status_code, detail=body)


def _resolve_agent_id_canonical(agent_id: str) -> str | None:
    """Return config agent id that matches agent_id (case-insensitive), or None."""
    if not agent_id or not (aid := (agent_id or "").strip()):
        return None
    try:
        for r in enrolled_get_all_ordered():
            if ((r.get("agent_id") or "").strip().lower() == aid.lower()):
                return (r.get("agent_id") or "").strip()
    except Exception:
        pass
    return None


def _resolve_server_address(server_id: str) -> tuple[str | None, int | None, str | None]:
    """
    Resolve server_id to (host, port, join_display).
    For favourite server_id (ip:port), returns (ip, port, name or ip:port).
    For preset server_id, reads preset server_cfg.ini (TCP_PORT, UDP_PORT, IP).
    Returns (None, None, None) if not found or invalid.
    """
    if not server_id or not _valid_server_id(server_id):
        return None, None, None
    fav = _get_favourite_by_id(server_id)
    if fav:
        host = (fav.get("ip") or "").strip()
        seed_port = int(fav.get("port") or 0)
        if not host or not (1 <= seed_port <= 65535):
            return None, None, None
        live = get_live_server_info(host, seed_port)
        game_port = live.get("game_port") if live.get("game_port") is not None else seed_port
        name = (fav.get("name") or live.get("name") or "").strip() or f"{host}:{game_port}"
        return host, game_port, f"{host}:{game_port}"
    try:
        preset_dir = _get_server_preset_dir_safe(server_id)
        sc_path, _ = _preset_ini_paths(preset_dir)
        if not sc_path.exists():
            return None, None, None
        parsed = parse_ac_server_cfg(sc_path)
        if not parsed:
            return None, None, None
        tcp_port = parsed["tcp_port"]
        udp_port = parsed["udp_port"]
        http_port = parsed.get("http_port")
        logger.info(
            "[server-net] preset='%s' cfg_path='%s' tcp=%s udp=%s http=%s",
            server_id, sc_path, tcp_port, udp_port, http_port,
        )
        join_port = tcp_port
        ip_from_file = parsed.get("ip")
        if ip_from_file:
            host = ip_from_file
            join_display = f"{host}:{join_port}"
        else:
            host = _get_server_join_host()
            join_addr = f"{host}:{join_port}"
            join_display = f"Admin PC (LAN IP) : {join_port}"
            logger.info("[server-addr] preset='%s' join_addr='%s'", server_id, join_addr)
        return host, join_port, join_display
    except HTTPException:
        return None, None, None
    except Exception as e:
        logger.debug("_resolve_server_address exception: server_id=%s e=%s", server_id, e)
        return None, None, None


@router.post("/agents/{agent_id}/launch_online")
async def launch_online(agent_id: str, request: LaunchOnlineRequest, _: None = Depends(require_operator)):
    """Launch one agent for online join. Forwards to agent launch_online (preset applied; join on agent)."""
    if (request.preset_id or "").strip():
        _validate_steering_shifting_name_http(request.preset_id or "")
    sanitized_payload = {
        "server_id": (request.server_id or "").strip() or None,
        "server_ip": (request.server_ip or "").strip() if request.server_ip else None,
        "server_port": request.server_port,
        "car_id": (request.car_id or "").strip() if request.car_id else None,
        "preset_id": (request.preset_id or "").strip() if request.preset_id else None,
    }
    try:
        canonical_id = _resolve_agent_id_canonical(agent_id)
        if not canonical_id:
            raise _launch_online_validation_error(
                404,
                "agent_not_found",
                f"Agent '{agent_id}' is not enrolled.",
                hint="Turn on Enrollment Mode and start the agent to enroll.",
            )
        agent_cfg = enrolled_get(canonical_id)
        if not agent_cfg:
            raise _launch_online_validation_error(
                404,
                "agent_not_found",
                f"Agent '{agent_id}' is not enrolled.",
                hint="Turn on Enrollment Mode and start the agent to enroll.",
            )
        cache = get_status_cache()
        agent_status = cache.get(canonical_id)
        if not agent_status or not getattr(agent_status, "online", False):
            raise _launch_online_validation_error(
                503,
                "agent_unreachable",
                f"Agent '{agent_id}' is offline or unreachable.",
                hint="Ensure the agent is running and reachable, then retry.",
            )
        server_ip = (request.server_ip or "").strip() or None
        server_port = request.server_port
        server_id = (request.server_id or "").strip() or None
        car_id = (request.car_id or "").strip() or None
        preset_id = (request.preset_id or "").strip() or None
        shifter_mode = (request.shifter_mode or "").strip() or None
        sim_display = (request.sim_display or "").strip() or None

        if not car_id:
            raise _launch_online_validation_error(
                400,
                "car_required",
                "car_id is required and must be non-empty.",
                hint="Select a car in the online join panel.",
            )
        if server_id and (not server_ip or server_port is None):
            resolved_host, resolved_port, resolved_join_display = _resolve_server_address(server_id)
            if resolved_host is not None and resolved_port is not None:
                server_ip = resolved_host
                server_port = resolved_port
                if resolved_join_display:
                    logger.info("launch_online: server_id=%s resolved join_display=%s", server_id, resolved_join_display)
            else:
                raise _launch_online_validation_error(
                    400,
                    "server_unresolvable",
                    "Server could not be resolved: missing or invalid [SERVER] section (TCP_PORT and UDP_PORT required).",
                    hint="Ensure server_cfg.ini has [SERVER] with TCP_PORT and UDP_PORT set. IP is optional.",
                )
        if not server_ip or server_port is None:
            raise _launch_online_validation_error(
                400,
                "server_required",
                "server_ip and server_port are required for online join (or provide server_id to resolve from preset).",
                hint="Select a server from the dropdown; server details are resolved from the preset.",
            )
        try:
            server_port_int = int(server_port)
            if server_port_int < 1 or server_port_int > 65535:
                raise ValueError("port out of range")
        except (TypeError, ValueError):
            raise _launch_online_validation_error(
                400,
                "invalid_port",
                f"server_port must be a valid port (1-65535), got: {server_port!r}.",
                hint="Use the server list; do not edit the request manually.",
            )
        body = {
            "server_ip": server_ip,
            "server_port": server_port_int,
            "car_id": car_id,
            "preset_id": preset_id,
            "shifter_mode": shifter_mode,
            "sim_display": sim_display,
        }
        max_min = request.max_running_time_minutes
        if max_min is not None and max_min >= 0:
            body["max_running_time_minutes"] = max_min
        pwd = get_config().global_server_password
        if pwd is not None:
            body["global_server_password"] = pwd
        if server_id:
            fav = _get_favourite_by_id(server_id)
            if fav:
                body["server_cfg_snapshot"] = _build_favourite_server_cfg_snapshot(fav, server_id, skip_live_fetch=True)
                body["preset_name"] = server_id
            else:
                sc_data, _ = _read_full_server_cfg(server_id)
                if not sc_data:
                    raise _launch_online_validation_error(
                        400,
                        "preset_server_cfg_missing",
                        f"Preset server_cfg.ini not found or unreadable for server_id={server_id!r}. Full sync required for online join.",
                        hint="Ensure preset exists with server_cfg.ini in presets/<server_id>/ or presets/<server_id>/cfg/.",
                    )
                body["server_cfg_snapshot"] = sc_data
                body["preset_name"] = server_id
        join_target = f"{server_ip}:{server_port_int}"
        logger.info(
            "launch_online: agent_id=%s requested_server_id=%s resolved_target=%s car_id=%s preset_id=%s join=%s",
            canonical_id,
            server_id or "(none)",
            join_target,
            car_id,
            preset_id or "(default)",
            join_target,
        )
        result = await send_agent_command(canonical_id, "launch_online", body)
        if not result.get("success"):
            msg = result.get("message", "Agent request failed")
            logger.warning("launch_online: agent_id=%s agent returned failure: %s", canonical_id, msg)
            raise HTTPException(
                status_code=500,
                detail={"error": "launch_online_failed", "detail": msg},
            )
        # Save assigned selection for sim display (admin path)
        selection = {
            "mode": {"kind": "online", "submode": "join"},
            "server": {
                "server_id": server_id,
                "address_override": None,
            },
            "car": {"car_id": car_id, "skin_id": "default"},
            "track": {},
            "assists": {"preset_id": preset_id or "race", "overrides": None},
            "rules": {"force_manual_shifting": False, "limit_speed_kph": None},
            "notes": {"display_title": join_target, "ui_tags": ["Online"]},
        }
        set_assigned_state(canonical_id, status="launching", selection=selection)
        return {"success": True, "message": result.get("message", "Sent")}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "launch_online: agent_id=%s sanitized_payload=%s uncaught exception",
            agent_id,
            sanitized_payload,
        )
        raise HTTPException(
            status_code=500,
            detail={"error": "launch_online_failed", "detail": str(e)},
        )


@router.post("/set-driver-name")
async def set_driver_name(request: SetDriverNameRequest, _: None = Depends(require_operator)):
    """Set driver name on a sim (updates race.ini [CAR_0] DRIVER_NAME and [REMOTE] NAME)."""
    result = await send_agent_command(
        request.sim_id, "set_driver_name", {"driver_name": request.driver_name}
    )
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("message", "Agent request failed"))
    return {"success": True, "message": result.get("message", "OK")}


@router.post("/agents/{agent_id}/update_race_selection")
async def update_race_selection(agent_id: str, request: UpdateRaceSelectionRequest, _: None = Depends(require_operator)):
    """Write server and/or car selection to the sim's race.ini when user selects server or car in UI (no launch)."""
    canonical_id = _canonical_agent_id(agent_id)
    if not canonical_id:
        raise HTTPException(status_code=404, detail=f"Agent {agent_id!r} not found")
    server_id = (request.server_id or "").strip() or None
    car_id = (request.car_id or "").strip() or None
    skin_id = (request.skin_id or "").strip() or "default"
    body: dict[str, Any] = {"car_id": car_id or None, "skin_id": skin_id}
    pwd = get_config().global_server_password
    if pwd is not None:
        body["global_server_password"] = pwd
    if server_id:
        resolved_host, resolved_port, _ = _resolve_server_address(server_id)
        if resolved_host is not None and resolved_port is not None:
            body["join_addr"] = f"{resolved_host}:{resolved_port}"
            body["server_ip"] = resolved_host
            body["server_port"] = resolved_port
            fav = _get_favourite_by_id(server_id)
            if fav:
                body["server_cfg_snapshot"] = _build_favourite_server_cfg_snapshot(fav, server_id, skip_live_fetch=True)
                body["preset_name"] = server_id
            else:
                sc_data, _ = _read_full_server_cfg(server_id)
                if not sc_data:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Preset server_cfg.ini not found or unreadable for server_id={server_id!r}. Required for race.ini sync.",
                    )
                body["server_cfg_snapshot"] = sc_data
                body["preset_name"] = server_id
        else:
            body["join_addr"] = None
    result = await send_agent_command(canonical_id, "update_race_selection", body)
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("message", "Agent request failed"))
    # Update controller state so sim display can show the selected car (red highlight)
    ctrl = get_controller_state(canonical_id)
    existing = (ctrl.assigned_selection or {}).copy()
    selection: dict[str, Any] = dict(existing)
    selection["car_id"] = car_id or existing.get("car_id")
    selection["car"] = {"car_id": car_id or (existing.get("car") or {}).get("car_id"), "skin_id": skin_id}
    if server_id:
        selection["server"] = selection.get("server") or {}
        if isinstance(selection["server"], dict):
            selection["server"] = dict(selection["server"])
            selection["server"]["server_id"] = server_id
    else:
        # Keep current assignment server in selection when only car was updated
        _, assignment_server_id = _resolve_assignment(canonical_id)
        if assignment_server_id:
            selection["server"] = dict(selection.get("server") or {})
            selection["server"]["server_id"] = assignment_server_id
    set_assigned_state(canonical_id, status="idle", selection=selection)
    return {"success": True, "message": result.get("message", "Updated")}


# Car list cache: avoid rescanning content/cars on every GET /cars (TTL seconds).
_CARS_CACHE_TTL = 300
_cars_cache: Optional[tuple[str, float, list[dict]]] = None

# --- Cars list (content/cars + CM cache previews) ---


def _parse_ui_skin_json(path: Path) -> str:
    """Read display name from content/cars/<car_id>/skins/<folder>/ui_skin.json (skinname or name)."""
    if not path.is_file():
        return ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return ""
    if not isinstance(data, dict):
        return ""
    return (data.get("skinname") or data.get("name") or "").strip()


def _list_skins_for_car(car_folder: Path) -> list[dict]:
    """List skins from content/cars/<car_id>/skins/; only folders containing ui_skin.json. Return [{folder, name}, ...]."""
    skins_dir = car_folder / "skins"
    if not skins_dir.is_dir():
        return []
    result = []
    for skin_path in skins_dir.iterdir():
        if not skin_path.is_dir() or skin_path.name == STFOLDER_NAME:
            continue
        ui_skin = skin_path / "ui_skin.json"
        if not ui_skin.is_file():
            continue
        display_name = _parse_ui_skin_json(ui_skin) or skin_path.name
        result.append({"folder": skin_path.name, "name": display_name})
    return sorted(result, key=lambda x: x["name"].lower())


# Path cache for car/track image resolution (avoids repeated filesystem scans when loading many thumbnails).
_IMAGE_PATH_CACHE_TTL = 300.0  # seconds
_car_preview_path_cache: dict[str, tuple[float, Optional[Path]]] = {}


def _find_car_preview(car_id: str) -> Optional[Path]:
    """Return path to car preview image: CM cache first, then content/cars/<car_id>/skins/<skin>/preview.jpg."""
    if not car_id or car_id.strip() == STFOLDER_NAME:
        return None
    now = time.time()
    if car_id in _car_preview_path_cache:
        cached_at, path = _car_preview_path_cache[car_id]
        if now - cached_at < _IMAGE_PATH_CACHE_TTL and path is not None and path.is_file():
            return path
        if now - cached_at >= _IMAGE_PATH_CACHE_TTL or path is None or not path.is_file():
            del _car_preview_path_cache[car_id]
    cache_dir = _car_cache_dir(car_id)
    if cache_dir.is_dir():
        for ext in ("*.png", "*.jpg", "*.jpeg"):
            for p in cache_dir.glob(ext):
                if p.is_file():
                    _car_preview_path_cache[car_id] = (now, p)
                return p
    cars_dir = _cars_dir()
    car_folder = cars_dir / car_id
    skins_dir = car_folder / "skins"
    if skins_dir.is_dir():
        for skin_path in skins_dir.iterdir():
            if not skin_path.is_dir() or skin_path.name == STFOLDER_NAME:
                continue
            for name in ("preview.jpg", "preview.jpeg", "preview.png"):
                p = skin_path / name
                if p.exists():
                    _car_preview_path_cache[car_id] = (now, p)
                    return p
    _car_preview_path_cache[car_id] = (now, None)
    return None


def _find_car_skin_preview(car_id: str, skin: str) -> Optional[Path]:
    """Return path to content/cars/<car_id>/skins/<skin>/preview.jpg (or .png)."""
    if not car_id or not skin or ".." in car_id or ".." in skin or "/" in car_id or "\\" in car_id or car_id.strip() == STFOLDER_NAME or skin.strip() == STFOLDER_NAME:
        return None
    cars_dir = _cars_dir()
    skin_dir = cars_dir / car_id / "skins" / skin
    if not skin_dir.is_dir():
        return None
    for name in ("preview.jpg", "preview.jpeg", "preview.png"):
        p = skin_dir / name
        if p.is_file():
            return p
    return None


def _safe_track_id(track_id: str) -> Optional[str]:
    """Return normalized track_id for path resolution; None if invalid (no .., /, \\, or .stfolder)."""
    if not track_id or not (s := track_id.strip()):
        return None
    if ".." in s or "/" in s or "\\" in s or s == STFOLDER_NAME:
        return None
    return s


def _normalize_layout(layout: Optional[str]) -> Optional[str]:
    """Return layout for path use; None if None, empty or 'default' (treat as no layout folder)."""
    if layout is None:
        return None
    if not (s := layout.strip()):
        return None
    if s.lower() == "default":
        return None
    if ".." in s or "/" in s or "\\" in s:
        return None
    return s


def _safe_config(config_raw: str) -> str:
    """Return config/layout for API response; 'default' if empty or contains traversal."""
    if not config_raw or not (s := config_raw.strip()):
        return "default"
    if ".." in s or "/" in s or "\\" in s:
        return "default"
    return s


# Last resolution logged (track_id, layout, outline_path, name_path) to log only when something changes.
_track_resolve_log_cache: tuple = (None, None, None, None)


def _resolve_track_outline_content(track_id: str, layout: Optional[str]) -> Optional[Path]:
    """
    Resolve track outline image from CONTENT only (no cache).
    Order: ui/<layout>/outline.png, then ui/outline.png.
    Returns first existing path or None. Used for Server Main track outline panel.
    """
    tid = _safe_track_id(track_id)
    layout_norm = _normalize_layout(layout)
    if not tid:
        return None
    tracks_dir = _tracks_dir()
    track_root = tracks_dir / tid
    if not track_root.is_dir():
        return None
    if layout_norm:
        p = track_root / "ui" / layout_norm / "outline.png"
        if p.is_file():
            return p
    p = track_root / "ui" / "outline.png"
    if p.is_file():
        return p
    return None


def _find_track_outline(track_id: str, layout: Optional[str]) -> Optional[Path]:
    """
    Resolve track outline/preview from cache then AC CONTENT (content/tracks/<track_id>/).
    Search order: cache → layout dir → base ui → track root.
    Per-tier order: outline.png, outline_cropped.png, map.png (track root: outline.png, map.png only).
    Returns first existing .png path.
    """
    global _track_resolve_log_cache
    tid = _safe_track_id(track_id)
    layout_norm = _normalize_layout(layout)
    if not tid:
        return None
    # Try cache first (CM/AC cache may have outline.png or preview.png)
    cache_dir = _track_cache_dir(tid, layout or "")
    for name in ("outline.png", "preview.png", "outline_cropped.png", "map.png"):
        p = cache_dir / name
        if p.is_file():
            return p
    if layout_norm:
        cache_dir_layout = _track_cache_dir(tid, layout_norm)
        for name in ("outline.png", "preview.png", "outline_cropped.png", "map.png"):
            p = cache_dir_layout / name
            if p.is_file():
                return p
    tracks_dir = _tracks_dir()
    track_root = tracks_dir / tid
    if not track_root.is_dir():
        return None
    candidates: list[Path] = []
    # A–C: If layout present and not default: ui/<layout>/
    if layout_norm:
        candidates.extend([
            track_root / "ui" / layout_norm / "outline.png",
            track_root / "ui" / layout_norm / "outline_cropped.png",
            track_root / "ui" / layout_norm / "map.png",
        ])
    # D–F: Base UI
    candidates.extend([
        track_root / "ui" / "outline.png",
        track_root / "ui" / "outline_cropped.png",
        track_root / "ui" / "map.png",
    ])
    # G–H: Track root (no outline_cropped)
    candidates.extend([
        track_root / "outline.png",
        track_root / "map.png",
    ])
    selected: Optional[Path] = None
    for p in candidates:
        if p.is_file() and p.suffix.lower() == ".png":
            selected = p
            break
    name_path_placeholder = "(see track name resolution)"
    cache_key = (tid, layout_norm, str(selected) if selected else None, name_path_placeholder)
    if cache_key != _track_resolve_log_cache:
        _track_resolve_log_cache = cache_key
        logger.info(
            "[track outline] track_id=%r layout=%r attempted=%s selected=%s",
            tid, layout_norm, [str(c) for c in candidates], str(selected) if selected else None,
        )
    return selected


def _find_track_map(track_id: str, layout: Optional[str]) -> Optional[Path]:
    """Resolve map.png only (for overlay on track preview). Same path order as outline: cache → ui/<layout>/ → ui/ → track root."""
    tid = _safe_track_id(track_id)
    layout_norm = _normalize_layout(layout)
    if not tid:
        return None
    cache_dir = _track_cache_dir(tid, layout or "")
    for p in (cache_dir / "map.png",):
        if p.is_file():
            return p
    if layout_norm:
        cache_dir_layout = _track_cache_dir(tid, layout_norm)
        if (cache_dir_layout / "map.png").is_file():
            return cache_dir_layout / "map.png"
    tracks_dir = _tracks_dir()
    track_root = tracks_dir / tid
    if not track_root.is_dir():
        return None
    candidates: list[Path] = []
    if layout_norm:
        candidates.append(track_root / "ui" / layout_norm / "map.png")
    candidates.extend([
        track_root / "ui" / "map.png",
        track_root / "map.png",
    ])
    for p in candidates:
        if p.is_file() and p.suffix.lower() == ".png":
            return p
    return None


def _find_track_base(track_id: str, layout: Optional[str]) -> Optional[Path]:
    """Resolve base track image for stacking under map overlay: outline, outline_cropped, or preview only (no map.png)."""
    tid = _safe_track_id(track_id)
    layout_norm = _normalize_layout(layout)
    if not tid:
        return None
    # Cache: outline, outline_cropped, preview (no map)
    cache_dir = _track_cache_dir(tid, layout or "")
    for name in ("outline.png", "preview.png", "outline_cropped.png"):
        p = cache_dir / name
        if p.is_file():
            return p
    for ext in (".png", ".jpg", ".jpeg"):
        p = cache_dir / ("preview" + ext)
        if p.is_file():
            return p
    if layout_norm:
        cache_dir_layout = _track_cache_dir(tid, layout_norm)
        for name in ("outline.png", "preview.png", "outline_cropped.png"):
            p = cache_dir_layout / name
            if p.is_file():
                return p
        for ext in (".png", ".jpg", ".jpeg"):
            p = cache_dir_layout / ("preview" + ext)
            if p.is_file():
                return p
    tracks_dir = _tracks_dir()
    track_root = tracks_dir / tid
    if not track_root.is_dir():
        return None
    candidates: list[Path] = []
    if layout_norm:
        candidates.extend([
            track_root / "ui" / layout_norm / "outline.png",
            track_root / "ui" / layout_norm / "outline_cropped.png",
        ])
        for ext in (".png", ".jpg", ".jpeg"):
            candidates.append(track_root / "ui" / layout_norm / ("preview" + ext))
    candidates.extend([
        track_root / "ui" / "outline.png",
        track_root / "ui" / "outline_cropped.png",
    ])
    for ext in (".png", ".jpg", ".jpeg"):
        candidates.append(track_root / "ui" / ("preview" + ext))
    candidates.extend([
        track_root / "outline.png",
    ])
    for ext in (".png", ".jpg", ".jpeg"):
        candidates.append(track_root / ("preview" + ext))
    for p in candidates:
        if p.is_file():
            return p
    return None


def _find_track_preview(track_id: str, layout: Optional[str]) -> Optional[Path]:
    """Resolve preview image: cache → ui/<layout>/preview.* → ui/preview.* → track root preview.*."""
    tid = _safe_track_id(track_id)
    if not tid:
        return None
    layout_norm = _normalize_layout(layout)
    # 0) CM/AC cache: Documents/Assetto Corsa/cache/tracks/<track_id>/[layout]/preview.*
    cache_dir = _track_cache_dir(tid, layout or "")
    for name in ("preview.png", "preview.jpg", "preview.jpeg"):
        p = cache_dir / name
        if p.is_file():
            return p
    if layout_norm:
        cache_dir_layout = _track_cache_dir(tid, layout_norm)
        for name in ("preview.png", "preview.jpg", "preview.jpeg"):
            p = cache_dir_layout / name
            if p.is_file():
                return p
    tracks_dir = _tracks_dir()
    track_root = tracks_dir / tid
    if not track_root.is_dir():
        return None
    # 1) ui/<layout>/preview.* only if layout is a real string
    if layout_norm:
        layout_dir = track_root / "ui" / layout_norm
        if layout_dir.is_dir():
            for name in ("preview.png", "preview.jpg", "preview.jpeg"):
                p = layout_dir / name
                if p.is_file():
                    return p
    # 2) base UI: ui/preview.*
    ui_dir = track_root / "ui"
    if ui_dir.is_dir():
        for name in ("preview.png", "preview.jpg", "preview.jpeg"):
            p = ui_dir / name
            if p.is_file():
                return p
    # 3) track root preview.*
    for name in ("preview.png", "preview.jpg", "preview.jpeg"):
        p = track_root / name
        if p.is_file():
            return p
    return None


def _load_json_with_fallback(path: Union[Path, str]) -> Optional[dict]:
    """
    Load JSON from file with encoding fallbacks. Safe for AC mod files (often cp1252/latin1).
    Tries: utf-8-sig, utf-8, cp1252, latin-1. Returns None if file missing, decode error, or invalid JSON.
    """
    p = Path(path) if not isinstance(path, Path) else path
    if not p.is_file():
        return None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(p, "r", encoding=encoding) as f:
                data = json.load(f)
            return data if isinstance(data, dict) else None
        except UnicodeDecodeError:
            continue
        except (json.JSONDecodeError, OSError):
            return None
    return None


def _prettify_track_id(track_id: str) -> str:
    """Fallback display name: replace underscores with spaces and title-case."""
    if not track_id:
        return ""
    return track_id.replace("_", " ").strip().title()


# Cache for track name resolution log (log once per change).
_track_name_log_cache: tuple = (None, None, None)


def _get_track_display_name(track_id: str, layout: Optional[str]) -> Optional[str]:
    """
    Resolve track display name from AC CONTENT only.
    Paths: if layout present and not default → content/tracks/<track_id>/ui/<layout>/ui_track.json;
    else → content/tracks/<track_id>/ui/ui_track.json. Parse JSON and use the `name` field.
    Fallback: prettified track_id.
    """
    global _track_name_log_cache
    tid = _safe_track_id(track_id)
    layout_norm = _normalize_layout(layout)
    if not tid:
        return None
    tracks_dir = _tracks_dir()
    track_root = tracks_dir / tid
    attempted: list[str] = []
    selected_path: Optional[str] = None
    result: Optional[str] = None
    # 1) ui/<layout>/ui_track.json (only if layout present and not default)
    if layout_norm:
        p = track_root / "ui" / layout_norm / "ui_track.json"
        attempted.append(str(p))
        if p.is_file():
            data = _load_json_with_fallback(p)
            # Prefer `name` field per AC ui_track.json; fallbacks for older mods.
            name = (data.get("name") or data.get("screenName") or data.get("uiName") or data.get("description") or "").strip() if data else ""
            if name:
                selected_path = str(p)
                result = name
    if result is None:
        # 2) ui/ui_track.json
        p = track_root / "ui" / "ui_track.json"
        attempted.append(str(p))
        if p.is_file():
            data = _load_json_with_fallback(p)
            # Prefer `name` field per AC ui_track.json; fallbacks for older mods.
            name = (data.get("name") or data.get("screenName") or data.get("uiName") or data.get("description") or "").strip() if data else ""
            if name:
                selected_path = str(p)
                result = name
    if result is None:
        selected_path = "(prettified track_id)"
        result = _prettify_track_id(tid)
    cache_key = (tid, layout_norm, selected_path)
    if cache_key != _track_name_log_cache:
        _track_name_log_cache = cache_key
        logger.info(
            "[track name] track_id=%r layout=%r attempted=%s selected=%s → %r",
            tid, layout_norm, attempted, selected_path, (result or "")[:60],
        )
    return result or None


def _get_track_ui_info(track_id: str, layout: Optional[str]) -> dict:
    """
    Load ui_track.json for the given track_id and layout. Returns a flat dict of display-safe values
    for UI: name, description, country, city, length_km, pits, author, year, etc.
    Tries ui/<layout>/ui_track.json first, then ui/ui_track.json.
    """
    tid = _safe_track_id(track_id)
    layout_norm = _normalize_layout(layout)
    out: dict[str, Any] = {}
    if not tid:
        return out
    tracks_dir = _tracks_dir()
    track_root = tracks_dir / tid
    if not track_root.is_dir():
        return out
    json_path: Optional[Path] = None
    if layout_norm:
        p = track_root / "ui" / layout_norm / "ui_track.json"
        if p.is_file():
            json_path = p
    if json_path is None:
        p = track_root / "ui" / "ui_track.json"
        if p.is_file():
            json_path = p
    if json_path is None or not json_path.is_file():
        out["name"] = _get_track_display_name(tid, layout_norm) or _prettify_track_id(tid)
        return out
    data = _load_json_with_fallback(json_path)
    if not data or not isinstance(data, dict):
        out["name"] = _get_track_display_name(tid, layout_norm) or _prettify_track_id(tid)
        return out
    # Map common ui_track.json / CM-style keys to a consistent response
    name_raw = (data.get("name") or data.get("screenName") or data.get("uiName") or "").strip()
    out["name"] = name_raw or _get_track_display_name(tid, layout_norm) or _prettify_track_id(tid)
    layout_name_raw = (data.get("layoutName") or data.get("layout_name") or data.get("layout") or "").strip()
    if layout_name_raw:
        out["layout_name"] = layout_name_raw
    desc = (data.get("description") or data.get("desc") or "").strip()
    if desc and desc != out.get("name"):
        out["description"] = desc
    for key, api_key in [
        ("country", "country"),
        ("city", "city"),
        ("length", "length"),
        ("pits", "pits"),
        ("author", "author"),
        ("year", "year"),
    ]:
        val = data.get(key) or data.get(key.capitalize())
        if val is not None and str(val).strip() != "":
            out[api_key] = str(val).strip() if not isinstance(val, (int, float)) else val
    # Common alternative keys
    if "length" not in out and data.get("lengthMeters") is not None:
        try:
            m = int(float(data["lengthMeters"]))
            out["length_km"] = round(m / 1000.0, 2)
        except (TypeError, ValueError):
            pass
    if "length" in out and isinstance(out["length"], (int, float)):
        try:
            v = float(out["length"])
            out["length_km"] = round(v / 1000.0, 2) if v >= 100 else round(v, 2)
        except (TypeError, ValueError):
            pass
    if "pits" not in out:
        for alt in ("pitboxes", "pitCount", "pit_count"):
            if data.get(alt) is not None:
                out["pits"] = data[alt]
                break
    # Categories/tags: list or comma-separated string (circuit, drift, rally, touge, oval, etc.)
    raw_cats = data.get("categories") or data.get("tags") or data.get("category") or data.get("tag")
    if raw_cats is not None:
        if isinstance(raw_cats, list):
            cats = [str(x).strip().lower() for x in raw_cats if x is not None and str(x).strip()]
        else:
            cats = [s.strip().lower() for s in str(raw_cats).split(",") if s.strip()]
        if cats:
            out["categories"] = list(dict.fromkeys(cats))
    # Strip None values for response
    return {k: v for k, v in out.items() if v is not None and v != ""}


def _build_safe_track_payload(server_id: str, track_id_raw: str, config_raw: str) -> dict:
    """Build track dict for API: normalize CSP-style TRACK from preset, then safe id/name. Never emit raw paths."""
    track_norm = _normalize_track_id_from_preset(track_id_raw or "")
    track_id_safe = (_safe_track_id(track_norm) or "").strip() or ""
    config_safe = _safe_config(config_raw)
    logger.info(
        "[preset track] preset=%r track_raw=%r track_norm=%r track_safe=%r config_raw=%r",
        server_id, track_id_raw or "(empty)", track_norm or "(empty)", track_id_safe or "(empty)", config_raw or "(empty)",
    )
    if not track_id_safe:
        return {"id": "", "config": "default", "name": "Unknown track"}
    layout_for_name = config_safe or "default"
    name = _get_track_display_name(track_id_safe, layout_for_name) or track_id_safe or ""
    return {"id": track_id_safe, "config": config_safe, "name": name or "Unknown track"}


def _sanitize_track_for_response(track: Any) -> dict:
    """Ensure track dict emitted by any API never contains raw path/traversal. Use for server-display."""
    if not track or not isinstance(track, dict):
        return {"id": "", "config": "default", "name": "Unknown track"}
    raw_id = (track.get("id") or "").strip()
    raw_config = (track.get("config") or "").strip()
    safe_id = _safe_track_id(raw_id) or ""
    safe_config = _safe_config(raw_config) if raw_config else "default"
    if not safe_id:
        return {"id": "", "config": "default", "name": "Unknown track"}
    name = _get_track_display_name(safe_id, safe_config or "default") or safe_id
    return {"id": safe_id, "config": safe_config, "name": name or safe_id}


def _list_cars() -> list[dict]:
    """Scan content/cars; parse ui/ui_car.json per car. Resilient: one bad mod can't crash the whole scan."""
    cars_dir = _cars_dir()
    if not cars_dir.is_dir():
        return []
    folders = [c for c in cars_dir.iterdir() if c.is_dir() and c.name != STFOLDER_NAME]
    result = []
    error_count = 0
    for child in sorted(folders):
        car_id = child.name
        try:
            ui_car = child / "ui" / "ui_car.json"
            if not ui_car.exists():
                meta = {"name": "", "class": "", "bhp": None, "weight": None, "topspeed": None}
                name = _prettify_car_id(car_id) or car_id
            else:
                meta = _parse_ui_car_json(ui_car)
                name = (meta.get("name") or "").strip() or _prettify_car_id(car_id) or car_id
            try:
                preview_url = f"/api/cars/{car_id}/preview" if _find_car_preview(car_id) else ""
            except Exception:
                preview_url = ""
            skins = []
            try:
                skins = _list_skins_for_car(child)
            except Exception:
                pass
            result.append({
                "car_id": car_id,
                "name": name,
                "class": meta.get("class") or "",
                "bhp": meta.get("bhp"),
                "weight": meta.get("weight"),
                "topspeed": meta.get("topspeed"),
                "preview_url": preview_url,
                "skins": skins,
            })
        except Exception as e:
            error_count += 1
            logger.warning("Car scan failed for %s: %s", car_id, e)
            try:
                result.append({
                    "car_id": car_id,
                    "name": _prettify_car_id(car_id) or car_id,
                    "class": "",
                    "bhp": None,
                    "weight": None,
                    "topspeed": None,
                    "preview_url": "",
                    "skins": [],
                })
            except Exception:
                pass
    logger.debug("Cars list: %d cars, %d errors", len(result), error_count)
    return result


@router.get("/cars")
async def get_cars():
    """List cars from AC content/cars with name, class, specs, and preview_url (cached for 5 min)."""
    global _cars_cache
    cfg = get_config()
    ac_cars_path = getattr(cfg, "ac_cars_path", None) or ""
    ac_content_root = getattr(cfg, "ac_content_root", None) or ""
    cars_dir = None
    try:
        cars_dir = _cars_dir()
        cars_dir_str = str(cars_dir)
        now = time.time()
        if _cars_cache is not None:
            cached_dir, cached_ts, cached_list = _cars_cache
            if cached_dir == cars_dir_str and (now - cached_ts) < _CARS_CACHE_TTL:
                return {"cars": cached_list, "cars_path": cars_dir_str}
        cars = _list_cars()
        _cars_cache = (cars_dir_str, now, cars)
        return {"cars": cars, "cars_path": str(cars_dir)}
    except Exception as e:
        tb = traceback.format_exc()
        print(tb)
        try:
            cars_dir_str = str(cars_dir) if cars_dir is not None else str(_cars_dir())
        except Exception:
            cars_dir_str = "(resolved failed)"
        return JSONResponse(
            status_code=500,
            content={
                "error": "Cars scan failed",
                "exception": str(e),
                "traceback": tb,
                "cars_dir": cars_dir_str,
                "ac_cars_path": ac_cars_path,
                "ac_content_root": ac_content_root,
            },
        )


@router.get("/cars/{car_id}/display-name")
async def get_car_display_name(car_id: str):
    """Return human-readable display name for a car (from ui_car.json or prettified id)."""
    name = _get_car_display_name(car_id)
    return {"name": name}


def _image_response(path: Path, media_type: str):
    """FileResponse with cache headers so browsers cache car/skin images."""
    resp = FileResponse(path, media_type=media_type)
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@router.get("/cars/{car_id}/preview")
async def get_car_preview(car_id: str):
    """Serve preview image from CM/AC cache, or 404 (frontend shows placeholder)."""
    if ".." in car_id or "/" in car_id or "\\" in car_id:
        raise HTTPException(status_code=404, detail="Invalid car_id")
    path = _find_car_preview(car_id)
    if path is None:
        raise HTTPException(status_code=404, detail="Preview not found")
    return _image_response(path, "image/png" if path.suffix.lower() == ".png" else "image/jpeg")


@router.get("/cars/{car_id}/skins/{skin_name}/livery")
async def get_car_skin_livery(car_id: str, skin_name: str):
    """Serve livery.png for a car skin (content/cars/<car_id>/skins/<skin_name>/livery.png)."""
    if ".." in car_id or "/" in car_id or "\\" in car_id:
        raise HTTPException(status_code=404, detail="Invalid car_id")
    if ".." in skin_name or "/" in skin_name or "\\" in skin_name:
        raise HTTPException(status_code=404, detail="Invalid skin_name")
    cars_dir = _cars_dir()
    path = cars_dir / car_id / "skins" / skin_name / "livery.png"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Livery not found")
    return _image_response(path, "image/png")


@router.get("/cars/{car_id}/skins/{skin_name}/preview")
async def get_car_skin_preview(car_id: str, skin_name: str):
    """Serve preview image for a car skin; falls back to any skin preview for the car if the specific skin has none."""
    if ".." in car_id or "/" in car_id or "\\" in car_id:
        raise HTTPException(status_code=404, detail="Invalid car_id")
    if ".." in skin_name or "/" in skin_name or "\\" in skin_name:
        raise HTTPException(status_code=404, detail="Invalid skin_name")
    path = _find_car_skin_preview(car_id, skin_name)
    if path is None:
        path = _find_car_preview(car_id)
    if path is None:
        raise HTTPException(status_code=404, detail="Skin preview not found")
    return _image_response(path, "image/jpeg" if path.suffix.lower() in (".jpg", ".jpeg") else "image/png")


@router.get("/tracks/{track_id}/display-name")
async def get_track_display_name(track_id: str, layout: Optional[str] = None):
    """Return display name for a track (and optional layout) for UI labels."""
    if ".." in track_id or "/" in track_id or "\\" in track_id or track_id.strip() == STFOLDER_NAME:
        raise HTTPException(status_code=404, detail="Invalid track_id")
    layout_norm = _normalize_layout(layout) if layout is not None else None
    name = _get_track_display_name(track_id.strip(), layout_norm)
    return {"name": name or _prettify_track_id(track_id.strip()) or track_id}


@router.get("/tracks/{track_id}/layouts/{layout}/info")
async def get_track_layout_info(track_id: str, layout: str):
    """Return track info from ui_track.json for the given layout (name, description, country, city, length_km, pits, author, year)."""
    if ".." in track_id or "/" in track_id or "\\" in track_id or track_id.strip() == STFOLDER_NAME:
        raise HTTPException(status_code=404, detail="Invalid track_id")
    raw_layout = (layout or "").strip()
    if raw_layout and (".." in raw_layout or "/" in raw_layout or "\\" in raw_layout):
        raise HTTPException(status_code=404, detail="Invalid layout")
    layout_norm = None if raw_layout.lower() in ("", "default") else raw_layout
    info = _get_track_ui_info(track_id.strip(), layout_norm)
    return info


@router.get("/tracks/{track_id}/layouts/{layout}/preview")
async def get_track_layout_preview(track_id: str, layout: str):
    """Serve track image: try preview.png/jpg first, then outline.png, outline_cropped.png, map.png (CONTENT only)."""
    if ".." in track_id or "/" in track_id or "\\" in track_id:
        raise HTTPException(status_code=404, detail="Invalid track_id")
    raw_layout = layout if layout is not None else ""
    if raw_layout and (".." in raw_layout or "/" in raw_layout or "\\" in raw_layout):
        raise HTTPException(status_code=404, detail="Invalid layout")
    layout_norm = None if (raw_layout or "").strip().lower() in ("", "default") else raw_layout.strip()
    path = _find_track_preview(track_id, layout_norm)
    if path is None or not path.is_file():
        path = _find_track_outline(track_id, layout_norm)
    resolved = str(path.resolve()) if path and path.is_file() else None
    logger.debug(
        "[track preview] track_id=%s layout=%s resolved=%s",
        track_id, layout_norm or "(default)", resolved,
    )
    if path is None or not path.is_file():
        raise HTTPException(status_code=404, detail="Track outline/preview not found")
    media_type = "image/jpeg" if path.suffix.lower() in (".jpg", ".jpeg") else "image/png"
    return _image_response(path, media_type)


@router.get("/tracks/{track_id}/layouts/{layout}/outline")
async def get_track_layout_outline(track_id: str, layout: str):
    """Serve track outline image for the given layout: ui/<layout>/outline.png or ui/outline.png (CONTENT only). 404 if missing."""
    if ".." in track_id or "/" in track_id or "\\" in track_id:
        raise HTTPException(status_code=404, detail="Invalid track_id")
    raw_layout = layout if layout is not None else ""
    if raw_layout and (".." in raw_layout or "/" in raw_layout or "\\" in raw_layout):
        raise HTTPException(status_code=404, detail="Invalid layout")
    layout_norm = None if (raw_layout or "").strip().lower() in ("", "default") else raw_layout.strip()
    path = _resolve_track_outline_content(track_id.strip(), layout_norm)
    if path is None or not path.is_file():
        raise HTTPException(status_code=404, detail="Track outline not found")
    return _image_response(path, "image/png")


@router.get("/tracks/{track_id}/layouts/{layout}/map")
async def get_track_layout_map(track_id: str, layout: str):
    """Serve track map.png for overlay on preview/outline. Same path order as outline (layout → ui → root). 404 if missing."""
    if ".." in track_id or "/" in track_id or "\\" in track_id:
        raise HTTPException(status_code=404, detail="Invalid track_id")
    raw_layout = layout if layout is not None else ""
    if raw_layout and (".." in raw_layout or "/" in raw_layout or "\\" in raw_layout):
        raise HTTPException(status_code=404, detail="Invalid layout")
    layout_norm = None if (raw_layout or "").strip().lower() in ("", "default") else raw_layout.strip()
    path = _find_track_map(track_id.strip(), layout_norm)
    if path is None or not path.is_file():
        raise HTTPException(status_code=404, detail="Track map not found")
    return _image_response(path, "image/png")


@router.get("/tracks/{track_id}/layouts/{layout}/base")
async def get_track_layout_base(track_id: str, layout: str):
    """Serve base track image (outline/preview, no map) for use under map overlay. Falls back to full preview if no base found."""
    if ".." in track_id or "/" in track_id or "\\" in track_id:
        raise HTTPException(status_code=404, detail="Invalid track_id")
    raw_layout = layout if layout is not None else ""
    if raw_layout and (".." in raw_layout or "/" in raw_layout or "\\" in raw_layout):
        raise HTTPException(status_code=404, detail="Invalid layout")
    layout_norm = None if (raw_layout or "").strip().lower() in ("", "default") else raw_layout.strip()
    path = _find_track_base(track_id.strip(), layout_norm)
    if path is None or not path.is_file():
        path = _find_track_preview(track_id, layout_norm)
    if path is None or not path.is_file():
        path = _find_track_outline(track_id, layout_norm)
    if path is None or not path.is_file():
        raise HTTPException(status_code=404, detail="Track base image not found")
    media_type = "image/jpeg" if path.suffix.lower() in (".jpg", ".jpeg") else "image/png"
    return _image_response(path, media_type)


from controller.api_server_config_routes import (
    ServerInstance,
    _ac_server_start,
    _ac_server_stop,
    _get_running_servers_list,
    _read_ports_from_preset,
    _running_servers,
    _running_servers_lock,
    _server_root_for_ac_server,
    router as _server_config_router,
)

router.include_router(_server_config_router)

from controller.api_logs_pool_routes import router as _logs_pool_router

router.include_router(_logs_pool_router)

# Re-export for tests that patch ``controller.api_routes.pool_manager``.
from controller.server_pool import pool_manager  # noqa: F401

# Self-test for _load_json_with_fallback (run: python -m controller.api_routes from repo root; does not run in production)
if __name__ == "__main__":
    import tempfile
    td = Path(tempfile.gettempdir())
    # UTF-8
    p1 = td / "pitbox_selftest_utf8.json"
    p1.write_text('{"name": "Test"}', encoding="utf-8")
    assert _load_json_with_fallback(p1) == {"name": "Test"}
    p1.unlink(missing_ok=True)
    # cp1252 byte (e.g. degree symbol 0xb0) that fails utf-8
    p2 = td / "pitbox_selftest_cp1252.json"
    p2.write_bytes(b'{"name": "Test \xb0"}')
    data = _load_json_with_fallback(p2)
    assert data is not None and data.get("name") == "Test °"
    p2.unlink(missing_ok=True)
    # Missing file
    assert _load_json_with_fallback(td / "nonexistent_pitbox_selftest.json") is None
    print("_load_json_with_fallback self-test: ok")
