import os
import json
import time
import math
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple

import requests

logger = logging.getLogger("WHALE_ENGINE")

# =========================================================
# ENV HELPERS
# =========================================================

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _hash32(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]


# =========================================================
# ENV CONFIG
# =========================================================

WHALE_ENABLED = _env_bool("WHALE_ENABLED", True)
WHALE_CHAT_ID = os.getenv("WHALE_CHAT_ID", "").strip()

WHALE_TOPN = _env_int("WHALE_TOPN", 250)

WHALE_L1_MIN_PCT = _env_float("WHALE_L1_MIN_PCT", 0.80)
WHALE_L1_MAX_PCT = _env_float("WHALE_L1_MAX_PCT", 5.00)
WHALE_L1_MIN_VOL_SPIKE = _env_float("WHALE_L1_MIN_VOL_SPIKE", 1.40)

WHALE_SCORE_MIN = _env_float("WHALE_SCORE_MIN", 8.50)

WHALE_COOLDOWN_MIN = _env_int("WHALE_COOLDOWN_MIN", 45)
WHALE_MAX_ALERTS_PER_SCAN = _env_int("WHALE_MAX_ALERTS_PER_SCAN", 2)

WHALE_WATCHLIST_ONLY = _env_bool("WHALE_WATCHLIST_ONLY", False)
WHALE_LATE_PCT_BLOCK = _env_float("WHALE_LATE_PCT_BLOCK", 4.50)

WHALE_DRY_RUN = _env_bool("WHALE_DRY_RUN", False)

TV_SCAN_URL = os.getenv("WHALE_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan")
TV_TIMEOUT = _env_int("WHALE_TV_TIMEOUT", 12)

DATA_DIR = os.getenv("DATA_DIR", "/var/data")
STATE_FILE = os.path.join(DATA_DIR, "whale_engine_state.json")
LAST_ALERT_FILE = os.path.join(DATA_DIR, "whale_engine_last_alert.json")
WATCHLIST_FILE = os.path.join(DATA_DIR, "momo_prime_watchlist.json")


# =========================================================
# UTIL
# =========================================================

def _load_json(path: str, default: dict) -> dict:
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or default
    except Exception:
        return default


def _save_json(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _norm(sym: str) -> str:
    s = (sym or "").strip().upper()
    if ":" in s:
        s = s.split(":")[-1]
    if s.endswith(".IS"):
        s = s[:-3]
    return s


# =========================================================
# TV SCAN
# =========================================================

def _tv_scan_topn(topn: int) -> List[Dict[str, Any]]:
    payload = {
        "filter": [
            {"left": "volume", "operation": "nempty"},
            {"left": "change", "operation": "nempty"},
            {"left": "close", "operation": "nempty"},
        ],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "change", "volume", "close", "average_volume_10d_calc"],
        "sort": {"sortBy": "volume", "sortOrder": "desc"},
        "range": [0, max(0, topn - 1)],
    }

    try:
        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
        r.raise_for_status()
        js = r.json() or {}
    except Exception:
        return []

    out = []
    for row in js.get("data") or []:
        d = row.get("d") or []
        if len(d) < 5:
            continue

        sym = _norm(str(d[0]))
        pct = _safe_float(d[1])
        vol = _safe_float(d[2])
        last = _safe_float(d[3])
        av10 = _safe_float(d[4])

        if not sym or pct is None or vol is None or last is None:
            continue

        vol_spike = None
        if av10 and av10 > 0:
            vol_spike = vol / av10

        out.append(
            {
                "symbol": sym,
                "pct": float(pct),
                "last": float(last),
                "vol_spike": float(vol_spike) if vol_spike else None,
            }
        )

    return out


# =========================================================
# SCORE
# =========================================================

def _score(row: Dict[str, Any]) -> float:
    pct = float(row.get("pct") or 0)
    vs = float(row.get("vol_spike") or 0)

    s = 0
    s += min(vs, 3.0) * 2.0
    s += min(pct, 3.0) / 3.0 * 2.0

    if 0.50 <= pct <= 3.50:
        s += 2.0

    return s


# =========================================================
# PRO 3 SATIR FORMAT
# =========================================================

def _format_pro(row: Dict[str, Any], score: float) -> str:
    sym = row["symbol"]
    pct = row["pct"]
    last = row["last"]
    vs = row.get("vol_spike")

    status = "WARM"
    if score >= 9.5:
        status = "STRONG"

    line1 = f"🐳 WHALE | {sym} | {pct:+.2f}% | {vs:.2f}x | S={score:.1f}"
    line2 = f"⏱ {datetime.now().strftime('%H:%M')} | Durum: {status}"
    line3 = "🧭 Aksiyon: Steady teyidi bekle"

    if WHALE_DRY_RUN:
        line1 = "DRY-RUN " + line1

    return "\n".join([line1, line2, line3])


# =========================================================
# WATCHLIST
# =========================================================

def _watchlist_add(sym: str):
    d = _load_json(WATCHLIST_FILE, {"symbols": []})
    syms = d.get("symbols") or []
    sym = _norm(sym)

    if sym in syms:
        syms.remove(sym)

    syms.insert(0, sym)
    d["symbols"] = syms[:180]
    d["updated_utc"] = _utc_now_iso()
    _save_json(WATCHLIST_FILE, d)


# =========================================================
# MAIN JOB
# =========================================================

async def job_whale_engine_scan(context):

    if not WHALE_ENABLED:
        return

    rows = _tv_scan_topn(WHALE_TOPN)
    if not rows:
        return

    last_state = _load_json(LAST_ALERT_FILE, {"last": {}})
    last_map = last_state.get("last") or {}

    candidates = []

    for r in rows:

        pct = r["pct"]
        vs = r.get("vol_spike")

        if pct < WHALE_L1_MIN_PCT or pct > WHALE_L1_MAX_PCT:
            continue

        if not vs or vs < WHALE_L1_MIN_VOL_SPIKE:
            continue

        if pct >= WHALE_LATE_PCT_BLOCK:
            continue  # late pump block

        s = _score(r)
        if s >= WHALE_SCORE_MIN:
            candidates.append((r["symbol"], s, r))

    if not candidates:
        return

    candidates.sort(key=lambda x: x[1], reverse=True)
    candidates = candidates[:WHALE_MAX_ALERTS_PER_SCAN]

    sent = 0

    for sym, s, r in candidates:

        last_entry = last_map.get(sym) or {}
        last_time = last_entry.get("t")

        if last_time:
            dt = datetime.fromisoformat(last_time.replace("Z", "+00:00"))
            if (time.time() - dt.timestamp()) < WHALE_COOLDOWN_MIN * 60:
                continue

        _watchlist_add(sym)

        if WHALE_WATCHLIST_ONLY:
            continue

        msg = _format_pro(r, s)
        h = _hash32(msg)

        if last_entry.get("h") == h:
            continue

        try:
            await context.application.bot_data["telegram_send"](
                context,
                WHALE_CHAT_ID,
                msg,
                disable_web_page_preview=True,
            )
            sent += 1
        except Exception:
            continue

        last_map[sym] = {
            "t": _utc_now_iso(),
            "h": h,
            "s": float(s),
        }

    last_state["last"] = last_map
    _save_json(LAST_ALERT_FILE, last_state)

    logger.info("WHALE_ENGINE sent=%d", sent)
