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
# ENV helpers
# =========================================================
def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name, "")
    if val == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


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


def _load_json(path: str, default: dict) -> dict:
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or default
    except Exception as e:
        logger.warning("WHALE load_json error: %s", e)
        return default


def _save_json(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning("WHALE save_json error: %s", e)


# =========================================================
# ENV
# =========================================================
WHALE_ENABLED = _env_bool("WHALE_ENABLED", True)
WHALE_CHAT_ID = os.getenv("WHALE_CHAT_ID", "").strip()

# Force bypass market gate (only for testing)
WHALE_FORCE = _env_bool("WHALE_FORCE", False)

WHALE_INTERVAL_MIN = _env_int("WHALE_INTERVAL_MIN", 2)
WHALE_TOPN = _env_int("WHALE_TOPN", 250)

# Layer 1: TV TOPN (geniş tarama)
WHALE_L1_MIN_PCT = _env_float("WHALE_L1_MIN_PCT", 0.80)
WHALE_L1_MAX_PCT = _env_float("WHALE_L1_MAX_PCT", 5.00)
WHALE_L1_MIN_VOL_SPIKE = _env_float("WHALE_L1_MIN_VOL_SPIKE", 1.40)
WHALE_L1_MIN_STEADY = _env_float("WHALE_L1_MIN_STEADY", 0.65)

# Layer 2: UNIVERSE (kontrollü evren)
WHALE_L2_MIN_PCT = _env_float("WHALE_L2_MIN_PCT", 0.60)
WHALE_L2_MAX_PCT = _env_float("WHALE_L2_MAX_PCT", 3.50)
WHALE_L2_MIN_VOL_SPIKE = _env_float("WHALE_L2_MIN_VOL_SPIKE", 1.20)
WHALE_L2_MIN_STEADY = _env_float("WHALE_L2_MIN_STEADY", 0.70)

# Early Accum (stealth)
WHALE_EARLY_ACCUM = _env_bool("WHALE_EARLY_ACCUM", False)
WHALE_EARLY_ACCUM_PCT_MIN = _env_float("WHALE_EARLY_ACCUM_PCT_MIN", 0.35)
WHALE_EARLY_ACCUM_PCT_MAX = _env_float("WHALE_EARLY_ACCUM_PCT_MAX", 1.20)
WHALE_EARLY_ACCUM_VOL = _env_float("WHALE_EARLY_ACCUM_VOL", 1.08)
WHALE_EARLY_ACCUM_STEADY = _env_float("WHALE_EARLY_ACCUM_STEADY", 0.80)

# Score / bonus
WHALE_SCORE_MIN = _env_float("WHALE_SCORE_MIN", 8.50)
WHALE_CONT_BONUS_2 = _env_float("WHALE_CONT_BONUS_2", 0.50)
WHALE_CONT_BONUS_3 = _env_float("WHALE_CONT_BONUS_3", 1.00)

# Spam control
WHALE_COOLDOWN_MIN = _env_int("WHALE_COOLDOWN_MIN", 45)
WHALE_MAX_ALERTS_PER_SCAN = _env_int("WHALE_MAX_ALERTS_PER_SCAN", 2)

# Message format
# 3 lines max: 1) header 2) metrics 3) mentor hint
WHALE_MSG_MAX_LINES = _env_int("WHALE_MSG_MAX_LINES", 3)

# TradingView
TV_SCAN_URL = os.getenv("WHALE_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = _env_int("WHALE_TV_TIMEOUT", 12)

# Universe tickers (env)
UNIVERSE_TICKERS = os.getenv("UNIVERSE_TICKERS", "").strip()
if not UNIVERSE_TICKERS:
    UNIVERSE_TICKERS = os.getenv("BIST200_TICKERS", "").strip()

# Dry-run (seans kapalı test)
WHALE_DRY_RUN = _env_bool("WHALE_DRY_RUN", False)
WHALE_DRY_RUN_TAG = _env_bool("WHALE_DRY_RUN_TAG", True)

# Data dir / state
DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
WHALE_STATE_FILE = os.path.join(DATA_DIR, "whale_engine_state.json")
WHALE_LAST_ALERT_FILE = os.path.join(DATA_DIR, "whale_engine_last_alert.json")

# KILIT watchlist integration (MOMO PRIME)
PRIME_WATCHLIST_FILE = os.path.join(DATA_DIR, "momo_prime_watchlist.json")
PRIME_WATCHLIST_MAX = _env_int("PRIME_WATCHLIST_MAX", 180)

# Debug logging
WHALE_DEBUG_LOG = _env_bool("WHALE_DEBUG_LOG", True)

# Patch: Scan log controls (Render ENV)
WHALE_LOG_SCAN = _env_bool("WHALE_LOG_SCAN", True)
WHALE_LOG_ROWS = _env_bool("WHALE_LOG_ROWS", False)

# =========================================================
# Secret filter (PRO)
# =========================================================
WHALE_SECRET_FILTER = _env_bool("WHALE_SECRET_FILTER", False)
WHALE_SECRET_MIN_CONT = _env_int("WHALE_SECRET_MIN_CONT", 2)
WHALE_SECRET_MIN_VS = _env_float("WHALE_SECRET_MIN_VS", 1.25)
WHALE_SECRET_MIN_VS_DELTA = _env_float("WHALE_SECRET_MIN_VS_DELTA", 0.05)
WHALE_SECRET_MIN_PCT_DELTA = _env_float("WHALE_SECRET_MIN_PCT_DELTA", 0.10)
WHALE_SECRET_REJECT_DECAY = _env_bool("WHALE_SECRET_REJECT_DECAY", True)

# Startup log (one-time visibility)
if WHALE_DEBUG_LOG:
    logger.info(
        "WHALE EARLY_ACCUM enabled=%s pct=[%s,%s] vol>=%s steady>=%s",
        WHALE_EARLY_ACCUM,
        WHALE_EARLY_ACCUM_PCT_MIN,
        WHALE_EARLY_ACCUM_PCT_MAX,
        WHALE_EARLY_ACCUM_VOL,
        WHALE_EARLY_ACCUM_STEADY,
    )
    logger.info(
        "WHALE SECRET_FILTER enabled=%s min_cont=%s min_vs=%s d_vs=%s d_pct=%s reject_decay=%s",
        WHALE_SECRET_FILTER,
        WHALE_SECRET_MIN_CONT,
        WHALE_SECRET_MIN_VS,
        WHALE_SECRET_MIN_VS_DELTA,
        WHALE_SECRET_MIN_PCT_DELTA,
        WHALE_SECRET_REJECT_DECAY,
    )


# =========================================================
# State defaults
# =========================================================
def _default_whale_state() -> dict:
    return {
        "schema_version": "1.2",
        "system": "whale_engine",
        "scan": {"last_scan_utc": None},
        "continuity": {},
        "prev": {},  # last metrics by symbol (for secret filter)
    }


def _default_last_alert() -> dict:
    return {
        "schema_version": "1.0",
        "system": "whale_engine",
        "cooldown_min": WHALE_COOLDOWN_MIN,
        "last_alert_by_symbol": {},
    }


# =========================================================
# Watchlist (for KILIT)
# =========================================================
def _wl_default() -> dict:
    return {"schema_version": "1.0", "system": "momo_prime_watchlist", "updated_utc": None, "symbols": []}


def _norm(sym: str) -> str:
    s = (sym or "").strip().upper()
    if ":" in s:
        s = s.split(":")[-1].strip()
    if s.endswith(".IS"):
        s = s[:-3]
    return s


def _wl_load() -> dict:
    try:
        if not os.path.exists(PRIME_WATCHLIST_FILE):
            return _wl_default()
        with open(PRIME_WATCHLIST_FILE, "r", encoding="utf-8") as f:
            d = json.load(f) or {}
        if "symbols" not in d:
            d["symbols"] = []
        return d
    except Exception as e:
        logger.warning("WHALE watchlist load error: %s", e)
        return _wl_default()


def _wl_save(d: dict) -> None:
    try:
        os.makedirs(os.path.dirname(PRIME_WATCHLIST_FILE), exist_ok=True)
        tmp = PRIME_WATCHLIST_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
        os.replace(tmp, PRIME_WATCHLIST_FILE)
    except Exception as e:
        logger.warning("WHALE watchlist save error: %s", e)


def prime_watchlist_add(symbol: str) -> None:
    s = _norm(symbol)
    if not s:
        return
    d = _wl_load()

    syms = [_norm(x) for x in (d.get("symbols") or [])]
    syms = [x for x in syms if x]

    if s in syms:
        syms = [x for x in syms if x != s]
    syms.insert(0, s)

    if len(syms) > PRIME_WATCHLIST_MAX:
        syms = syms[:PRIME_WATCHLIST_MAX]

    d["symbols"] = syms
    d["updated_utc"] = _utc_now_iso()
    _wl_save(d)


# =========================================================
# TradingView scan
# =========================================================
def _tv_scan(payload: dict, tag: str) -> List[Dict[str, Any]]:
    try:
        if WHALE_DEBUG_LOG and WHALE_LOG_SCAN:
            logger.info("WHALE TV SCAN START tag=%s url=%s timeout=%s", tag, TV_SCAN_URL, TV_TIMEOUT)

        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)

        if WHALE_DEBUG_LOG and WHALE_LOG_SCAN:
            logger.info("WHALE TV SCAN HTTP tag=%s status=%s", tag, r.status_code)

        r.raise_for_status()
        js = r.json() or {}

        data = js.get("data") or []
        if WHALE_DEBUG_LOG and WHALE_LOG_SCAN:
            logger.info("WHALE TV SCAN PARSED tag=%s data_len=%s", tag, len(data))

        out: List[Dict[str, Any]] = []
        for row in data:
            d = row.get("d") or []
            if len(d) < 4:
                continue

            sym = str(d[0]).strip().upper()
            pct = _safe_float(d[1])
            vol = _safe_float(d[2])
            last = _safe_float(d[3])
            av10 = _safe_float(d[4]) if len(d) >= 5 else None

            if not sym or pct is None or vol is None or last is None:
                continue

            sym_norm = _norm(sym)

            vol_spike_10g = None
            if av10 is not None and av10 > 0:
                vol_spike_10g = vol / av10

            out.append(
                {
                    "symbol_raw": sym,
                    "symbol": sym_norm,
                    "pct": float(pct),
                    "volume": float(vol),
                    "last": float(last),
                    "av10": float(av10) if av10 is not None else None,
                    "vol_spike_10g": float(vol_spike_10g) if vol_spike_10g is not None else None,
                }
            )

        if WHALE_DEBUG_LOG and WHALE_LOG_SCAN:
            logger.info("WHALE TV SCAN OUT tag=%s out_len=%s", tag, len(out))

        if WHALE_LOG_ROWS and WHALE_DEBUG_LOG:
            for rr in out[:60]:
                logger.info(
                    "TVROW %s pct=%s vol=%s av10=%s v10=%s last=%s",
                    rr.get("symbol"),
                    rr.get("pct"),
                    rr.get("volume"),
                    rr.get("av10"),
                    rr.get("vol_spike_10g"),
                    rr.get("last"),
                )

        return out
    except Exception as e:
        logger.warning("WHALE TV SCAN ERROR tag=%s err=%s", tag, e)
        return []


def _tv_topn_rows(topn: int) -> List[Dict[str, Any]]:
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
        "range": [0, max(0, int(topn) - 1)],
    }
    return _tv_scan(payload, tag=f"TOPN:{topn}")


def _tv_universe_rows(tickers: List[str]) -> List[Dict[str, Any]]:
    t: List[str] = []
    for x in tickers:
        x = (x or "").strip().upper()
        if not x:
            continue
        norm = _norm(x)
        if norm:
            t.append(f"BIST:{norm}")

    if not t:
        return []

    payload = {
        "filter": [
            {"left": "volume", "operation": "nempty"},
            {"left": "change", "operation": "nempty"},
            {"left": "close", "operation": "nempty"},
        ],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": t},
        "columns": ["name", "change", "volume", "close", "average_volume_10d_calc"],
        "sort": {"sortBy": "volume", "sortOrder": "desc"},
        "range": [0, max(0, len(t) - 1)],
    }
    return _tv_scan(payload, tag=f"UNIVERSE:{len(t)}")


def _parse_universe_env(raw: str) -> List[str]:
    raw = (raw or "").strip()
    if not raw:
        return []

    parts = [p.strip() for p in raw.replace("\n", ",").split(",") if p.strip()]
    out: List[str] = []
    seen: set = set()

    for p in parts:
        norm = _norm(p)
        if not norm:
            continue
        if norm in seen:
            continue
        seen.add(norm)
        out.append(norm)

    return out


# =========================================================
# Steady proxy + filters (WHALE = ham erken uyarı havuzu)
# =========================================================
def _steady_proxy(pct: float, vol_spike_10g: Optional[float]) -> float:
    s = 0.50
    if 0.30 <= pct <= 3.50:
        s += 0.15
    if vol_spike_10g is not None and vol_spike_10g >= 1.05:
        s += 0.15
    if pct <= 2.20:
        s += 0.10
    return max(0.0, min(1.0, s))


def _passes_layer1(row: Dict[str, Any]) -> bool:
    pct = float(row.get("pct") or 0.0)
    vs = _safe_float(row.get("vol_spike_10g"))
    sp = _steady_proxy(pct, vs)

    if pct < WHALE_L1_MIN_PCT or pct > WHALE_L1_MAX_PCT:
        return False
    if vs is None or vs < WHALE_L1_MIN_VOL_SPIKE:
        return False
    if sp < WHALE_L1_MIN_STEADY:
        return False
    return True


def _passes_layer2(row: Dict[str, Any]) -> bool:
    pct = float(row.get("pct") or 0.0)
    vs = _safe_float(row.get("vol_spike_10g"))
    sp = _steady_proxy(pct, vs)

    if pct < WHALE_L2_MIN_PCT or pct > WHALE_L2_MAX_PCT:
        return False
    if vs is None or vs < WHALE_L2_MIN_VOL_SPIKE:
        return False
    if sp < WHALE_L2_MIN_STEADY:
        return False
    return True


def _passes_early_accum(row: Dict[str, Any]) -> bool:
    if not WHALE_EARLY_ACCUM:
        return False

    pct = float(row.get("pct") or 0.0)
    vs = _safe_float(row.get("vol_spike_10g"))
    sp = _steady_proxy(pct, vs)

    if pct < WHALE_EARLY_ACCUM_PCT_MIN or pct > WHALE_EARLY_ACCUM_PCT_MAX:
        return False
    if vs is None or vs < WHALE_EARLY_ACCUM_VOL:
        return False
    if sp < WHALE_EARLY_ACCUM_STEADY:
        return False
    return True


# =========================================================
# Scoring + continuity
# =========================================================
def _score(row: Dict[str, Any], layer: str, cont_count: int) -> float:
    pct = float(row.get("pct") or 0.0)
    vs = float(row.get("vol_spike_10g") or 0.0)
    sp = _steady_proxy(pct, _safe_float(vs))

    s = 0.0
    s += sp * 6.0
    s += min(vs, 3.0) * 2.0

    pct_part = 0.0 if pct <= 0 else min(pct, 3.0) / 3.0
    s += pct_part * 2.0

    # Layer bonus
    if layer == "L2":
        s += 0.70
    elif layer == "E":
        s += 0.55
    else:
        s += 0.40

    if cont_count >= 3:
        s += WHALE_CONT_BONUS_3
    elif cont_count >= 2:
        s += WHALE_CONT_BONUS_2

    return float(s)


def _continuity_update(state: dict, symbols_seen: List[str]) -> Dict[str, int]:
    cont = (state.get("continuity") or {})
    now_utc = _utc_now_iso()

    seen_set = set([_norm(x) for x in symbols_seen if _norm(x)])
    out_counts: Dict[str, int] = {}

    for sym in seen_set:
        cur = cont.get(sym) or {}
        prev_count = int(cur.get("count") or 0)
        new_count = prev_count + 1
        cont[sym] = {"count": new_count, "last_seen_utc": now_utc}
        out_counts[sym] = new_count

    # prune old (>48h)
    try:
        now_ts = time.time()
        drop = []
        for sym, cur in cont.items():
            last = (cur.get("last_seen_utc") or "")
            if not last:
                continue
            try:
                dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
                if (now_ts - dt.timestamp()) > (2 * 24 * 3600):
                    drop.append(sym)
            except Exception:
                continue
        for sym in drop:
            cont.pop(sym, None)
    except Exception:
        pass

    state["continuity"] = cont
    return out_counts


# =========================================================
# Secret filter (PRO) - continuity + improvement gate
# NOTE: Must compare against previous scan metrics BEFORE updating them.
# =========================================================
def _secret_filter_pass(prev_map: dict, row: Dict[str, Any], cont_count: int) -> bool:
    if not WHALE_SECRET_FILTER:
        return True

    if cont_count < WHALE_SECRET_MIN_CONT:
        return False

    sym = _norm(row.get("symbol") or "")
    if not sym:
        return False

    pct = float(row.get("pct") or 0.0)
    vs = _safe_float(row.get("vol_spike_10g"))
    if vs is None:
        return False

    if vs < WHALE_SECRET_MIN_VS:
        return False

    prev = (prev_map or {}).get(sym) or {}
    prev_pct = _safe_float(prev.get("pct"))
    prev_vs = _safe_float(prev.get("vs"))

    # After restart, prev may be missing. cont>=2 already gives some confirmation.
    if prev_pct is None or prev_vs is None:
        return True

    d_pct = pct - prev_pct
    d_vs = vs - prev_vs

    # Must improve either price or volume-spike since previous scan
    if d_pct < WHALE_SECRET_MIN_PCT_DELTA and d_vs < WHALE_SECRET_MIN_VS_DELTA:
        return False

    # Reject decay: if volume-spike is dropping, skip (often fake)
    if WHALE_SECRET_REJECT_DECAY and d_vs < 0:
        return False

    return True


# =========================================================
# Alert decision (cooldown + hash)
# =========================================================
def _cooldown_ok(last_utc: Optional[str]) -> bool:
    if not last_utc:
        return True
    try:
        dt = datetime.fromisoformat(last_utc.replace("Z", "+00:00"))
        return (time.time() - dt.timestamp()) >= (WHALE_COOLDOWN_MIN * 60)
    except Exception:
        return True


def _format_message_3lines(row: Dict[str, Any], layer: str, score: float, cont_count: int) -> str:
    sym = (row.get("symbol") or "?").strip()
    pct = float(row.get("pct") or 0.0)
    last = row.get("last")
    vs = _safe_float(row.get("vol_spike_10g"))
    sp = _steady_proxy(pct, vs)

    def fnum(x: Any, nd: int = 2) -> str:
        try:
            return f"{float(x):.{nd}f}"
        except Exception:
            return "n/a"

    # Level
    if score >= 10.5:
        tag = "KILIT"
        trust = "Y"
    elif score >= 9.5:
        tag = "RADAR"
        trust = "O-Y"
    else:
        tag = "IZLE"
        trust = "O"

    # Mentor B favourisi: pullback + teyit
    teyit_ok = (score >= 9.5 and cont_count >= 2 and (vs is not None and vs >= 1.30))

    if teyit_ok:
        hint = "Mentor: Pullback + kırılım = giriş | Skor düşmezse koru"
    else:
        hint = "Mentor: Teyit bekle (Skor≥9.5 & Cont≥2 & Vol≥1.30x)"

    prefix = "DRY " if (WHALE_DRY_RUN and WHALE_DRY_RUN_TAG) else ""

    layer_tag = "E" if layer == "E" else layer[-1]
    line1 = f"🐳 {prefix}WHALE {tag}({trust}) {sym} L{layer_tag} Cont:{int(cont_count)}"
    line2 = f"F:{fnum(last, 2)}  %:{pct:+.2f}  V10:{fnum(vs, 2)}x  Steady:{fnum(sp, 2)}  S:{fnum(score, 2)}"
    line3 = hint

    lines = [line1, line2, line3]
    return "\n".join(lines[: max(1, int(WHALE_MSG_MAX_LINES))])


# =========================================================
# PUBLIC JOB (main.py will schedule this)
# Requires adapters in bot_data:
#  - bist_session_open(): bool
#  - telegram_send(ctx, chat_id, text, **kwargs) async
# =========================================================
async def job_whale_engine_scan(context) -> None:
    if not WHALE_ENABLED:
        return
    if not WHALE_CHAT_ID:
        logger.warning("WHALE_ENGINE: missing WHALE_CHAT_ID")
        return

    app = getattr(context, "application", None)
    bot_data = getattr(app, "bot_data", {}) if app else {}

    bist_open_fn = bot_data.get("bist_session_open")
    telegram_send = bot_data.get("telegram_send")

    if not telegram_send:
        logger.warning("WHALE_ENGINE: missing telegram_send adapter")
        return

    # Market gate
    if (not WHALE_FORCE) and (not WHALE_DRY_RUN):
        try:
            if bist_open_fn and (not bist_open_fn()):
                if WHALE_DEBUG_LOG and WHALE_LOG_SCAN:
                    logger.info(
                        "WHALE wrapper exit: market closed (force=%s dry_run=%s)",
                        WHALE_FORCE,
                        WHALE_DRY_RUN,
                    )
                return
        except Exception:
            pass

    st = _load_json(WHALE_STATE_FILE, _default_whale_state())
    prev_map_before = dict(st.get("prev") or {})

    la = _load_json(WHALE_LAST_ALERT_FILE, _default_last_alert())
    last_map = la.get("last_alert_by_symbol") or {}

    # Layer 1 (ham erken uyarı havuzu)
    l1_rows = _tv_topn_rows(WHALE_TOPN)
    if WHALE_LOG_SCAN and WHALE_DEBUG_LOG:
        logger.info("WHALE rows_len TOPN=%s -> %s", WHALE_TOPN, len(l1_rows))

    l1_candidates = [r for r in l1_rows if _passes_layer1(r)]
    if WHALE_LOG_SCAN and WHALE_DEBUG_LOG:
        logger.info("WHALE cand_len L1=%s", len(l1_candidates))

    early_candidates = [r for r in l1_rows if _passes_early_accum(r)]
    if WHALE_LOG_SCAN and WHALE_DEBUG_LOG:
        logger.info("WHALE cand_len EARLY=%s", len(early_candidates))

    # Layer 2 (kontrollü evren)
    universe = _parse_universe_env(UNIVERSE_TICKERS)
    l2_rows = _tv_universe_rows(universe) if universe else []
    if WHALE_LOG_SCAN and WHALE_DEBUG_LOG:
        logger.info("WHALE rows_len UNIVERSE=%s -> %s", len(universe), len(l2_rows))

    l2_candidates = [r for r in l2_rows if _passes_layer2(r)]
    if WHALE_LOG_SCAN and WHALE_DEBUG_LOG:
        logger.info("WHALE cand_len L2=%s", len(l2_candidates))

    # Merge (L2 overwrite, EARLY fills gaps)
    merged: Dict[str, Dict[str, Any]] = {}
    for r in l1_candidates:
        merged[r["symbol"]] = dict(r)
        merged[r["symbol"]]["layer"] = "L1"
    for r in l2_candidates:
        merged[r["symbol"]] = dict(r)
        merged[r["symbol"]]["layer"] = "L2"
    for r in early_candidates:
        sym = r["symbol"]
        if sym not in merged:
            merged[sym] = dict(r)
            merged[sym]["layer"] = "E"

    if not merged:
        if WHALE_LOG_SCAN and WHALE_DEBUG_LOG:
            logger.info("WHALE_ENGINE: no candidates")
        st["scan"]["last_scan_utc"] = _utc_now_iso()
        _save_json(WHALE_STATE_FILE, st)
        return

    symbols_seen = list(merged.keys())
    cont_counts = _continuity_update(st, symbols_seen)

    scored: List[Tuple[str, float, Dict[str, Any]]] = []
    for sym, r in merged.items():
        layer = r.get("layer") or "L1"
        cc = int(cont_counts.get(sym) or 1)
        s = _score(r, layer, cc)
        r["score"] = s
        r["cont"] = cc

        if s >= WHALE_SCORE_MIN and _secret_filter_pass(prev_map_before, r, cc):
            scored.append((sym, s, r))

    # Update prev metrics AFTER scoring/filtering (so deltas work next scan)
    now_utc = _utc_now_iso()
    prev_map_after = dict(prev_map_before)
    for sym, r in merged.items():
        vs = _safe_float(r.get("vol_spike_10g"))
        prev_map_after[sym] = {
            "pct": float(r.get("pct") or 0.0),
            "vs": float(vs) if vs is not None else None,
            "utc": now_utc,
        }
    st["prev"] = prev_map_after
    st["scan"]["last_scan_utc"] = now_utc
    _save_json(WHALE_STATE_FILE, st)

    if not scored:
        if WHALE_LOG_SCAN and WHALE_DEBUG_LOG:
            logger.info("WHALE_ENGINE: no alerts (score below min or secret filter)")
        return

    scored.sort(key=lambda x: x[1], reverse=True)
    scored = scored[: max(1, int(WHALE_MAX_ALERTS_PER_SCAN))]

    sent = 0
    for sym, s, r in scored:
        entry = last_map.get(sym) or {}
        if not _cooldown_ok(entry.get("last_alert_utc")):
            continue

        msg = _format_message_3lines(r, r.get("layer") or "L1", s, int(r.get("cont") or 1))
        mh = _hash32(msg)
        if entry.get("last_hash") == mh:
            continue

        try:
            await telegram_send(
                context,
                WHALE_CHAT_ID,
                msg,
                disable_web_page_preview=True,
            )
            sent += 1
        except Exception as e:
            logger.warning("WHALE_ENGINE: send error: %s", e)
            continue

        last_map[sym] = {
            "last_alert_utc": _utc_now_iso(),
            "last_hash": mh,
            "last_score": float(s),
        }

        # KILIT pipeline: whale sinyali gelince izleme listesine al
        try:
            prime_watchlist_add(sym)
        except Exception:
            pass

    la["last_alert_by_symbol"] = last_map
    _save_json(WHALE_LAST_ALERT_FILE, la)

    logger.info("WHALE_ENGINE: sent=%d", sent)
