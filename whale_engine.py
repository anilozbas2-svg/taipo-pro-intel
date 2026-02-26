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

# ==========================
# ENV helpers
# ==========================
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


# ==========================
# ENV
# ==========================
WHALE_ENABLED = _env_bool("WHALE_ENABLED", True)
WHALE_CHAT_ID = os.getenv("WHALE_CHAT_ID", "").strip()

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

# Score / bonus
WHALE_SCORE_MIN = _env_float("WHALE_SCORE_MIN", 8.50)
WHALE_CONT_BONUS_2 = _env_float("WHALE_CONT_BONUS_2", 0.50)
WHALE_CONT_BONUS_3 = _env_float("WHALE_CONT_BONUS_3", 1.00)

# Spam control
WHALE_COOLDOWN_MIN = _env_int("WHALE_COOLDOWN_MIN", 45)
WHALE_MAX_ALERTS_PER_SCAN = _env_int("WHALE_MAX_ALERTS_PER_SCAN", 2)

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

# KILIT watchlist integration
PRIME_WATCHLIST_FILE = os.path.join(DATA_DIR, "momo_prime_watchlist.json")
PRIME_WATCHLIST_MAX = _env_int("PRIME_WATCHLIST_MAX", 180)


# ==========================
# State defaults
# ==========================
def _default_whale_state() -> dict:
    return {
        "schema_version": "1.0",
        "system": "whale_engine",
        "scan": {"last_scan_utc": None},
        "continuity": {},
    }


def _default_last_alert() -> dict:
    return {
        "schema_version": "1.0",
        "system": "whale_engine",
        "cooldown_min": WHALE_COOLDOWN_MIN,
        "last_alert_by_symbol": {},
    }


# ==========================
# Watchlist (for KILIT)
# ==========================
def _wl_default() -> dict:
    return {"schema_version": "1.0", "system": "momo_prime_watchlist", "updated_utc": None, "symbols": []}


def _wl_norm(sym: str) -> str:
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
    s = _wl_norm(symbol)
    if not s:
        return
    d = _wl_load()

    syms = [_wl_norm(x) for x in (d.get("symbols") or [])]
    syms = [x for x in syms if x]

    if s in syms:
        syms = [x for x in syms if x != s]
    syms.insert(0, s)

    if len(syms) > PRIME_WATCHLIST_MAX:
        syms = syms[:PRIME_WATCHLIST_MAX]

    d["symbols"] = syms
    d["updated_utc"] = _utc_now_iso()
    _wl_save(d)


# ==========================
# TradingView scan helpers
# ==========================
def _tv_scan(payload: dict) -> List[Dict[str, Any]]:
    try:
        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
        r.raise_for_status()
        js = r.json() or {}

        out: List[Dict[str, Any]] = []
        for row in (js.get("data") or []):
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

            sym_norm = _wl_norm(sym)

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
        return out
    except Exception as e:
        logger.warning("WHALE TV scan error: %s", e)
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
    return _tv_scan(payload)


def _tv_universe_rows(tickers: List[str]) -> List[Dict[str, Any]]:
    t: List[str] = []
    for x in tickers:
        x = (x or "").strip().upper()
        if not x:
            continue
        norm = _wl_norm(x)
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
    return _tv_scan(payload)


def _parse_universe_env(raw: str) -> List[str]:
    raw = (raw or "").strip()
    if not raw:
        return []

    parts = [p.strip() for p in raw.replace("\n", ",").split(",") if p.strip()]
    out: List[str] = []
    seen: set = set()

    for p in parts:
        norm = _wl_norm(p)
        if not norm:
            continue
        if norm in seen:
            continue
        seen.add(norm)
        out.append(norm)

    return out


# ==========================
# Steady proxy + filters
# ==========================
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


# ==========================
# Scoring + continuity
# ==========================
def _score(row: Dict[str, Any], layer: str, cont_count: int) -> float:
    pct = float(row.get("pct") or 0.0)
    vs = float(row.get("vol_spike_10g") or 0.0)
    sp = _steady_proxy(pct, _safe_float(vs))

    s = 0.0
    s += sp * 6.0
    s += min(vs, 3.0) * 2.0

    if pct <= 0:
        pct_part = 0.0
    else:
        pct_part = min(pct, 3.0) / 3.0
    s += pct_part * 2.0

    if layer == "L1":
        s += 0.40
    else:
        s += 0.70

    if cont_count >= 3:
        s += WHALE_CONT_BONUS_3
    elif cont_count >= 2:
        s += WHALE_CONT_BONUS_2

    return float(s)


def _continuity_update(state: dict, symbols_seen: List[str]) -> Dict[str, int]:
    cont = (state.get("continuity") or {})
    now_utc = _utc_now_iso()

    seen_set = set([_wl_norm(x) for x in symbols_seen if _wl_norm(x)])
    out_counts: Dict[str, int] = {}

    for sym in seen_set:
        cur = cont.get(sym) or {}
        prev_count = int(cur.get("count") or 0)
        new_count = prev_count + 1
        cont[sym] = {"count": new_count, "last_seen_utc": now_utc}
        out_counts[sym] = new_count

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


# ==========================
# Alert decision (cooldown + hash)
# ==========================
def _cooldown_ok(last_utc: Optional[str]) -> bool:
    if not last_utc:
        return True
    try:
        dt = datetime.fromisoformat(last_utc.replace("Z", "+00:00"))
        return (time.time() - dt.timestamp()) >= (WHALE_COOLDOWN_MIN * 60)
    except Exception:
        return True


def _format_message(row: Dict[str, Any], layer: str, score: float, cont_count: int) -> str:
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

    # Seviye
    if score >= 10.5:
        level = "BALİNA KİLİT"
        trust = "YÜKSEK"
    elif score >= 9.5:
        level = "BALİNA RADAR"
        trust = "ORTA-YÜKSEK"
    else:
        level = "ÖN RADAR (İZLE)"
        trust = "ORTA"

    # Pump analizi
    if pct >= 4.50:
        tempo_note = "Pump riski yüksek (aşırı şişme)."
    elif pct >= 3.50:
        tempo_note = "Hızlanma var, pump mı kontrollü mü takip şart."
    else:
        tempo_note = "Kontrollü tırmanış profili."

    # Mentor B teyit
    teyit_ok = (score >= 9.5 and cont_count >= 2 and (vs is not None and vs >= 1.30))

    if teyit_ok:
        action_block = [
            "NE YAPAYIM? (Mentor B) → GİRİŞ PLANI AKTİF",
            "1) Pullback bekle (kontrollü geri çekilme).",
            "2) Pullback sonrası yukarı kırılım gelirse giriş değerlendir.",
            "3) Sonraki scan’de skor düşmezse pozisyon korunur.",
        ]
    else:
        action_block = [
            "NE YAPAYIM? (Mentor B) → ŞİMDİLİK İZLE",
            "1) Teyit gelmeden giriş yok.",
            "2) Teyit şartı: Skor ≥ 9.5 + Süreklilik ≥ 2 + Hacim ≥ 1.30x",
            "3) Pullback + yeniden yukarı kırılım görmeden giriş düşünme.",
        ]

    risk_block = [
        "RİSK NOTU",
        f"- {tempo_note}",
        "- Hacim 1.10x altına düşerse izlemeye dön.",
        "- Günlük %+3.50 üstü pump riski artar.",
        "- Skor 1 puan ve üzeri düşerse balina zayıflıyor olabilir.",
    ]

    prefix = "DRY-RUN TEST\n" if (WHALE_DRY_RUN and WHALE_DRY_RUN_TAG) else ""

    msg_lines = [
        prefix + f"🐳 WHALE ENGINE — {level} | Güven: {trust}",
        "",
        f"Hisse: {sym}   Fiyat: {fnum(last, 2)}",
        msg_lines.insert(0, "🧪 TEST: whale_engine.py format aktif (deploy kontrol)")
        msg_lines.insert(1, "------------------------------")
        f"Günlük: {pct:+.2f}%   Hacim(10g): {fnum(vs, 2)}x   Steady: {fnum(sp, 2)}   Skor: {fnum(score, 2)}",
        f"Süreklilik: {int(cont_count)} scan   Katman: {layer}",
        "",
    ]
    msg_lines.extend(action_block)
    msg_lines.append("")
    msg_lines.extend(risk_block)
    msg_lines.append("")
    msg_lines.append(f"Saat: {datetime.now().strftime('%H:%M')}")

    return "\n".join(msg_lines)


# ==========================
# PUBLIC JOB (main.py will schedule this)
# Requires adapters in bot_data:
#  - bist_session_open(): bool
#  - telegram_send(ctx, chat_id, text, **kwargs) async
# ==========================
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

    # Seans kapalıysa normalde bloklar; dry-run açıksa geçer
    if not WHALE_DRY_RUN:
        try:
            if bist_open_fn and (not bist_open_fn()):
                return
        except Exception:
            pass

    st = _load_json(WHALE_STATE_FILE, _default_whale_state())
    la = _load_json(WHALE_LAST_ALERT_FILE, _default_last_alert())
    last_map = la.get("last_alert_by_symbol") or {}

    # Layer 1
    l1_rows = _tv_topn_rows(WHALE_TOPN)
    l1_candidates = [r for r in l1_rows if _passes_layer1(r)]

    # Layer 2
    universe = _parse_universe_env(UNIVERSE_TICKERS)
    l2_rows = _tv_universe_rows(universe) if universe else []
    l2_candidates = [r for r in l2_rows if _passes_layer2(r)]

    # Merge (L2 overwrite)
    merged: Dict[str, Dict[str, Any]] = {}
    for r in l1_candidates:
        merged[r["symbol"]] = dict(r)
        merged[r["symbol"]]["layer"] = "L1"
    for r in l2_candidates:
        merged[r["symbol"]] = dict(r)
        merged[r["symbol"]]["layer"] = "L2"

    symbols_seen = list(merged.keys())
    cont_counts = _continuity_update(st, symbols_seen)

    st["scan"]["last_scan_utc"] = _utc_now_iso()
    _save_json(WHALE_STATE_FILE, st)

    if not merged:
        logger.info("WHALE_ENGINE: no candidates")
        return

    scored: List[Tuple[str, float, Dict[str, Any]]] = []
    for sym, r in merged.items():
        layer = r.get("layer") or "L1"
        cc = int(cont_counts.get(sym) or 1)
        s = _score(r, layer, cc)
        r["score"] = s
        r["cont"] = cc
        if s >= WHALE_SCORE_MIN:
            scored.append((sym, s, r))

    if not scored:
        logger.info("WHALE_ENGINE: no alerts (score below min)")
        return

    scored.sort(key=lambda x: x[1], reverse=True)
    scored = scored[:max(1, int(WHALE_MAX_ALERTS_PER_SCAN))]

    sent = 0
    for sym, s, r in scored:
        entry = last_map.get(sym) or {}
        if not _cooldown_ok(entry.get("last_alert_utc")):
            continue

        msg = _format_message(r, r.get("layer") or "L1", s, int(r.get("cont") or 1))
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

        # KILIT pipeline
        try:
            prime_watchlist_add(sym)
        except Exception:
            pass

    la["last_alert_by_symbol"] = last_map
    _save_json(WHALE_LAST_ALERT_FILE, la)

    logger.info("WHALE_ENGINE: sent=%d", sent)
