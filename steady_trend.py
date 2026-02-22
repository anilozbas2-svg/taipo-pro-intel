import os
import json
import time
import math
import logging
import inspect
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests

logger = logging.getLogger("STEADY_TREND")

# =========================================================
# ENV HELPERS
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


def _safe_chat_id(raw: str) -> Optional[int]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# =========================================================
# FILE HELPERS (persistent state)
# =========================================================
def _load_json(path: str, default: dict) -> dict:
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or default
    except Exception as e:
        logger.warning("STEADY_TREND load_json error: %s", e)
        return default


def _save_json(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning("STEADY_TREND save_json error: %s", e)


# =========================================================
# TICKER NORMALIZATION
# =========================================================
def _tv_ticker(sym: str) -> str:
    """
    Converts:
      THYAO.IS   -> BIST:THYAO
      THYAO      -> BIST:THYAO
      BIST:THYAO -> BIST:THYAO
    """
    s = (sym or "").strip().upper()
    if not s:
        return ""
    if s.endswith(".IS"):
        s = s[:-3]
    if ":" in s:
        return s
    return f"BIST:{s}"


def _norm_symbol(sym: str) -> str:
    """
    Converts:
      BIST:THYAO -> THYAO
      THYAO.IS   -> THYAO
      THYAO      -> THYAO
    """
    s = (sym or "").strip().upper()
    if not s:
        return ""
    if ":" in s:
        s = s.split(":")[-1].strip()
    if s.endswith(".IS"):
        s = s[:-3]
    return s


# =========================================================
# ENV
# =========================================================
STEADY_TREND_ENABLED = _env_bool("STEADY_TREND_ENABLED", False)

STEADY_TREND_CHAT_ID_RAW = os.getenv("STEADY_TREND_CHAT_ID", "").strip()
STEADY_TREND_CHAT_ID = _safe_chat_id(STEADY_TREND_CHAT_ID_RAW)

STEADY_TREND_INTERVAL_MIN = _env_int("STEADY_TREND_INTERVAL_MIN", 2)
STEADY_TREND_COOLDOWN_MIN = _env_int("STEADY_TREND_COOLDOWN_MIN", 45)

# Classic (proxy) filters
STEADY_TREND_MIN_PCT = _env_float("STEADY_TREND_MIN_PCT", 0.60)
STEADY_TREND_MAX_PCT = _env_float("STEADY_TREND_MAX_PCT", 2.20)

# "Sessiz tırmanış" yakalamak için VOL_SPIKE şartını gevşetiyoruz
STEADY_TREND_MIN_VOL_SPIKE = _env_float("STEADY_TREND_MIN_VOL_SPIKE", 0.85)

# Senin ENV’de 0.30, default da 0.30 olmalı
STEADY_TREND_PROXY_MIN_STEADY = _env_float("STEADY_TREND_PROXY_MIN_STEADY", 0.30)

# Silent climb (2 saat, 15dk bucket) – “sessiz tırmanış” modu
STEADY_TREND_SILENT_ENABLED = _env_bool("STEADY_TREND_SILENT_ENABLED", True)
STEADY_TREND_BUCKET_MIN = _env_int("STEADY_TREND_BUCKET_MIN", 15)  # 15dk
STEADY_TREND_WINDOW_MIN = _env_int("STEADY_TREND_WINDOW_MIN", 120)  # 2 saat
STEADY_TREND_MIN_BUCKETS = _env_int("STEADY_TREND_MIN_BUCKETS", 8)  # 8x15dk = 2 saat

# Silent climb thresholds
STEADY_TREND_SILENT_MIN_CLIMB_PCT = _env_float("STEADY_TREND_SILENT_MIN_CLIMB_PCT", 2.20)
STEADY_TREND_SILENT_MAX_CLIMB_PCT = _env_float("STEADY_TREND_SILENT_MAX_CLIMB_PCT", 5.50)
STEADY_TREND_SILENT_MAX_DRAWDOWN_PCT = _env_float("STEADY_TREND_SILENT_MAX_DRAWDOWN_PCT", 1.20)
STEADY_TREND_SILENT_MIN_POS_RATIO = _env_float("STEADY_TREND_SILENT_MIN_POS_RATIO", 0.65)

# TV
TV_SCAN_URL = os.getenv("STEADY_TREND_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = _env_int("STEADY_TREND_TV_TIMEOUT", 12)

# Data dir / state
DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
STEADY_TREND_STATE_FILE = os.path.join(DATA_DIR, "steady_trend_state.json")
STEADY_TREND_ALERT_FILE = os.path.join(DATA_DIR, "steady_trend_last_alert.json")

# Dry-run (seans kapalı test)
STEADY_TREND_DRY_RUN = _env_bool("STEADY_TREND_DRY_RUN", False)
STEADY_TREND_DRY_RUN_TAG = _env_bool("STEADY_TREND_DRY_RUN_TAG", False)

# Per-scan picks
STEADY_TREND_TOPK = _env_int("STEADY_TREND_TOPK", 3)

# =========================================================
# STATE DEFAULTS
# =========================================================
def _default_state() -> dict:
    return {
        "schema_version": "2.0",
        "system": "steady_trend",
        "updated_utc": None,
        "buckets": {
            # "THYAO": [{"b": int_bucket_id, "t": iso, "last": float, "pct": float, "vs": float, "vol": float}]
        },
    }


def _default_alerts() -> dict:
    return {
        "schema_version": "2.0",
        "system": "steady_trend",
        "cooldown_min": STEADY_TREND_COOLDOWN_MIN,
        "last_alert_by_symbol": {
            # "THYAO": {"last_alert_utc": "..."}
        },
    }


# =========================================================
# COOLDOWN (persistent)
# =========================================================
def _cooldown_ok(symbol: str, alerts: dict) -> bool:
    now_ts = time.time()
    mp = alerts.get("last_alert_by_symbol") or {}
    entry = mp.get(symbol) or {}
    last_utc = entry.get("last_alert_utc")
    if not last_utc:
        return True
    try:
        dt = datetime.fromisoformat(str(last_utc).replace("Z", "+00:00"))
        if (now_ts - dt.timestamp()) < (STEADY_TREND_COOLDOWN_MIN * 60):
            return False
        return True
    except Exception:
        return True


def _mark_alert(symbol: str, alerts: dict) -> None:
    mp = alerts.get("last_alert_by_symbol") or {}
    mp[str(symbol).strip().upper()] = {"last_alert_utc": _utc_now_iso()}
    alerts["last_alert_by_symbol"] = mp


# =========================================================
# TRADINGVIEW SCAN
# =========================================================
def _tv_scan_for_tickers(tickers: List[str]) -> List[Dict[str, Any]]:
    tv_tickers = [_tv_ticker(t) for t in (tickers or []) if t and str(t).strip()]
    tv_tickers = [t for t in tv_tickers if t]
    if not tv_tickers:
        return []

    payload = {
        "filter": [
            {"left": "volume", "operation": "nempty"},
            {"left": "change", "operation": "nempty"},
            {"left": "close", "operation": "nempty"},
        ],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": tv_tickers},
        "columns": [
            "name",
            "change",  # percent change (günlük)
            "volume",
            "close",
            "average_volume_10d_calc",
        ],
        "sort": {"sortBy": "volume", "sortOrder": "desc"},
        "range": [0, min(200, max(0, len(tv_tickers) - 1))],
    }

    try:
        r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
        r.raise_for_status()
        js = r.json() or {}

        out: List[Dict[str, Any]] = []
        for row in (js.get("data") or []):
            d = row.get("d") or []
            # expected: [name, change, volume, close, avg_vol_10d]
            if len(d) < 4:
                continue

            sym = _norm_symbol(str(d[0]))
            pct = _safe_float(d[1])
            vol = _safe_float(d[2])
            last = _safe_float(d[3])
            av10 = _safe_float(d[4]) if len(d) >= 5 else None

            if not sym or pct is None or vol is None or last is None:
                continue

            vol_spike_10g = None
            if av10 is not None and av10 > 0:
                vol_spike_10g = float(vol) / float(av10)

            # "Steady proxy": candle datası yok, o yüzden kontrollü proxy skor (0..1)
            steady_proxy = 0.20
            if STEADY_TREND_MIN_PCT <= float(pct) <= STEADY_TREND_MAX_PCT:
                steady_proxy += 0.45
            if vol_spike_10g is not None and vol_spike_10g >= STEADY_TREND_MIN_VOL_SPIKE:
                steady_proxy += 0.35
            steady_proxy = max(0.0, min(1.0, steady_proxy))

            out.append(
                {
                    "symbol": sym,
                    "last": float(last),
                    "pct_day": float(pct),
                    "volume": float(vol),
                    "av10": float(av10) if av10 is not None else None,
                    "vol_spike_10g": float(vol_spike_10g) if vol_spike_10g is not None else None,
                    "steady_proxy": float(steady_proxy),
                }
            )

        return out
    except Exception as e:
        logger.warning("STEADY_TREND: TV scan error: %s", e)
        return []


# =========================================================
# CLASSIC FILTER & SCORE
# =========================================================
def _passes_filters(row: Dict[str, Any]) -> bool:
    pct = _safe_float(row.get("pct_day"))
    vs = _safe_float(row.get("vol_spike_10g"))
    proxy = _safe_float(row.get("steady_proxy"))

    if pct is None or proxy is None:
        return False

    # "Sessiz tırmanış" için vs None gelebilir; ama TV genelde avg_vol_10d veriyor.
    if vs is None:
        vs = 1.0

    if pct < STEADY_TREND_MIN_PCT:
        return False
    if pct > STEADY_TREND_MAX_PCT:
        return False
    if vs < STEADY_TREND_MIN_VOL_SPIKE:
        return False
    if proxy < STEADY_TREND_PROXY_MIN_STEADY:
        return False
    return True


def _steady_score(row: Dict[str, Any]) -> float:
    pct = float(row.get("pct_day") or 0.0)
    vs = float(row.get("vol_spike_10g") or 1.0)
    proxy = float(row.get("steady_proxy") or 0.0)

    denom = max(0.01, (STEADY_TREND_MAX_PCT - STEADY_TREND_MIN_PCT))
    pct_norm = max(0.0, min(1.0, (pct - STEADY_TREND_MIN_PCT) / denom))

    s = 0.0
    s += proxy * 4.0
    s += min(vs, 3.0) * 2.0
    s += pct_norm * 1.5
    return float(s)


# =========================================================
# SILENT CLIMB (2h / 15m bucket) – HISTORY
# =========================================================
def _bucket_id(ts: float) -> int:
    step = max(1, int(STEADY_TREND_BUCKET_MIN)) * 60
    return int(ts // step)


def _history_update(state: dict, rows: List[Dict[str, Any]]) -> None:
    now_ts = time.time()
    b_id = _bucket_id(now_ts)
    now_iso = _utc_now_iso()

    buckets = state.get("buckets") or {}
    for r in rows:
        sym = str(r.get("symbol") or "").strip().upper()
        if not sym:
            continue

        last = _safe_float(r.get("last"))
        pct = _safe_float(r.get("pct_day"))
        vs = _safe_float(r.get("vol_spike_10g"))
        vol = _safe_float(r.get("volume"))

        if last is None or pct is None:
            continue

        series = buckets.get(sym) or []

        # One sample per bucket: replace if same bucket, else append
        if series and int(series[-1].get("b") or -1) == b_id:
            series[-1] = {"b": b_id, "t": now_iso, "last": float(last), "pct": float(pct), "vs": float(vs) if vs is not None else None, "vol": float(vol) if vol is not None else None}
        else:
            series.append({"b": b_id, "t": now_iso, "last": float(last), "pct": float(pct), "vs": float(vs) if vs is not None else None, "vol": float(vol) if vol is not None else None})

        # Prune: keep last window + buffer
        max_keep = max(12, int(STEADY_TREND_MIN_BUCKETS) + 6)
        if len(series) > max_keep:
            series = series[-max_keep:]

        buckets[sym] = series

    state["buckets"] = buckets
    state["updated_utc"] = now_iso


def _silent_metrics(series: List[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    if not series:
        return None

    # Need last N buckets
    need = max(2, int(STEADY_TREND_MIN_BUCKETS))
    if len(series) < need:
        return None

    s = series[-need:]
    prices = [float(x.get("last") or 0.0) for x in s if _safe_float(x.get("last")) is not None]
    if len(prices) < need:
        return None

    first = prices[0]
    last = prices[-1]
    if first <= 0:
        return None

    climb_pct = (last / first - 1.0) * 100.0

    # Positive step ratio
    ups = 0
    steps = 0
    peak = prices[0]
    max_dd = 0.0
    for i in range(1, len(prices)):
        steps += 1
        if prices[i] >= prices[i - 1]:
            ups += 1
        if prices[i] > peak:
            peak = prices[i]
        dd = (peak - prices[i]) / max(0.000001, peak) * 100.0
        if dd > max_dd:
            max_dd = dd

    pos_ratio = float(ups) / float(max(1, steps))

    # last bucket should not be red hard
    last_step_pct = (prices[-1] / prices[-2] - 1.0) * 100.0

    return {
        "climb_pct": float(climb_pct),
        "max_drawdown_pct": float(max_dd),
        "pos_ratio": float(pos_ratio),
        "last_step_pct": float(last_step_pct),
    }


def _silent_pass(m: Dict[str, float]) -> bool:
    if m["climb_pct"] < STEADY_TREND_SILENT_MIN_CLIMB_PCT:
        return False
    if m["climb_pct"] > STEADY_TREND_SILENT_MAX_CLIMB_PCT:
        return False
    if m["max_drawdown_pct"] > STEADY_TREND_SILENT_MAX_DRAWDOWN_PCT:
        return False
    if m["pos_ratio"] < STEADY_TREND_SILENT_MIN_POS_RATIO:
        return False
    # Last bucket: avoid sharp drop
    if m["last_step_pct"] < -0.60:
        return False
    return True


def _silent_score(m: Dict[str, float], row: Dict[str, Any]) -> float:
    # Score focused on "2h smooth climb"
    climb = float(m["climb_pct"])
    dd = float(m["max_drawdown_pct"])
    pos = float(m["pos_ratio"])

    vs = _safe_float(row.get("vol_spike_10g"))
    if vs is None:
        vs = 1.0

    s = 0.0
    # climb 2.2..5.5 -> normalize 0..1
    denom = max(0.01, (STEADY_TREND_SILENT_MAX_CLIMB_PCT - STEADY_TREND_SILENT_MIN_CLIMB_PCT))
    climb_norm = max(0.0, min(1.0, (climb - STEADY_TREND_SILENT_MIN_CLIMB_PCT) / denom))

    # drawdown inverse
    dd_norm = max(0.0, min(1.0, 1.0 - (dd / max(0.01, STEADY_TREND_SILENT_MAX_DRAWDOWN_PCT))))

    s += climb_norm * 5.0
    s += dd_norm * 3.0
    s += max(0.0, min(1.0, pos)) * 2.0

    # tiny volume bonus (but not required)
    s += min(float(vs), 2.0) * 0.6

    return float(s)


# =========================================================
# MESSAGE FORMAT (Mentor + Ne yapayım?)
# =========================================================
def _format_msg(row: Dict[str, Any], mode: str, score: float, extra: Optional[Dict[str, float]] = None) -> str:
    def fnum(x: Any, nd: int = 2) -> str:
        try:
            return f"{float(x):.{nd}f}"
        except Exception:
            return "n/a"

    sym = str(row.get("symbol") or "n/a").strip().upper()
    last = row.get("last")
    pct = row.get("pct_day")
    vs = row.get("vol_spike_10g")
    proxy = row.get("steady_proxy")

    prefix = ""
    if STEADY_TREND_DRY_RUN and STEADY_TREND_DRY_RUN_TAG:
        prefix = "DRY-RUN\n\n"

    # Badge
    if score >= 9.5:
        badge = "🐳 STEADY KILIT"
        stance = "YÜKSEK ÖNCELİK"
    elif score >= 8.0:
        badge = "🦈 STEADY RADAR"
        stance = "TAKİP"
    else:
        badge = "🟡 STEADY İZ"
        stance = "TEYİT BEKLE"

    # Mentor action plan (No HTML <b>)
    # Not: finansal tavsiye değil, sistem disiplini metni.
    action = []
    action.append("NE YAPAYIM?")
    action.append("1) Giriş: İlk uyarıda atlama. 1-2 teyit (bir sonraki scan/bucket) bekle.")
    action.append("2) Teyit: Fiyat yeni zirve yaptıktan sonra küçük geri çekilmede (acele yok).")
    action.append("3) Risk: Tek seferde full girme. Parça parça (2-3 dilim).")
    action.append("4) Çıkış: İvme bozulursa (2 ardışık bucket aşağı / sert kırmızı) disiplinle azalt.")
    action.append("5) İptal: Sert spike + sert geri verme görürsen bu sinyal değil, tuzak olabilir.")

    lines = []
    lines.append(prefix + f"{badge}  |  {stance}")
    lines.append("────────────────────────────")
    lines.append(f"Hisse: {sym}")
    lines.append(f"Mod: {mode}")
    lines.append(f"Fiyat: {fnum(last, 2)}")
    if pct is not None:
        lines.append(f"Günlük: {fnum(pct, 2)}%")
    if vs is not None:
        lines.append(f"Hacim (10g): {fnum(vs, 2)}x")
    if proxy is not None:
        lines.append(f"Steady Proxy: {fnum(proxy, 2)}")
    lines.append(f"Skor: {fnum(score, 2)}")

    if extra:
        lines.append("────────────────────────────")
        lines.append(f"2s Climb: {fnum(extra.get('climb_pct'), 2)}%")
        lines.append(f"Max DD: {fnum(extra.get('max_drawdown_pct'), 2)}%")
        lines.append(f"Pozitif adım: {fnum(extra.get('pos_ratio'), 2)}")

    lines.append("────────────────────────────")
    lines.extend(action)
    lines.append("────────────────────────────")
    lines.append(f"Saat: {datetime.now().strftime('%H:%M')}")

    return "\n".join(lines)


# =========================================================
# CORE JOB
# =========================================================
async def steady_trend_job(ctx, bist_open_fn, fetch_rows_fn, telegram_send_fn) -> None:
    if not STEADY_TREND_ENABLED:
        return
    if STEADY_TREND_CHAT_ID is None:
        return
    if not telegram_send_fn or not fetch_rows_fn:
        return

    # BIST gate
    try:
        if (not STEADY_TREND_DRY_RUN) and bist_open_fn and (not bist_open_fn()):
            return
    except Exception:
        if not STEADY_TREND_DRY_RUN:
            return

    # fetch rows (universe)
    try:
        if inspect.iscoroutinefunction(fetch_rows_fn):
            rows = await fetch_rows_fn(ctx)
        else:
            rows = fetch_rows_fn(ctx)
    except Exception:
        return

    tickers: List[str] = []
    for r in (rows or []):
        t = (r.get("ticker") or r.get("symbol") or "").strip().upper()
        if t:
            tickers.append(t)
    if not tickers:
        return

    tv_rows = _tv_scan_for_tickers(tickers)
    if not tv_rows:
        return

    # Load state + alerts
    st = _load_json(STEADY_TREND_STATE_FILE, _default_state())
    al = _load_json(STEADY_TREND_ALERT_FILE, _default_alerts())

    # Update history buckets (for silent climb)
    _history_update(st, tv_rows)
    _save_json(STEADY_TREND_STATE_FILE, st)

    picks: List[Tuple[float, Dict[str, Any], str, Optional[Dict[str, float]]]] = []

    # Classic picks
    for r in tv_rows:
        if not _passes_filters(r):
            continue
        s = _steady_score(r)
        r["steady_score"] = s
        picks.append((s, r, "STEADY-PROXY", None))

    # Silent climb picks (2h smooth up) – can catch “hacim patlamadan tırmanış”
    if STEADY_TREND_SILENT_ENABLED:
        buckets = st.get("buckets") or {}
        for r in tv_rows:
            sym = str(r.get("symbol") or "").strip().upper()
            if not sym:
                continue
            series = buckets.get(sym) or []
            m = _silent_metrics(series)
            if not m:
                continue
            if not _silent_pass(m):
                continue
            ss = _silent_score(m, r)
            r["silent_score"] = ss
            picks.append((ss + 2.0, r, "SESSİZ-TIRMANIŞ (2s)", m))  # +2 bonus to prioritize

    if not picks:
        _save_json(STEADY_TREND_ALERT_FILE, al)
        return

    picks.sort(key=lambda x: float(x[0]), reverse=True)
    top = picks[:max(1, int(STEADY_TREND_TOPK))]

    sent = 0
    for score, r, mode, extra in top:
        sym = str(r.get("symbol") or "").strip().upper()
        if not sym:
            continue

        if not _cooldown_ok(sym, al):
            continue

        msg = _format_msg(r, mode, float(score), extra)

        try:
            if inspect.iscoroutinefunction(telegram_send_fn):
                await telegram_send_fn(ctx, STEADY_TREND_CHAT_ID, msg)
            else:
                telegram_send_fn(ctx, STEADY_TREND_CHAT_ID, msg)
            sent += 1
        except Exception:
            continue

        _mark_alert(sym, al)

    _save_json(STEADY_TREND_ALERT_FILE, al)
    logger.info("STEADY_TREND: sent=%d", sent)


# =========================================================
# Backward compatibility entrypoint (main.py schedule_jobs can call this)
# Expects in app.bot_data:
#   - bist_session_open(): bool
#   - fetch_universe_rows(ctx) -> list[dict]  (sync or async)
#   - telegram_send(ctx, chat_id, text, **kwargs) async or sync
# =========================================================
async def job_steady_trend_scan(context, *args, **kwargs) -> None:
    app = getattr(context, "application", None)
    bot_data = getattr(app, "bot_data", {}) if app else {}

    bist_open_fn = bot_data.get("bist_session_open")
    fetch_rows_fn = bot_data.get("fetch_universe_rows")
    telegram_send_fn = bot_data.get("telegram_send")

    if not fetch_rows_fn or not telegram_send_fn:
        return

    await steady_trend_job(context, bist_open_fn, fetch_rows_fn, telegram_send_fn)
```0
