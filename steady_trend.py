import os
import json
import time
import math
import hashlib
import logging
import inspect
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional

import requests

logger = logging.getLogger("STEADY_TREND")

logger.warning("STEADY FILE LOADED NEW VERSION")

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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_chat_id(raw: str) -> Optional[int]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


# =========================================================
# TR TIME / MARKET GATE
# =========================================================
TR_TZ = ZoneInfo("Europe/Istanbul")


def _steady_is_trading_time_tr() -> bool:
    now = datetime.now(TR_TZ)

    # 5=Saturday, 6=Sunday
    if now.weekday() >= 5:
        return False

    h = now.hour

    # BIST normal seans 10:00 - 18:00 (TR)
    return 10 <= h < 18


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


def _parse_tickers_env(raw: str) -> List[str]:
    raw = (raw or "").strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.replace("\n", ",").split(",") if p.strip()]
    out: List[str] = []
    seen: set = set()
    for p in parts:
        n = _norm_symbol(p)
        if not n:
            continue
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


# =========================================================
# ENV
# =========================================================
STEADY_TREND_ENABLED = _env_bool("STEADY_TREND_ENABLED", False)
STEADY_TREND_CHAT_ID = _safe_chat_id(os.getenv("STEADY_TREND_CHAT_ID", "").strip())

STEADY_TREND_INTERVAL_MIN = _env_int("STEADY_TREND_INTERVAL_MIN", 2)
STEADY_TREND_COOLDOWN_MIN = _env_int("STEADY_TREND_COOLDOWN_MIN", 45)

# Sessiz tırmanış penceresi
STEADY_WINDOW_MIN = _env_int("STEADY_WINDOW_MIN", 120)
STEADY_UP_RATIO_MIN = _env_float("STEADY_UP_RATIO_MIN", 0.65)
STEADY_MAX_DRAWDOWN_PCT = _env_float("STEADY_MAX_DRAWDOWN_PCT", 0.80)

# Trend toplam yükseliş bandı (pencere içi)
STEADY_TREND_MIN_PCT = _env_float("STEADY_TREND_MIN_PCT", 0.60)
STEADY_TREND_MAX_PCT = _env_float("STEADY_TREND_MAX_PCT", 9.99)

# Hacim / proxy
STEADY_TREND_MIN_VOL_SPIKE = _env_float("STEADY_TREND_MIN_VOL_SPIKE", 0.90)
STEADY_TREND_PROXY_MIN_STEADY = _env_float("STEADY_TREND_PROXY_MIN_STEADY", 0.30)

# Dry-run
STEADY_TREND_DRY_RUN = _env_bool("STEADY_TREND_DRY_RUN", False)
STEADY_TREND_DRY_RUN_TAG = _env_bool("STEADY_TREND_DRY_RUN_TAG", False)

# TradingView
TV_SCAN_URL = os.getenv("STEADY_TREND_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = _env_int("STEADY_TREND_TV_TIMEOUT", 12)

# Chunk/batch (ban/rate-limit azaltır)
STEADY_TV_BATCH_SIZE = _env_int("STEADY_TV_BATCH_SIZE", 80)
STEADY_TV_BATCH_SLEEP_MS = _env_int("STEADY_TV_BATCH_SLEEP_MS", 350)
STEADY_TV_RETRY = _env_int("STEADY_TV_RETRY", 2)
STEADY_TV_RETRY_SLEEP_MS = _env_int("STEADY_TV_RETRY_SLEEP_MS", 700)

# Universe tickers
STEADY_UNIVERSE_TICKERS = os.getenv("STEADY_UNIVERSE_TICKERS", "").strip()

# Spam-kill (ENV isimleri sende COOLDOWN_SEC / MAX_PER_TICK olarak var)
# İstersen STEADY_SPAM_COOLDOWN_SEC / STEADY_MAX_PER_TICK de kullanabilirsin.
STEADY_SPAM_COOLDOWN_SEC = _env_int("STEADY_SPAM_COOLDOWN_SEC", _env_int("COOLDOWN_SEC", 7200))
STEADY_MAX_PER_TICK = _env_int("STEADY_MAX_PER_TICK", _env_int("MAX_PER_TICK", 3))

# Force bypass market checks (test için)
STEADY_TREND_FORCE = _env_bool("STEADY_TREND_FORCE", False)

# State
DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
STEADY_STATE_FILE = os.path.join(DATA_DIR, "steady_trend_state.json")


# =========================================================
# STATE IO
# =========================================================
def _default_state() -> dict:
    return {
        "schema_version": "1.2",
        "system": "steady_trend",
        "updated_utc": None,
        "last_sent_utc": {},     # symbol -> iso
        "sent_ts": {},           # symbol -> epoch (spam kill)
        "sent_sig_ts": {},       # signature -> epoch (spam kill)
        "series": {},            # symbol -> [{"t": iso, "p": price, "pct": day_pct, "vs": vol_spike}, ...]
    }


def _load_json(path: str, default: dict) -> dict:
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f) or {}
        return d if isinstance(d, dict) else default
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


def _cooldown_ok(state: dict, symbol: str) -> bool:
    sym = _norm_symbol(symbol)
    if not sym:
        return False

    last_map = state.get("last_sent_utc") or {}
    last_utc = last_map.get(sym)

    if not last_utc:
        return True
    try:
        dt = datetime.fromisoformat(last_utc.replace("Z", "+00:00"))
        return (time.time() - dt.timestamp()) >= (STEADY_TREND_COOLDOWN_MIN * 60)
    except Exception:
        return True


def _mark_sent(state: dict, symbol: str) -> None:
    sym = _norm_symbol(symbol)
    if not sym:
        return
    last_map = state.get("last_sent_utc") or {}
    last_map[sym] = _utc_now_iso()
    state["last_sent_utc"] = last_map


# =========================================================
# TRADINGVIEW SCAN (chunked)
# =========================================================
def _tv_scan_with_retry(payload: dict) -> dict:
    last_err: Optional[Exception] = None
    for i in range(max(0, STEADY_TV_RETRY) + 1):
        try:
            r = requests.post(TV_SCAN_URL, json=payload, timeout=TV_TIMEOUT)
            r.raise_for_status()
            return r.json() or {}
        except Exception as e:
            last_err = e
            if i < STEADY_TV_RETRY:
                time.sleep(float(STEADY_TV_RETRY_SLEEP_MS) / 1000.0)
                continue
    if last_err:
        raise last_err
    return {}


def _tv_scan_for_tickers_batch(tickers: List[str]) -> List[Dict[str, Any]]:
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
        "columns": ["name", "change", "volume", "close", "average_volume_10d_calc"],
        "sort": {"sortBy": "volume", "sortOrder": "desc"},
        "range": [0, max(0, len(tv_tickers) - 1)],
    }

    js = _tv_scan_with_retry(payload)

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

        # DEBUG: satır gerçekten geliyor mu?
        logger.info("TVROW %s pct=%s vol=%s av10=%s", sym, pct, vol, av10)

        vol_spike_10g = None
        if av10 is not None and av10 > 0:
            vol_spike_10g = vol / av10

        # proxy: OHLC yok -> kontrollü skor
        steady_proxy = 0.20
        if STEADY_TREND_MIN_PCT <= pct <= STEADY_TREND_MAX_PCT:
            steady_proxy += 0.45
        if vol_spike_10g is not None and vol_spike_10g >= STEADY_TREND_MIN_VOL_SPIKE:
            steady_proxy += 0.35
        steady_proxy = max(0.0, min(1.0, steady_proxy))

        out.append(
            {
                "symbol": sym,
                "last": float(last),
                "pct_day": float(pct),
                "vol_spike_10g": float(vol_spike_10g) if vol_spike_10g is not None else None,
                "steady_proxy": float(steady_proxy),
            }
        )
    return out


def _tv_scan_for_tickers_chunked(tickers: List[str]) -> List[Dict[str, Any]]:
    t = [_norm_symbol(x) for x in (tickers or [])]
    t = [x for x in t if x]
    if not t:
        return []

    batch_size = max(10, int(STEADY_TV_BATCH_SIZE))
    all_rows: List[Dict[str, Any]] = []

    batches = [t[i:i + batch_size] for i in range(0, len(t), batch_size)]
    for i, b in enumerate(batches):
        try:
            rows = _tv_scan_for_tickers_batch(b)
            all_rows.extend(rows)
        except Exception as e:
            logger.warning("STEADY_TREND: TV batch scan error: %s", e)

        if i < (len(batches) - 1) and STEADY_TV_BATCH_SLEEP_MS > 0:
            time.sleep(float(STEADY_TV_BATCH_SLEEP_MS) / 1000.0)

    return all_rows


# =========================================================
# SERIES UPDATE + TREND METRICS
# =========================================================
def _series_push(state: dict, row: Dict[str, Any]) -> None:
    sym = _norm_symbol(row.get("symbol") or "")
    if not sym:
        return

    series = state.get("series") or {}
    arr = series.get(sym) or []

    arr.append(
        {
            "t": _utc_now_iso(),
            "p": float(row.get("last") or 0.0),
            "pct": float(row.get("pct_day") or 0.0),
            "vs": float(row["vol_spike_10g"]) if row.get("vol_spike_10g") is not None else None,
        }
    )

    keep_min = max(30, int(STEADY_WINDOW_MIN) + 30)
    max_points = max(40, int(keep_min / max(1, STEADY_TREND_INTERVAL_MIN)) + 10)
    if len(arr) > max_points:
        arr = arr[-max_points:]

    series[sym] = arr
    state["series"] = series


def _window_slice(arr: List[dict]) -> List[dict]:
    if not arr:
        return []
    need_points = max(5, int(STEADY_WINDOW_MIN / max(1, STEADY_TREND_INTERVAL_MIN)))
    if len(arr) < need_points:
        return []
    return arr[-need_points:]


def _trend_metrics(arr: List[dict]) -> Optional[Dict[str, float]]:
    w = _window_slice(arr)
    if not w or len(w) < 5:
        return None

    prices = [float(x.get("p") or 0.0) for x in w]
    if any(p <= 0 for p in prices):
        return None

    first = prices[0]
    last = prices[-1]
    total_pct = (last / first - 1.0) * 100.0

    ups = 0
    for i in range(1, len(prices)):
        if prices[i] >= prices[i - 1]:
            ups += 1
    up_ratio = ups / max(1, (len(prices) - 1))

    peak = prices[0]
    max_dd = 0.0
    for p in prices[1:]:
        if p > peak:
            peak = p
        dd = (peak - p) / peak * 100.0
        if dd > max_dd:
            max_dd = dd

    return {
        "total_pct": float(total_pct),
        "up_ratio": float(up_ratio),
        "max_drawdown_pct": float(max_dd),
    }


# =========================================================
# FILTER & SCORE
# =========================================================
def _passes_filters(row: Dict[str, Any]) -> bool:
    pct = _safe_float(row.get("pct_day"))
    vs = _safe_float(row.get("vol_spike_10g"))
    proxy = _safe_float(row.get("steady_proxy"))

    if pct is None or proxy is None:
        return False

    # Sessiz tırmanış: negatif gün içi performansta ele
    if pct < 0:
        return False

    # avg_vol gelmezse sessiz sinyal bozulmasın (vs None -> 0)
    if vs is None:
        vs = 0.0

    # Hacim modu:
    # STRICT -> hacim şart (eleme)
    # BONUS  -> eleme yok, skor belirleyecek
    vol_mode = os.getenv("STEADY_VOL_MODE", "STRICT").strip().upper()
    if vs < STEADY_TREND_MIN_VOL_SPIKE:
        if vol_mode != "BONUS":
            return False

    if proxy < STEADY_TREND_PROXY_MIN_STEADY:
        return False

    return True


def _steady_score(row: Dict[str, Any], m: Dict[str, float]) -> float:
    proxy = float(row.get("steady_proxy") or 0.0)
    vs = float(row.get("vol_spike_10g") or 0.0)

    total_pct = float(m.get("total_pct") or 0.0)
    up_ratio = float(m.get("up_ratio") or 0.0)
    max_dd = float(m.get("max_drawdown_pct") or 0.0)

    # normalize: pencere içi getiri bandına göre
    pct_band = max(0.01, (STEADY_TREND_MAX_PCT - STEADY_TREND_MIN_PCT))
    pct_norm = max(0.0, min(1.0, (total_pct - STEADY_TREND_MIN_PCT) / pct_band))

    # drawdown iyi ise 1'e yakın
    dd_norm = 1.0 - max(0.0, min(1.0, max_dd / max(0.01, STEADY_MAX_DRAWDOWN_PCT)))

    # Hacim ağırlığı: BONUS modda hacim “ödül” daha etkili
    vol_mode = os.getenv("STEADY_VOL_MODE", "STRICT").strip().upper()
    vs_w = 1.2 if vol_mode == "BONUS" else 1.0

    s = 0.0
    s += proxy * 2.5
    s += min(vs, 3.0) * vs_w
    s += pct_norm * 3.0
    s += up_ratio * 2.0
    s += dd_norm * 1.5

    # BONUS modda, hacim çok zayıfsa küçük ceza (spam/pseudo-sinyal azaltır)
    if vol_mode == "BONUS" and vs < 0.75:
        s -= 0.35

    return float(s)


# =========================================================
# MESSAGE FORMAT (PRO + “Ne yapayım?”)
# =========================================================
def _format_msg(row: Dict[str, Any], m: Dict[str, float]) -> str:
    def fnum(x: Any, nd: int = 2) -> str:
        try:
            return f"{float(x):.{nd}f}"
        except Exception:
            return "n/a"

    sym = _norm_symbol(row.get("symbol") or "n/a")
    price = row.get("last")
    day_pct = row.get("pct_day")
    vs = row.get("vol_spike_10g")
    proxy = row.get("steady_proxy")
    score = float(row.get("steady_score") or 0.0)

    total_pct = float(m.get("total_pct") or 0.0)
    up_ratio = float(m.get("up_ratio") or 0.0)
    max_dd = float(m.get("max_drawdown_pct") or 0.0)

    if score >= 13.0:
        quality = "ÇOK YÜKSEK"
    elif score >= 10.5:
        quality = "YÜKSEK"
    elif score >= 8.8:
        quality = "ORTA"
    else:
        quality = "DÜŞÜK"

    if score >= 11.0 and up_ratio >= 0.85 and max_dd <= 1.2:
        verdict = "🟢 GİRİŞ PLANI AKTİF"
        plan = [
            "Pullback/yatay 5–15 dk bekle (kovalama yok).",
            "Önceki tepe üstü kırılım gelirse küçük lotla giriş.",
            "Sonraki scan’de skor düşmezse ekleme değerlendir.",
        ]
    elif score >= 9.0:
        verdict = "🟡 TEYİT BEKLE"
        plan = [
            "1–2 scan izleme; acele giriş yok.",
            "Trend artıyor + MaxDD büyümüyor ise plan aktive olur.",
            "Hacim(10g) 1.10x altına düşerse izleme moduna dön.",
        ]
    else:
        verdict = "🟠 İZLEME"
        plan = [
            "Skor/UpRatio güçlenmeden giriş yok.",
            "Listeye al; güçlü scan’i bekle.",
        ]

    risks = [
        f"MaxDD {fnum(max_dd, 2)}% üstüne çıkarsa disiplinle çık.",
        "Skor 1+ puan düşerse momentum zayıflıyor olabilir.",
    ]
    if vs is not None:
        risks.insert(1, "Hacim 1.10x altına düşerse momentum zayıflar.")
    if day_pct is not None:
        risks.insert(2, "Günlük +3.50% üstü hızlanırsa pump/volatilite riski artar.")

    why = [
        f"Trend {fnum(total_pct, 2)}% / {STEADY_WINDOW_MIN}dk",
        f"Up {fnum(up_ratio, 2)}",
        f"MaxDD {fnum(max_dd, 2)}%",
        f"Hacim {fnum(vs, 2)}x",
        f"Proxy {fnum(proxy, 2)}",
    ]

    prefix = ""
    if STEADY_TREND_DRY_RUN and STEADY_TREND_DRY_RUN_TAG:
        prefix = "🧪 DRY-RUN TEST\n"

    lines: List[str] = []
    lines.append(prefix + f"🐳 STEADY TREND — Sessiz Tırmanış | Güven: {quality}")
    lines.append("────────────────────────")
    lines.append(f"📌 Hisse: {sym}  |  💰 Fiyat: {fnum(price, 2)}  |  📈 Günlük: {fnum(day_pct, 2)}%")
    lines.append(f"📊 Hacim(10g): {fnum(vs, 2)}x  |  🧭 Proxy: {fnum(proxy, 2)}  |  ⭐ Skor: {fnum(score, 2)}")
    lines.append("────────────────────────")
    lines.append(f"🕒 Pencere: {STEADY_WINDOW_MIN} dk")
    lines.append(f"✅ Trend Getiri: {fnum(total_pct, 2)}%")
    lines.append(f"✅ Up-Ratio: {fnum(up_ratio, 2)}")
    lines.append(f"⚠️ Max Drawdown: {fnum(max_dd, 2)}%")
    lines.append("────────────────────────")
    lines.append(f"🧠 Karar: {verdict}")
    lines.append("🎯 Ne yapayım?")
    for p in plan:
        lines.append(f"- {p}")
    lines.append("🧯 Risk kontrol")
    for r in risks:
        lines.append(f"- {r}")
    lines.append("────────────────────────")
    lines.append("🔎 Neden geldi?")
    lines.append("- " + " | ".join([x for x in why if x and x != "n/a"]))
    lines.append(f"⏱ Saat: {datetime.now(TR_TZ).strftime('%H:%M')}")

    return "\n".join(lines)


# =========================================================
# UNIVERSE RESOLUTION
# =========================================================
async def _get_universe(ctx, fetch_rows_fn) -> List[str]:
    env_list = _parse_tickers_env(STEADY_UNIVERSE_TICKERS)
    if env_list:
        return env_list

    if not fetch_rows_fn:
        return []

    try:
        if inspect.iscoroutinefunction(fetch_rows_fn):
            rows = await fetch_rows_fn(ctx)
        else:
            rows = fetch_rows_fn(ctx)
    except Exception:
        rows = []

    tickers: List[str] = []
    for r in (rows or []):
        t = (r.get("ticker") or r.get("symbol") or "").strip().upper()
        if t:
            tickers.append(t)
    return [_norm_symbol(x) for x in tickers if _norm_symbol(x)]


# =========================================================
# CORE JOB
# =========================================================
async def steady_trend_job(ctx, bist_open_fn, fetch_rows_fn, telegram_send_fn) -> None:
    if not STEADY_TREND_ENABLED:
        return

    if STEADY_TREND_CHAT_ID is None:
        return

    # Debug gate
    try:
        now_dbg = datetime.now(TR_TZ)
        logger.info(
            "STEADY GATE now=%s weekday=%s hour=%s dry=%s force=%s",
            now_dbg.isoformat(),
            now_dbg.weekday(),
            now_dbg.hour,
            STEADY_TREND_DRY_RUN,
            STEADY_TREND_FORCE,
        )
    except Exception:
        pass

    # HARD MARKET LOCK
    if (not STEADY_TREND_DRY_RUN) and (not STEADY_TREND_FORCE):
        if not _steady_is_trading_time_tr():
            logger.info("STEADY exit: out of trading time")
            return

        try:
            if bist_open_fn:
                ok = bool(bist_open_fn())
                logger.info("STEADY bist_open_fn=%s", ok)
                if not ok:
                    logger.info("STEADY exit: market closed")
                    return
            else:
                # bist_open_fn yoksa fail-closed
                logger.info("STEADY exit: missing bist_open_fn (fail-closed)")
                return
        except Exception as e:
            logger.warning("STEADY exit: bist_open_fn error: %s", e)
            return

    # State
    state = _load_json(STEADY_STATE_FILE, _default_state())
    now_ts = int(time.time())

    sent_ts = state.get("sent_ts")
    if not isinstance(sent_ts, dict):
        sent_ts = {}
        state["sent_ts"] = sent_ts

    sent_sig_ts = state.get("sent_sig_ts")
    if not isinstance(sent_sig_ts, dict):
        sent_sig_ts = {}
        state["sent_sig_ts"] = sent_sig_ts

    # Universe
    universe = await _get_universe(ctx, fetch_rows_fn)
    logger.info("STEADY universe_len=%d", len(universe) if universe else 0)
    if not universe:
        logger.warning("STEADY exit: universe empty")
        return

    # TV scan
    tv_rows = _tv_scan_for_tickers_chunked(universe)
    if not tv_rows:
        logger.info("STEADY exit: tv_rows empty")
        return

    # Series push
    for r in tv_rows:
        _series_push(state, r)
    state["updated_utc"] = _utc_now_iso()

    picks: List[Dict[str, Any]] = []
    for r in tv_rows:
        sym = r.get("symbol")
        if not sym:
            continue

        arr = (state.get("series") or {}).get(_norm_symbol(sym)) or []
        m = _trend_metrics(arr)
        if not m:
            continue

        total_pct = float(m.get("total_pct") or 0.0)
        up_ratio = float(m.get("up_ratio") or 0.0)
        max_dd = float(m.get("max_drawdown_pct") or 0.0)

        if total_pct < STEADY_TREND_MIN_PCT or total_pct > STEADY_TREND_MAX_PCT:
            continue
        if up_ratio < STEADY_UP_RATIO_MIN:
            continue
        if max_dd > STEADY_MAX_DRAWDOWN_PCT:
            continue
        if not _passes_filters(r):
            continue

        r["trend_total_pct"] = total_pct
        r["trend_up_ratio"] = up_ratio
        r["trend_max_dd"] = max_dd
        r["steady_score"] = _steady_score(r, m)
        picks.append(r)

    _save_json(STEADY_STATE_FILE, state)

    if not picks:
        logger.info("STEADY: no picks")
        return

    picks.sort(key=lambda x: float(x.get("steady_score") or 0.0), reverse=True)

    max_per_tick = max(0, int(STEADY_MAX_PER_TICK))
    if max_per_tick == 0:
        logger.info("STEADY: STEADY_MAX_PER_TICK=0 -> no messages")
        return

    cooldown_sec = max(60, int(STEADY_SPAM_COOLDOWN_SEC))

    top = picks[: max(1, max_per_tick)]

    def _make_sig(sym: str, rr: Dict[str, Any]) -> str:
        try:
            tp = float(rr.get("trend_total_pct") or 0.0)
            ur = float(rr.get("trend_up_ratio") or 0.0)
            dd = float(rr.get("trend_max_dd") or 0.0)
            sc = float(rr.get("steady_score") or 0.0)
        except Exception:
            tp, ur, dd, sc = 0.0, 0.0, 0.0, 0.0
        key = f"{sym}|{tp:.2f}|{ur:.2f}|{dd:.2f}|{sc:.2f}"
        return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]

    sent_count = 0

    for r in top:
        sym = _norm_symbol(r.get("symbol") or "")
        if not sym:
            continue

        last_sym_ts = int(sent_ts.get(sym) or 0)
        if (now_ts - last_sym_ts) < cooldown_sec:
            continue

        sig = _make_sig(sym, r)
        last_sig_ts = int(sent_sig_ts.get(sig) or 0)
        if (now_ts - last_sig_ts) < cooldown_sec:
            continue

        try:
            if not _cooldown_ok(state, sym):
                continue
        except Exception:
            continue

        m = {
            "total_pct": float(r.get("trend_total_pct") or 0.0),
            "up_ratio": float(r.get("trend_up_ratio") or 0.0),
            "max_drawdown_pct": float(r.get("trend_max_dd") or 0.0),
        }

        msg = _format_msg(r, m)

        if not telegram_send_fn:
            continue

        try:
            if inspect.iscoroutinefunction(telegram_send_fn):
                await telegram_send_fn(ctx, STEADY_TREND_CHAT_ID, msg)
            else:
                telegram_send_fn(ctx, STEADY_TREND_CHAT_ID, msg)

            _mark_sent(state, sym)
            sent_ts[sym] = now_ts
            sent_sig_ts[sig] = now_ts

            sent_count += 1
            if sent_count >= max_per_tick:
                break

        except Exception as e:
            logger.warning("STEADY send error: %s", e)
            continue

    _save_json(STEADY_STATE_FILE, state)
    logger.info("STEADY: sent=%d", sent_count)


# =========================================================
# BACKWARD COMPAT ENTRYPOINT (main.py schedules this)
# =========================================================
async def job_steady_trend_scan(context, *args, **kwargs) -> None:
    logger.warning("STEADY RUNTIME ENTRY reached")
    logger.info("STEADY_SCAN tick: enabled=%s chat_id=%s dry_run=%s force=%s", STEADY_TREND_ENABLED, STEADY_TREND_CHAT_ID, STEADY_TREND_DRY_RUN, STEADY_TREND_FORCE)

    app = getattr(context, "application", None)
    bot_data = getattr(app, "bot_data", {}) if app else {}

    bist_open_fn = bot_data.get("bist_session_open")
    fetch_rows_fn = bot_data.get("fetch_universe_rows")
    telegram_send_fn = (
        bot_data.get("telegram_send")
        or bot_data.get("telegram_send_fn")
        or bot_data.get("send_telegram")
        or bot_data.get("tg_send")
    )

    logger.info(
        "STEADY_ADAPTERS fetch_rows_fn=%s telegram_send_fn=%s bist_open_fn=%s",
        bool(fetch_rows_fn),
        bool(telegram_send_fn),
        bool(bist_open_fn),
    )

    # Burada return etmiyoruz: universe ENV varsa fetch olmasa da çalışabilir.
    if not telegram_send_fn:
        logger.warning("STEADY_TREND: missing telegram_send adapter (scan will run, no messages)")

    # Market precheck (wrapper seviyesinde)
    if (not STEADY_TREND_DRY_RUN) and (not STEADY_TREND_FORCE):
        if not _steady_is_trading_time_tr():
            logger.info("STEADY wrapper exit: out of trading time")
            return

        try:
            if bist_open_fn:
                ok = bool(bist_open_fn())
                logger.info("STEADY wrapper bist_open_fn=%s", ok)
                if not ok:
                    logger.info("STEADY wrapper exit: market closed")
                    return
            else:
                logger.info("STEADY wrapper exit: missing bist_open_fn (fail-closed)")
                return
        except Exception as e:
            logger.warning("STEADY wrapper exit: bist_open_fn error: %s", e)
            return

    await steady_trend_job(
        context,
        bist_open_fn,
        fetch_rows_fn,
        telegram_send_fn,
    )
