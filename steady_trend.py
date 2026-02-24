import os
import json
import time
import math
import logging
import inspect
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
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
      THYAO.IS -> BIST:THYAO
      THYAO    -> BIST:THYAO
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

# “sessiz tırmanış” penceresi
STEADY_WINDOW_MIN = _env_int("STEADY_WINDOW_MIN", 120)
STEADY_UP_RATIO_MIN = _env_float("STEADY_UP_RATIO_MIN", 0.65)
STEADY_MAX_DRAWDOWN_PCT = _env_float("STEADY_MAX_DRAWDOWN_PCT", 0.80)  # 0.80% geri çekilme üstü riskli

# toplam yükseliş bandı (2 saatlik pencere için)
STEADY_TREND_MIN_PCT = _env_float("STEADY_TREND_MIN_PCT", 0.60)
STEADY_TREND_MAX_PCT = _env_float("STEADY_TREND_MAX_PCT", 3.80)

# hacim şartı (sessiz tırmanışta hacim patlaması istemeyebiliriz)
STEADY_TREND_MIN_VOL_SPIKE = _env_float("STEADY_TREND_MIN_VOL_SPIKE", 0.90)
STEADY_TREND_PROXY_MIN_STEADY = _env_float("STEADY_TREND_PROXY_MIN_STEADY", 0.30)

STEADY_TREND_DRY_RUN = _env_bool("STEADY_TREND_DRY_RUN", False)
STEADY_TREND_DRY_RUN_TAG = _env_bool("STEADY_TREND_DRY_RUN_TAG", False)

TV_SCAN_URL = os.getenv("STEADY_TREND_TV_SCAN_URL", "https://scanner.tradingview.com/turkey/scan").strip()
TV_TIMEOUT = _env_int("STEADY_TREND_TV_TIMEOUT", 12)

# Chunk/batch ayarları (ban/rate-limit azaltır)
STEADY_TV_BATCH_SIZE = _env_int("STEADY_TV_BATCH_SIZE", 80)
STEADY_TV_BATCH_SLEEP_MS = _env_int("STEADY_TV_BATCH_SLEEP_MS", 350)
STEADY_TV_RETRY = _env_int("STEADY_TV_RETRY", 2)
STEADY_TV_RETRY_SLEEP_MS = _env_int("STEADY_TV_RETRY_SLEEP_MS", 700)

# Universe tickers (env)
STEADY_UNIVERSE_TICKERS = os.getenv("STEADY_UNIVERSE_TICKERS", "").strip()

# State
DATA_DIR = os.getenv("DATA_DIR", "/var/data").strip() or "/var/data"
STEADY_STATE_FILE = os.path.join(DATA_DIR, "steady_trend_state.json")


# =========================================================
# STATE IO
# =========================================================
def _default_state() -> dict:
    return {
        "schema_version": "1.0",
        "system": "steady_trend",
        "updated_utc": None,
        "last_sent_utc": {},   # symbol -> iso
        "series": {},          # symbol -> [{"t": iso, "p": price, "pct": day_pct, "vs": vol_spike}, ...]
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
        
        if not sym or pct is None or vol is None or last is None:
            continue

        # --- DEBUG: TV row check ---
        logger.info("TVROW %s pct=%s vol=%s av10=%s", sym, pct, vol, av10)

        vol_spike_10g = None
        if av10 is not None and av10 > 0:
            vol_spike_10g = vol / av10

        vol_spike_10g = None
        if av10 is not None and av10 > 0:
            vol_spike_10g = vol / av10

        # proxy: candle yok -> kontrollü skor
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

        # batch arası mini sleep
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

    # pencereden biraz fazla tut (window + 30dk)
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
# FILTER & SCORE (final decision)
# =========================================================
def _passes_filters(row: Dict[str, Any]) -> bool:
    pct = _safe_float(row.get("pct_day"))
    vs = _safe_float(row.get("vol_spike_10g"))
    proxy = _safe_float(row.get("steady_proxy"))

    if pct is None or proxy is None:
        return False

    # Günlük değil: trend penceresi için bandı (min/max) kullanıyoruz
    # ama TV day% zaten gün boyu; yine de “sessiz tırmanış” için soft filtre:
    if pct < 0:
        return False

    if vs is None:
        # avg_vol dönmezse “sessiz” sinyal bozulmasın: yine de geçebilir
        vs = 0.0

    if vs < STEADY_TREND_MIN_VOL_SPIKE:
        # sessiz tırmanışta hacim düşük olabilir; bu yüzden MIN_VOL_SPIKE’ı çok yüksek tutma.
        # İstersen env ile 0.80-0.95 aralığı çalışır.
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

    # normalize
    pct_band = max(0.01, (STEADY_TREND_MAX_PCT - STEADY_TREND_MIN_PCT))
    pct_norm = max(0.0, min(1.0, (total_pct - STEADY_TREND_MIN_PCT) / pct_band))
    dd_norm = 1.0 - max(0.0, min(1.0, max_dd / max(0.01, STEADY_MAX_DRAWDOWN_PCT)))

    s = 0.0
    s += proxy * 2.5
    s += min(vs, 3.0) * 1.0
    s += pct_norm * 3.0
    s += up_ratio * 2.0
    s += dd_norm * 1.5
    return float(s)


# =========================================================
# MESSAGE FORMAT (with “Ne yapayım?”)
# =========================================================
def _format_msg(row: Dict[str, Any], m: Dict[str, float]) -> str:
    def fnum(x: Any, nd: int = 2) -> str:
        try:
            return f"{float(x):.{nd}f}"
        except Exception:
            return "n/a"

    sym = row.get("symbol", "n/a")
    price = row.get("last")
    day_pct = row.get("pct_day")
    vs = row.get("vol_spike_10g")
    proxy = row.get("steady_proxy")
    score = row.get("steady_score")

    total_pct = m.get("total_pct")
    up_ratio = m.get("up_ratio")
    max_dd = m.get("max_drawdown_pct")

    # Mentor kararı (net)
    # - Skor yüksekse: giriş planı daha net
    # - Skor orta: teyit şart
    if float(score or 0.0) >= 7.8:
        verdict = "🟢 GİRİŞ ADAYI"
        action = (
            "Ne yapayım?\n"
            "• Giriş: 1) 5-10 dk yatay/mini geri çekilme gör, 2) kırınca küçük lot gir.\n"
            "• Teyit: 1 sonraki scan’de trend bozulmuyorsa ekleme düşünebilirsin.\n"
            "• Stop: max drawdown üstüne çıkarsa (geri çekilme büyürse) disiplinle çık.\n"
            "• Risk: tavan kovalamıyoruz; amaç kontrollü tırmanış."
        )
    else:
        verdict = "🟡 TEYİT BEKLE"
        action = (
            "Ne yapayım?\n"
            "• Giriş yok: 1 scan daha teyit.\n"
            "• Teyit: up_ratio aynı kalır + drawdown büyümezse girişe döner.\n"
            "• Risk: zayıf trendler ‘tek mum’ olup sönebilir."
        )

    prefix = "🧪 DRY-RUN\n" if (STEADY_TREND_DRY_RUN and STEADY_TREND_DRY_RUN_TAG) else ""

    msg = (
        prefix
        + "🐳 STEADY TREND – Sessiz Tırmanış\n"
        + "━━━━━━━━━━━━━━━━━━━━━━\n"
        + f"🎯 Hisse: {sym}\n"
        + f"💰 Fiyat: {fnum(price, 2)}\n"
        + f"📈 Günlük: {fnum(day_pct, 2)}%\n"
        + f"📊 Hacim (10g): {fnum(vs, 2)}x\n"
        + f"🧭 Proxy: {fnum(proxy, 2)}\n"
        + "━━━━━━━━━━━━━━━━━━━━━━\n"
        + f"⏳ Pencere: {STEADY_WINDOW_MIN} dk\n"
        + f"✅ Trend Getiri: {fnum(total_pct, 2)}%\n"
        + f"✅ Up-Ratio: {fnum(up_ratio, 2)}\n"
        + f"⚠️ Max Drawdown: {fnum(max_dd, 2)}%\n"
        + f"⭐ Skor: {fnum(score, 2)}\n\n"
        + f"{verdict}\n\n"
        + f"{action}\n\n"
        + f"⏱ {datetime.now().strftime('%H:%M')}"
    )

    return msg


# =========================================================
# MAIN ENTRY (called from main.py via app.bot_data adapters)
# =========================================================
def _resolve_universe(fetch_rows_fn, ctx) -> List[str]:
    # 1) ENV varsa direkt onu kullan (en net kontrol)
    env_list = _parse_tickers_env(STEADY_UNIVERSE_TICKERS)
    if env_list:
        return env_list

    # 2) Yoksa main.py içindeki fetch_universe_rows adaptöründen al
    try:
        if inspect.iscoroutinefunction(fetch_rows_fn):
            # Bu fonksiyon async ise, caller tarafında await edilecek; burada işleme sokmuyoruz.
            return []
        rows = fetch_rows_fn(ctx) if fetch_rows_fn else []
    except Exception:
        rows = []

    tickers: List[str] = []
    for r in (rows or []):
        t = (r.get("ticker") or r.get("symbol") or "").strip().upper()
        if t:
            tickers.append(t)
    return [_norm_symbol(x) for x in tickers if _norm_symbol(x)]


async def steady_trend_job(ctx, bist_open_fn, fetch_rows_fn, telegram_send_fn) -> None:
    if not STEADY_TREND_ENABLED:
        return
    # --- DEBUG: trading gate ---
    try:
        now_dbg = datetime.now()
        logger.info(
            "STEADY GATE now=%s weekday=%s hour=%s dry=%s",
            now_dbg.isoformat(),
            now_dbg.weekday(),
            now_dbg.hour,
            STEADY_TREND_DRY_RUN,
        )
    except Exception:
        pass
    if STEADY_TREND_CHAT_ID is None:
        return
    if not telegram_send_fn:
        return

    # --- HARD MARKET LOCK (double safety) ---
    # DRY_RUN kapaliyken market disi zamanlarda steady kesin sus.
    if not STEADY_TREND_DRY_RUN:
        # --- DEBUG: trading gate ---
        try:
            now_dbg = datetime.now()
            logger.info(
                "STEADY GATE now=%s weekday=%s hour=%s dry=%s",
                now_dbg.isoformat(),
                now_dbg.weekday(),
                now_dbg.hour,
                STEADY_TREND_DRY_RUN,
            )
        except Exception:
            pass

        # 1) Saat + hafta sonu kilidi (garanti)
        if not _steady_is_trading_time_tr():
            return

        # 2) Ek sigorta: BIST acik fonksiyonu varsa onu da kontrol et (fail-closed)
        try:
            if bist_open_fn:
                ok = bool(bist_open_fn())
                logger.info("STEADY GATE bist_open_fn=%s", ok)
                if not ok:
                    return
        except Exception as e:
            logger.exception("STEADY GATE bist_open_fn error: %s", e)
            return

    state = _load_json(STEADY_STATE_FILE, _default_state())

    # Universe çöz
    universe: List[str] = _parse_tickers_env(STEADY_UNIVERSE_TICKERS)
    if not universe and fetch_rows_fn:
        # fetch_rows_fn async olabilir: burada handle edelim
        try:
            if inspect.iscoroutinefunction(fetch_rows_fn):
                rows = await fetch_rows_fn(ctx)
                tickers: List[str] = []
                for r in (rows or []):
                    t = (r.get("ticker") or r.get("symbol") or "").strip().upper()
                    if t:
                        tickers.append(t)
                universe = [_norm_symbol(x) for x in tickers if _norm_symbol(x)]
            else:
                universe = _resolve_universe(fetch_rows_fn, ctx)
        except Exception:
            universe = []

    if not universe:
        return

    # TV scan (chunked)
    tv_rows = _tv_scan_for_tickers_chunked(universe)
    if not tv_rows:
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

        # Asıl sessiz tırmanış koşulları
        total_pct = float(m.get("total_pct") or 0.0)
        up_ratio = float(m.get("up_ratio") or 0.0)
        max_dd = float(m.get("max_drawdown_pct") or 0.0)

        if total_pct < STEADY_TREND_MIN_PCT or total_pct > STEADY_TREND_MAX_PCT:
            continue
        if up_ratio < STEADY_UP_RATIO_MIN:
            continue
        if max_dd > STEADY_MAX_DRAWDOWN_PCT:
            continue

        # hacim/proxy filtresi
        if not _passes_filters(r):
            continue

        r["trend_total_pct"] = total_pct
        r["trend_up_ratio"] = up_ratio
        r["trend_max_dd"] = max_dd

        r["steady_score"] = _steady_score(r, m)
        picks.append(r)

    # state kaydet (mesaj olmasa bile seriyi koru)
    _save_json(STEADY_STATE_FILE, state)

    if not picks:
        return

    picks.sort(key=lambda x: float(x.get("steady_score") or 0.0), reverse=True)
    top = picks[:3]

    for r in top:
        sym = _norm_symbol(r.get("symbol") or "")
        if not sym:
            continue
        if not _cooldown_ok(state, sym):
            continue

        m = {
            "total_pct": float(r.get("trend_total_pct") or 0.0),
            "up_ratio": float(r.get("trend_up_ratio") or 0.0),
            "max_drawdown_pct": float(r.get("trend_max_dd") or 0.0),
        }

        msg = _format_msg(r, m)

        try:
            if inspect.iscoroutinefunction(telegram_send_fn):
                await telegram_send_fn(ctx, STEADY_TREND_CHAT_ID, msg)
            else:
                telegram_send_fn(ctx, STEADY_TREND_CHAT_ID, msg)
            _mark_sent(state, sym)
        except Exception as e:
            logger.warning("STEADY_TREND send error: %s", e)
            continue

    # son kez state kaydet (cooldown işledi)
    _save_json(STEADY_STATE_FILE, state)


# =========================================================
# Backward compatibility entrypoint (main.py schedule_jobs can call this)
# =========================================================
async def job_steady_trend_scan(context, *args, **kwargs) -> None:
    app = getattr(context, "application", None)
    bot_data = getattr(app, "bot_data", {}) if app else {}

    bist_open_fn = bot_data.get("bist_session_open")
    fetch_rows_fn = bot_data.get("fetch_universe_rows")
    telegram_send_fn = bot_data.get("telegram_send")

    if not telegram_send_fn:
        logger.warning("STEADY_TREND: missing telegram_send adapter")
        return

    # --- HARD MARKET LOCK (TR saat + hafta sonu) + BIST open sigortası
    # DRY_RUN kapalıyken market dışı zamanda steady kesin susar.
    if not STEADY_TREND_DRY_RUN:
        # 1) Saat + hafta sonu kilidi (garanti)
        if not _steady_is_trading_time_tr():
            return

        # 2) Ek sigorta: BIST açık fonksiyonu varsa onu da kontrol et (fail-closed)
        try:
            if bist_open_fn and (not bist_open_fn()):
                return
        except Exception:
            return

    await steady_trend_job(context, bist_open_fn, fetch_rows_fn, telegram_send_fn)
