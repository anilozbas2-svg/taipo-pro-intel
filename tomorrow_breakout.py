from typing import Any, Dict, List, Optional


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _norm_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if ":" in s:
        s = s.split(":")[-1].strip()
    if s.endswith(".IS"):
        s = s[:-3]
    return s


def is_narrow_band(row: Dict[str, Any], max_band_pct: float = 3.0) -> bool:
    band_pct = _safe_float(row.get("band_pct"))
    if band_pct is None:
        return False
    return band_pct <= max_band_pct


def is_near_resistance(
    row: Dict[str, Any],
    resistance_key: str = "resistance",
    price_key: str = "price",
    max_distance_pct: float = 1.0,
) -> bool:
    resistance = _safe_float(row.get(resistance_key))
    price = _safe_float(row.get(price_key))

    if resistance is None or price is None or resistance <= 0:
        return False

    distance_pct = ((resistance - price) / resistance) * 100.0
    if distance_pct < 0:
        distance_pct = 0.0

    return distance_pct <= max_distance_pct


def has_volume_pressure(row: Dict[str, Any], min_volume_ratio: float = 1.4) -> bool:
    volume_ratio = _safe_float(row.get("volume_ratio"))
    if volume_ratio is None:
        return False
    return volume_ratio >= min_volume_ratio


def has_continuity(row: Dict[str, Any], min_continuity: int = 3) -> bool:
    continuity = _safe_int(row.get("continuity"), 0)
    return continuity >= min_continuity


def is_breakout_ready(
    row: Dict[str, Any],
    max_band_pct: float = 3.0,
    max_distance_pct: float = 1.0,
    min_volume_ratio: float = 1.4,
    min_continuity: int = 3,
) -> bool:
    if not is_narrow_band(row, max_band_pct=max_band_pct):
        return False

    if not is_near_resistance(row, max_distance_pct=max_distance_pct):
        return False

    if not has_volume_pressure(row, min_volume_ratio=min_volume_ratio):
        return False

    if not has_continuity(row, min_continuity=min_continuity):
        return False

    return True


def build_breakout_ready_list(
    rows: List[Dict[str, Any]],
    max_band_pct: float = 2.4,
    max_distance_pct: float = 0.75,
    min_volume_ratio: float = 1.25,
    min_continuity: int = 3,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for row in rows or []:
        if is_breakout_ready(
            row,
            max_band_pct=max_band_pct,
            max_distance_pct=max_distance_pct,
            min_volume_ratio=min_volume_ratio,
            min_continuity=min_continuity,
        ):
            out.append(dict(row))

    out.sort(
        key=lambda x: (
            _safe_float(x.get("volume_ratio")) or 0.0,
            _safe_int(x.get("continuity"), 0),
        ),
        reverse=True,
    )
    return out
    
def compute_breakout_score(
    row: Dict[str, Any],
    max_band_pct: float = 3.0,
    max_distance_pct: float = 1.0,
    min_volume_ratio: float = 1.4,
    min_continuity: int = 3,
) -> int:
    score = 0

    band_pct = _safe_float(row.get("band_pct"))
    resistance = _safe_float(row.get("resistance"))
    price = _safe_float(row.get("price"))
    volume_ratio = _safe_float(row.get("volume_ratio"))
    continuity = _safe_int(row.get("continuity"), 0)

    # 1) Dar bant skoru
    if band_pct is not None:
        if band_pct <= 1.5:
            score += 3
        elif band_pct <= max_band_pct:
            score += 2

    # 2) Dirence yakınlık skoru
    if resistance is not None and price is not None and resistance > 0:
        distance_pct = ((resistance - price) / resistance) * 100.0
        if distance_pct < 0:
            distance_pct = 0.0

        if distance_pct <= 0.5:
            score += 3
        elif distance_pct <= max_distance_pct:
            score += 2

    # 3) Hacim skoru
    if volume_ratio is not None:
        if volume_ratio >= 1.8:
            score += 3
        elif volume_ratio >= min_volume_ratio:
            score += 2
        elif volume_ratio >= 1.2:
            score += 1

    # 4) Continuity skoru
    if continuity >= (min_continuity + 1):
        score += 2
    elif continuity >= min_continuity:
        score += 1

    return min(score, 10)
    
def compute_accumulation_score(
    row: Dict[str, Any],
    max_band_pct: float = 35.0,
    min_volume_ratio: float = 1.20,
    max_pct_change: float = 2.0,
    min_continuity: int = 2,
) -> int:

    score = 0

    band_pct = _safe_float(row.get("band_pct"))
    volume_ratio = _safe_float(row.get("volume_ratio"))
    pct_change = _safe_float(row.get("pct_change"))
    if pct_change is None:
        pct_change = _safe_float(row.get("change"))

    continuity = _safe_int(row.get("continuity"), 0)

    # 🔹 1) BAND (SIKIŞMA KALİTESİ)
    if band_pct is not None:
        if band_pct <= 12:
            score += 3
        elif band_pct <= 22:
            score += 2
        elif band_pct <= max_band_pct:
            score += 1

    # 🔹 2) HACİM (TAHTACI TOPLAMA)
    if volume_ratio is not None:
        if 1.25 <= volume_ratio <= 3.5:
            score += 3   # ideal toplama
        elif volume_ratio >= 1.2:
            score += 2
        elif volume_ratio >= 1.05:
            score += 1

        # ⚠️ aşırı hacim = breakout olabilir (toplama değil)
        if volume_ratio > 4:
            score -= 1

    # 🔹 3) FİYAT HAREKETİ (SAKİN TOPLAMA)
    if pct_change is not None:
        abs_pct = abs(pct_change)

        if abs_pct <= 0.8:
            score += 3
        elif abs_pct <= 1.2:
            score += 2
        elif abs_pct <= max_pct_change:
            score += 1

        # ⚠️ fazla oynak = accumulation değil
        if abs_pct > 3:
            score -= 1

    # 🔹 4) CONTINUITY (YATAY SÜRE)
    if continuity >= 6:
        score += 2
    elif continuity >= min_continuity:
        score += 1

    # 🔹 5) FAKE BREAKOUT FİLTRESİ
        breakout_score = _safe_float(row.get("breakout_score"))
        if breakout_score is not None:
            if breakout_score > 7:
                score -= 2
            elif breakout_score > 5:
                score -= 1

    return max(0, min(score, 10))
    
def compute_v5_entry_score(
    row: Dict[str, Any],
    min_close_pos: float = 80.0,
    strong_close_pos: float = 90.0,
    min_burst: float = 1.80,
    soft_max_pct_change: float = 3.0,
    hard_volume_spike: float = 4.0,
) -> int:
    score = 0

    close_pos = _safe_float(row.get("close_pos"))
    burst = _safe_float(row.get("burst"))
    volume_ratio = _safe_float(row.get("volume_ratio"))

    pct_change = _safe_float(row.get("pct_change"))
    if pct_change is None:
        pct_change = _safe_float(row.get("change"))

    breakout_score = _safe_float(row.get("breakout_score"))

    # 1) Üst banda yakın kapanış
    if close_pos is not None:
        if close_pos >= strong_close_pos:
            score += 2
        elif close_pos >= min_close_pos:
            score += 1

    # 2) Ani ivme / burst
    if burst is not None and burst >= min_burst:
        score += 1

    # 3) Aşırı hacim spike cezası
    if volume_ratio is not None:
        if volume_ratio > 4.0:
            score -= 1
        elif volume_ratio >= 3.5:
            score -= 1

    # 4) Günlük kaçış cezası
    if pct_change is not None:
        abs_pct = abs(pct_change)

        if abs_pct >= 3.0:
            score -= 2
        elif abs_pct >= 2.2:
            score -= 2
        elif abs_pct >= 1.6:
            score -= 1

    # 5) Fake breakout cezası
    if breakout_score is not None:
        if breakout_score > 7:
            score -= 2
        elif breakout_score > 5:
            score -= 1

    return max(0, min(score, 3))
    
def is_quality_entry_candidate(row: Dict[str, Any]) -> bool:
    close_pos = _safe_float(row.get("close_pos"))
    volume_ratio = _safe_float(row.get("volume_ratio"))

    pct_change = _safe_float(row.get("pct_change"))
    if pct_change is None:
        pct_change = _safe_float(row.get("change"))

    if close_pos is None or close_pos < 80:
        return False

    if volume_ratio is None or not (1.20 <= volume_ratio <= 3.20):
        return False

    if pct_change is None or abs(pct_change) > 1.80:
        return False

    return True
