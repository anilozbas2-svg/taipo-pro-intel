"""
TAIPO PRO INTEL - Core Structure
Buraya ileride: bist/crypto market modülleri, strateji motoru, scoring vs eklenecek.
"""

from dataclasses import dataclass

@dataclass
class AppConfig:
    market: str = "BIST100"   # "BIST100" veya "CRYPTO"
    mode: str = "HYBRID"      # "A", "B", "HYBRID"
    interval_minutes: int = 60
