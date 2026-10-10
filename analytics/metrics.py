"""Pure analytics helpers. Spark jobs implement the same formulas with window functions."""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Optional


def rolling_mean(values: list[Optional[float]], window: int = 7) -> list[Optional[float]]:
    out: list[Optional[float]] = []
    for i in range(len(values)):
        chunk = [v for v in values[max(0, i - window + 1) : i + 1] if v is not None]
        out.append(sum(chunk) / len(chunk) if chunk else None)
    return out


def case_fatality_ratio(
    total_deaths: Optional[float], total_cases: Optional[float]
) -> Optional[float]:
    if total_cases is None or total_deaths is None or total_cases <= 0:
        return None
    return (total_deaths / total_cases) * 100.0


def percentage_change(recent: Optional[float], previous: Optional[float]) -> Optional[float]:
    if recent is None or previous is None or previous == 0:
        return None
    return ((recent - previous) / previous) * 100.0


def attach_rolling_and_cfr(rows: list[dict], window: int = 7) -> list[dict]:
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[(row["iso_code"], row.get("source_type", "historical"))].append(row)

    result: list[dict] = []
    for _, group in grouped.items():
        group.sort(key=lambda r: r["date"])
        cases = [r.get("daily_cases", r.get("new_cases")) for r in group]
        deaths = [r.get("daily_deaths", r.get("new_deaths")) for r in group]
        r7c = rolling_mean(cases, window)
        r7d = rolling_mean(deaths, window)
        for row, rc, rd in zip(group, r7c, r7d):
            enriched = dict(row)
            enriched["rolling_7d_cases"] = rc
            enriched["rolling_7d_deaths"] = rd
            enriched["case_fatality_ratio"] = case_fatality_ratio(
                row.get("total_deaths"), row.get("total_cases")
            )
            result.append(enriched)
    return result


def regional_increase_signals(
    rows: Iterable[dict],
    threshold_pct: float = 10.0,
    min_obs: int = 7,
    window: int = 7,
) -> list[dict]:
    """
    Compare the latest `window` daily-case average with the preceding window.
    These are regional increase signals, not outbreak predictions.
    """
    by_region: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        by_region[(row["iso_code"], row.get("source_type", "historical"))].append(row)

    signals: list[dict] = []
    for (iso, source), group in by_region.items():
        group.sort(key=lambda r: r["date"])
        values = [
            r.get("daily_cases", r.get("new_cases"))
            for r in group
            if r.get("daily_cases", r.get("new_cases")) is not None
        ]
        if len(values) < min_obs:
            continue
        recent_vals = values[-window:]
        previous_vals = values[-(window * 2) : -window]
        if len(recent_vals) < min_obs or len(previous_vals) < min_obs:
            continue
        recent = sum(recent_vals) / len(recent_vals)
        previous = sum(previous_vals) / len(previous_vals)
        pct = percentage_change(recent, previous)
        increasing = pct is not None and pct > threshold_pct
        latest = group[-1]
        if pct is None:
            why = (
                f"Insufficient previous-period baseline (previous 7-day average is "
                f"{previous:.2f}); percentage change is undefined."
            )
        elif increasing:
            why = (
                f"Latest 7-day average of daily reported cases ({recent:.2f}) is "
                f"{pct:.1f}% higher than the preceding 7-day average ({previous:.2f}), "
                f"above the {threshold_pct:.0f}% configuration threshold. "
                "This is a statistical increase signal, not a medical diagnosis."
            )
        else:
            why = (
                f"Latest 7-day average ({recent:.2f}) vs preceding ({previous:.2f}) "
                f"changed by {pct:.1f}%, which is not above {threshold_pct:.0f}%."
            )
        signals.append(
            {
                "iso_code": iso,
                "location": latest.get("location"),
                "date": latest.get("date"),
                "recent_7d_avg": round(recent, 4),
                "previous_7d_avg": round(previous, 4),
                "percentage_change": None if pct is None else round(pct, 4),
                "is_increasing": increasing,
                "observation_count": len(recent_vals) + len(previous_vals),
                "threshold_pct": threshold_pct,
                "explanation": why,
                "source_type": source,
            }
        )

    signals.sort(
        key=lambda s: (s["percentage_change"] is None, -(s["percentage_change"] or 0))
    )
    return signals
