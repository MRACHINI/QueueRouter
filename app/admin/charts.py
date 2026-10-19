"""Small server-rendered SVG charts (no JavaScript or CDN; colors come from admin.css classes)."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from html import escape

WIDTH, HEIGHT = 720, 200
LEFT, RIGHT, TOP, BOTTOM = 56, 8, 10, 26


def _nice_max(value: float) -> float:
    """Round an axis maximum up to 1, 2 or 5 x 10^n."""
    if value <= 0:
        return 1
    exponent = 10 ** math.floor(math.log10(value))
    for step in (1, 2, 5, 10):
        if value <= step * exponent:
            return step * exponent
    return 10 * exponent


def compact(value: float) -> str:
    for limit, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(value) >= limit:
            return f"{value / limit:.3g}{suffix}"
    return f"{value:.3g}" if value != int(value) else f"{int(value)}"


def day_range(days: int, today: date | None = None) -> list[str]:
    today = today or datetime.now(UTC).date()
    return [(today - timedelta(days=n)).isoformat() for n in range(days - 1, -1, -1)]


def stacked_bars(title: str, labels: list[str], series: list[tuple[str, str, list[float]]], money: bool = False) -> str:
    """Stacked bar chart. ``series`` = [(name, css class, values per label)]."""
    totals = [sum(s[2][i] for s in series) for i in range(len(labels))]
    top = _nice_max(max(totals, default=0))
    plot_w, plot_h = WIDTH - LEFT - RIGHT, HEIGHT - TOP - BOTTOM
    slot = plot_w / max(len(labels), 1)
    bar = max(1.0, min(36.0, slot * 0.7))
    fmt = (lambda v: f"${compact(v)}") if money else compact
    parts = [f'<svg class="chart" viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-label="{escape(title)}" >']
    for i in range(5):  # grid lines with axis labels
        value = top * i / 4
        y = TOP + plot_h - plot_h * i / 4
        parts.append(f'<line class="grid" x1="{LEFT}" x2="{WIDTH - RIGHT}" y1="{y:.1f}" y2="{y:.1f}"/>')
        parts.append(f'<text class="axis" x="{LEFT - 6}" y="{y + 4:.1f}" text-anchor="end">{fmt(value)}</text>')
    label_every = max(1, math.ceil(len(labels) / 10))
    for i, label in enumerate(labels):
        x = LEFT + slot * i + (slot - bar) / 2
        y = TOP + plot_h
        tip = [label] + [f"{name}: {fmt(values[i])}" for name, _, values in series]
        parts.append(f"<g><title>{escape(' · '.join(tip))}</title>")
        parts.append(f'<rect class="hit" x="{LEFT + slot * i:.1f}" y="{TOP}" width="{slot:.1f}" height="{plot_h}"/>')
        for _, css, values in series:
            h = plot_h * values[i] / top
            if h <= 0:
                continue
            y -= h
            parts.append(f'<rect class="{css}" x="{x:.1f}" y="{y:.1f}" width="{bar:.1f}" height="{h:.1f}"/>')
        parts.append("</g>")
        if i % label_every == 0:
            short = label[5:] if len(label) == 10 else label  # YYYY-MM-DD -> MM-DD
            cx = LEFT + slot * i + slot / 2
            parts.append(
                f'<text class="axis" x="{cx:.1f}" y="{HEIGHT - 8}" text-anchor="middle">{escape(short)}</text>'
            )
    parts.append("</svg>")
    legend = "".join(f'<span class="legend {css}">{escape(name)}</span>' for name, css, _ in series)
    return f'<figure class="chart-box"><figcaption>{escape(title)} {legend}</figcaption>{"".join(parts)}</figure>'


def usage_charts(rows: list[dict], days: int, today: date | None = None) -> list[str]:
    """Per-day charts for the Usage page from its rows (one per day/client[/model])."""
    labels = day_range(days, today)
    index = {d: i for i, d in enumerate(labels)}

    def per_day(key) -> list[float]:
        values = [0.0] * len(labels)
        for r in rows:
            i = index.get(r["day"])
            if i is not None:
                values[i] += key(r) or 0
        return values

    charts = [
        stacked_bars(
            "Tokens per day",
            labels,
            [
                ("in", "s1", per_day(lambda r: r["prompt_tokens"])),
                ("out", "s2", per_day(lambda r: r["completion_tokens"])),
            ],
        ),
        stacked_bars(
            "Requests per day",
            labels,
            [
                ("ok", "s1", per_day(lambda r: r["requests"] - r["errors"])),
                ("errors", "s3", per_day(lambda r: r["errors"])),
            ],
        ),
    ]
    if any(r.get("cost") for r in rows):
        charts.append(
            stacked_bars(
                "Cost per day at official prices", labels, [("cost", "s2", per_day(lambda r: r["cost"]))], money=True
            )
        )
    return charts
