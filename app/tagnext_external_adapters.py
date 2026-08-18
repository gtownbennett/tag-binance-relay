"""Source-aware external forecast extraction for TAGGER.

The adapter boundary deliberately operates on parsed structure, never raw-page
"first dollar after year" matches.  Structured JSON and table rows take
precedence; the conservative text fallback requires horizon and value labels in
the same compact block and is disabled for unknown assets.
"""
from __future__ import annotations

import calendar
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlsplit


TAG_CONTRACT = "0x208bf3e7da9639f1eaefa2de78c23396b0682025"
PARSER_FRAMEWORK_VERSION = "tagnext-source-adapters-v2"

TARGET_SEMANTICS = {
    "point_at_deadline", "year_end", "mid_year", "period_average",
    "period_minimum", "period_maximum", "range_for_period", "possible_high",
    "possible_low", "central_scenario", "conditional_bull", "conditional_bear",
    "direction_only", "probability", "scenario_calculator",
}


def _number(value: Any) -> float | None:
    try:
        result = float(str(value).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return None
    return result if result == result and result >= 0 else None


def _utc(value: datetime | str | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _year_bounds(year: int) -> tuple[datetime, datetime]:
    return (
        datetime(year, 1, 1, tzinfo=timezone.utc),
        datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
    )


def normalize_horizon(
    label: str, *, issue_at: datetime | None = None,
) -> dict[str, Any]:
    """Normalize a published label while preserving the original wording."""
    original = " ".join(str(label or "").split())
    compact = original.lower().replace("–", "-")
    issued = issue_at or datetime.now(timezone.utc)
    # Preserve the case-sensitive finance convention: ``3M``/``6M`` are
    # months, while ``3m`` is minutes.  Longer spellings remain unambiguous.
    compact_for_matching = compact
    case_sensitive_month = re.fullmatch(r"(\d+)M", original)
    if case_sensitive_month:
        compact_for_matching = f"{case_sensitive_month.group(1)}mo"
    calendar_year = re.fullmatch(r"20\d{2}", compact)
    if calendar_year:
        year = int(compact)
        start, end = _year_bounds(year)
        return {
            "originalHorizonLabel": original, "normalizedHorizon": str(year),
            "periodStart": start, "periodEnd": end, "deadline": end,
        }
    patterns = (
        (r"^(\d+)\s*(?:minute|min|m)s?$", "m", lambda n: timedelta(minutes=n)),
        (r"^(\d+)\s*(?:hour|hr|h)s?$", "h", lambda n: timedelta(hours=n)),
        (r"^(\d+)\s*(?:day|d)s?$", "d", lambda n: timedelta(days=n)),
        (r"^(\d+)\s*(?:week|wk|w)s?$", "w", lambda n: timedelta(days=7 * n)),
        (r"^(\d+)\s*(?:month|mo)s?$", "mo", lambda n: timedelta(days=30 * n)),
        (r"^(\d+)\s*(?:year|yr|y)s?$", "y", lambda n: timedelta(days=365 * n)),
    )
    aliases = {"daily": "1d", "weekly": "7d", "monthly": "1mo", "24h": "24h"}
    compact = aliases.get(compact_for_matching, compact_for_matching)
    for pattern, suffix, duration in patterns:
        match = re.fullmatch(pattern, compact)
        if match:
            amount = int(match.group(1))
            delta = duration(amount)
            canonical = f"{amount}{suffix}"
            if suffix == "w":
                canonical = f"{amount * 7}d"
            elif suffix == "mo" and amount in {3, 6}:
                canonical = f"{amount}m"
            elif suffix == "y" and amount == 1:
                canonical = "1y"
            return {
                "originalHorizonLabel": original, "normalizedHorizon": canonical,
                "periodStart": issued, "periodEnd": issued + delta,
                "deadline": issued + delta,
            }
    return {
        "originalHorizonLabel": original, "normalizedHorizon": compact or None,
        "periodStart": None, "periodEnd": None, "deadline": None,
    }


class _ForecastHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip = 0
        self._cell: list[str] | None = None
        self._row: list[str] | None = None
        self.tables: list[list[str]] = []
        self.visible: list[str] = []
        self.script_type: str | None = None
        self.script_text: list[str] = []
        self.structured_scripts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key.lower(): value for key, value in attrs}
        lower = tag.lower()
        if lower in {"style", "svg", "noscript", "nav", "footer", "form"}:
            self._skip += 1
        if lower == "script":
            self.script_type = str(attributes.get("type") or "").lower()
            self.script_text = []
        if lower == "tr":
            self._row = []
        if lower in {"td", "th"}:
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        lower = tag.lower()
        if lower in {"td", "th"} and self._cell is not None:
            if self._row is not None:
                self._row.append(" ".join(" ".join(self._cell).split()))
            self._cell = None
        if lower == "tr" and self._row is not None:
            if any(self._row):
                self.tables.append(self._row)
            self._row = None
        if lower == "script":
            if self.script_type in {"application/ld+json", "application/json"}:
                self.structured_scripts.append("".join(self.script_text))
            self.script_type = None
            self.script_text = []
        if lower in {"style", "svg", "noscript", "nav", "footer", "form"} and self._skip:
            self._skip -= 1

    def handle_data(self, data: str) -> None:
        if self.script_type is not None:
            self.script_text.append(data)
            return
        if self._skip:
            return
        value = " ".join(data.split())
        if not value:
            return
        if self._cell is not None:
            self._cell.append(value)
        self.visible.append(value)


@dataclass(frozen=True)
class ForecastDocument:
    url: str
    fetched_at: datetime
    visible_text: str
    table_rows: tuple[tuple[str, ...], ...]
    structured_data: tuple[Any, ...]
    response_hash: str | None = None
    headers: Mapping[str, str] = field(default_factory=dict)


def parse_document(
    *, url: str, html: str, fetched_at: datetime,
    response_hash: str | None = None, headers: Mapping[str, str] | None = None,
) -> ForecastDocument:
    parser = _ForecastHTMLParser()
    parser.feed(html)
    structured: list[Any] = []
    for raw in parser.structured_scripts:
        try:
            structured.append(json.loads(raw))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
    return ForecastDocument(
        url=url, fetched_at=fetched_at,
        visible_text=" ".join(parser.visible),
        table_rows=tuple(tuple(row) for row in parser.tables),
        structured_data=tuple(structured), response_hash=response_hash,
        headers=dict(headers or {}),
    )


def _walk(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        for item in value.values():
            yield from _walk(item)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            yield from _walk(item)


def _direction(block: str) -> str | None:
    lowered = block.lower()
    higher = bool(re.search(r"\b(bullish|increase|rise|higher|upside|gain)\b", lowered))
    lower = bool(re.search(r"\b(bearish|decrease|fall|lower|downside|decline)\b", lowered))
    return "HIGHER" if higher and not lower else "LOWER" if lower and not higher else None


def _claim(
    *, source_id: str, label: str, semantics: str,
    value: float | None = None, low: float | None = None, high: float | None = None,
    direction: str | None = None, issue_at: datetime | None = None,
    update_at: datetime | None = None, probability: float | None = None,
    scenario_class: str | None = None, methodology_version: str | None = None,
    conditional_trigger: str | None = None, gradeability: str = "point",
    target_currency: str = "USD", native_value: float | None = None,
    native_low: float | None = None, native_high: float | None = None,
) -> dict[str, Any]:
    horizon = normalize_horizon(label, issue_at=issue_at or update_at)
    if semantics not in TARGET_SEMANTICS:
        raise ValueError(f"unsupported target semantics: {semantics}")
    return {
        "sourceId": source_id, "assetAuthority": "tagger",
        **horizon, "targetSemantics": semantics,
        "targetPrice": value, "targetLow": low, "targetHigh": high,
        "targetCurrency": target_currency,
        "targetNativePrice": native_value,
        "targetNativeLow": native_low,
        "targetNativeHigh": native_high,
        "direction": direction, "sourceIssueAt": issue_at,
        "sourceUpdateAt": update_at, "probability": probability,
        "scenarioClass": scenario_class,
        "methodologyVersion": methodology_version,
        "conditionalTrigger": conditional_trigger,
        "gradeability": gradeability,
    }


def _table_claims(
    source_id: str, rows: Sequence[Sequence[str]], *, issue_at: datetime | None,
    adapter_id: str,
) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    header: list[str] = []
    for raw_row in rows:
        row = [" ".join(str(value).split()) for value in raw_row]
        joined = " | ".join(row)
        if not header and re.search(r"year|horizon|period|date", joined, re.I):
            header = [value.lower() for value in row]
            continue
        horizon_cell = next((value for value in row if re.fullmatch(r"20\d{2}|\d+\s*(?:min|m|h|d|day|week|month|year)s?", value, re.I)), None)
        if not horizon_cell:
            continue
        labels = header if len(header) == len(row) else [""] * len(row)
        values = [(labels[index], _number(cell)) for index, cell in enumerate(row) if cell != horizon_cell]
        values = [(label, value) for label, value in values if value is not None]
        if not values:
            continue
        emitted = False
        for label, value in values:
            semantic = None
            gradeability = "period" if re.fullmatch(r"20\d{2}", horizon_cell) else "point"
            if re.search(r"(?:low|bear)\s+case", label, re.I):
                semantic = "conditional_bear"
                gradeability = "scenario"
            elif re.search(r"(?:high|bull)\s+case", label, re.I):
                semantic = "conditional_bull"
                gradeability = "scenario"
            elif re.search(r"(?:mid|base|central)\s+case", label, re.I):
                semantic = "central_scenario"
                gradeability = "scenario"
            elif re.search(r"min|low", label, re.I):
                semantic = "period_minimum" if gradeability == "period" else "possible_low"
            elif re.search(r"max|high", label, re.I):
                semantic = "period_maximum" if gradeability == "period" else "possible_high"
            elif re.search(r"avg|average|mean", label, re.I):
                semantic = "period_average"
            elif re.search(r"mid.?year", label, re.I):
                semantic = "mid_year"
            elif re.search(r"end|close|target|price|prediction|forecast", label, re.I):
                semantic = "year_end" if re.fullmatch(r"20\d{2}", horizon_cell) else "point_at_deadline"
            if semantic:
                claims.append(_claim(
                    source_id=source_id, label=horizon_cell, semantics=semantic,
                    value=value, direction=_direction(joined), issue_at=issue_at,
                    methodology_version=adapter_id, gradeability=gradeability,
                ))
                emitted = True
        if not emitted and len(values) == 1:
            claims.append(_claim(
                source_id=source_id, label=horizon_cell,
                semantics="year_end" if re.fullmatch(r"20\d{2}", horizon_cell) else "point_at_deadline",
                value=values[0][1], direction=_direction(joined), issue_at=issue_at,
                methodology_version=adapter_id,
                gradeability="period" if re.fullmatch(r"20\d{2}", horizon_cell) else "point",
            ))
    return claims


def _structured_claims(
    source_id: str, documents: Sequence[Any], *, issue_at: datetime | None,
    adapter_id: str,
) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    horizon_keys = ("year", "horizon", "period", "label", "date")
    for item in _walk(documents):
        horizon = next((str(item[key]) for key in horizon_keys if item.get(key) is not None), None)
        if not horizon or not re.search(r"20\d{2}|\d+\s*(?:min|m|h|d|day|week|month|year)", horizon, re.I):
            continue
        mapping = (
            (("minimum", "min", "low"), "period_minimum"),
            (("maximum", "max", "high"), "period_maximum"),
            (("average", "avg", "mean"), "period_average"),
            (("end", "yearEnd", "target", "price", "value"), "year_end" if re.fullmatch(r"20\d{2}", horizon) else "point_at_deadline"),
        )
        item_issue_at = issue_at
        item_update_at: datetime | None = None
        for key in ("datePublished", "publishedAt", "issueDate", "createdAt"):
            if item.get(key):
                try:
                    item_issue_at = _utc(str(item[key]))
                    break
                except (TypeError, ValueError):
                    pass
        for key in ("dateModified", "updatedAt", "updateDate", "lastUpdated"):
            if item.get(key):
                try:
                    item_update_at = _utc(str(item[key]))
                    break
                except (TypeError, ValueError):
                    pass
        for keys, semantics in mapping:
            value = next((_number(item[key]) for key in keys if item.get(key) is not None), None)
            if value is not None:
                claims.append(_claim(
                    source_id=source_id, label=horizon, semantics=semantics,
                    value=value, issue_at=item_issue_at, update_at=item_update_at,
                    methodology_version=adapter_id,
                    gradeability="period" if semantics.startswith("period_") else "point",
                ))
    return claims


def _conservative_labeled_claims(
    source_id: str, text: str, *, issue_at: datetime | None, adapter_id: str,
) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    blocks = re.split(r"(?<=[.!?])\s+|\s{2,}", text)
    for block in blocks:
        if len(block) > 600 or not re.search(r"forecast|prediction|target|minimum|maximum|average|scenario", block, re.I):
            continue
        horizon_match = re.search(r"\b(20\d{2}|\d+\s*(?:minutes?|mins?|hours?|hrs?|days?|weeks?|months?|years?|[mhdwy]))\b", block, re.I)
        value_match = re.search(r"(?:\$|USD\s*)(0?\.\d+|\d+(?:\.\d+)?)", block, re.I)
        if not horizon_match or not value_match:
            continue
        semantics = (
            "period_average" if re.search(r"average|mean", block, re.I)
            else "period_minimum" if re.search(r"minimum|\bmin\b", block, re.I)
            else "period_maximum" if re.search(r"maximum|\bmax\b", block, re.I)
            else "year_end" if re.search(r"year.?end|end of", block, re.I)
            else "point_at_deadline"
        )
        claims.append(_claim(
            source_id=source_id, label=horizon_match.group(1), semantics=semantics,
            value=_number(value_match.group(1)), direction=_direction(block),
            issue_at=issue_at, methodology_version=f"{adapter_id}:conservative_fallback",
            gradeability="period" if semantics.startswith("period_") else "point",
        ))
    return claims


def _paired_header_table_claims(
    source_id: str, rows: Sequence[Sequence[str]], *, issue_at: datetime | None,
    adapter_id: str,
) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    for index, header in enumerate(rows[:-1]):
        values = rows[index + 1]
        if len(header) != len(values):
            continue
        for label, raw_value in zip(header, values):
            horizon = re.search(r"(?:TAG\s+in\s+)?(20\d{2})|\b(24H|\d+\s*(?:days?|weeks?|months?|years?))\b", label, re.I)
            value = _number(raw_value)
            if not horizon or value is None or re.search(r"current", label, re.I):
                continue
            horizon_label = next(group for group in horizon.groups() if group)
            claims.append(_claim(
                source_id=source_id, label=horizon_label,
                semantics="scenario_calculator", value=value, issue_at=issue_at,
                scenario_class="user_input_growth_calculator",
                methodology_version=f"{adapter_id}:paired_table",
                conditional_trigger="default growth input shown by source",
                gradeability="scenario",
            ))
    return claims


def _wallet_number(value: str) -> float | None:
    ordinary = re.search(r"\$\s*(0?\.\d+|\d+(?:\.\d+)?)", value)
    micro = re.search(r"\$\s*0\.0\s+(\d+)\s+(\d+)", value)
    if micro:
        return _number("0." + ("0" * int(micro.group(1))) + micro.group(2))
    return _number(ordinary.group(1)) if ordinary else None


def _walletinvestor_claims(
    source_id: str, document: ForecastDocument, *, issue_at: datetime | None,
    adapter_id: str,
) -> list[dict[str, Any]]:
    text = document.visible_text
    claims: list[dict[str, Any]] = []
    summary_patterns = (
        (r"7-Day\s+(\$\s*0\.0\s+\d+\s+\d+|\$\s*[0-9.]+)", "7D"),
        (r"3-Month\s+(\$\s*0\.0\s+\d+\s+\d+|\$\s*[0-9.]+)", "3M"),
        (r"1-Year\s+(\$\s*0\.0\s+\d+\s+\d+|\$\s*[0-9.]+)", "1Y"),
        (r"1-year target\s+(\$\s*0\.0\s+\d+\s+\d+|\$\s*[0-9.]+)", "1Y"),
    )
    for pattern, label in summary_patterns:
        match = re.search(pattern, text, re.I)
        value = _wallet_number(match.group(1)) if match else None
        if value is not None:
            claims.append(_claim(
                source_id=source_id, label=label, semantics="point_at_deadline",
                value=value, issue_at=issue_at,
                methodology_version=f"{adapter_id}:summary", gradeability="point",
            ))
    for scenario, probability in (("Bear", 0.20), ("Base", 0.55), ("Bull", 0.25)):
        match = re.search(
            rf"{scenario} case\s+\d+% likely\s+(\$\s*0\.0\s+\d+\s+\d+|\$\s*[0-9.]+)",
            text, re.I,
        )
        value = _wallet_number(match.group(1)) if match else None
        if value is not None:
            semantics = "conditional_bear" if scenario == "Bear" else "conditional_bull" if scenario == "Bull" else "central_scenario"
            claims.append(_claim(
                source_id=source_id, label="1Y", semantics=semantics,
                value=value, probability=probability, issue_at=issue_at,
                scenario_class=f"{scenario.lower()}_case",
                methodology_version=f"{adapter_id}:probability_scenario",
                conditional_trigger="source model scenario",
            ))
    month_names = {name[:3].lower(): index for index, name in enumerate(calendar.month_name) if name}
    for match in re.finditer(
        r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(20\d{2})\s+"
        r"(\$\s*0\.0\s+\d+\s+\d+|\$\s*[0-9.]+)",
        text, re.I,
    ):
        value = _wallet_number(match.group(3))
        if value is None:
            continue
        year = int(match.group(2))
        month = month_names[match.group(1).lower()]
        last_day = calendar.monthrange(year, month)[1]
        claim = _claim(
            source_id=source_id, label=f"{match.group(1)} {year}",
            semantics="point_at_deadline", value=value, issue_at=issue_at,
            methodology_version=f"{adapter_id}:monthly_projection",
        )
        claim.update({
            "normalizedHorizon": f"{year:04d}-{month:02d}",
            "periodStart": datetime(year, month, 1, tzinfo=timezone.utc),
            "periodEnd": datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc),
            "deadline": datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc),
        })
        claims.append(claim)
    return deduplicate_claims(claims)


def _cmc_ai_claims(
    source_id: str, document: ForecastDocument, *, adapter_id: str,
) -> list[dict[str, Any]]:
    text = document.visible_text
    if not re.search(r"Tagger\s*\(TAG\)\s*Price Prediction By CMC AI", text, re.I):
        return []
    update_at = None
    match = re.search(r"(\d{1,2}\s+[A-Za-z]+\s+20\d{2}\s+\d{1,2}:\d{2}[AP]M)", text)
    if match:
        try:
            update_at = datetime.strptime(match.group(1), "%d %B %Y %I:%M%p").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return [_claim(
        source_id=source_id, label="forward outlook",
        semantics="direction_only", direction=_direction(text),
        issue_at=update_at, update_at=update_at,
        methodology_version=f"{adapter_id}:qualitative_tldr",
        gradeability="qualitative",
    )]


def _annual_rendered_min_avg_max_claims(
    source_id: str, document: ForecastDocument, *, issue_at: datetime,
    adapter_id: str,
) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    for year, minimum, average, maximum in re.findall(
        r"\b(20\d{2})\s+\$\s*([0-9.]+)\s+\$\s*([0-9.]+)\s+\$\s*([0-9.]+)",
        document.visible_text,
    ):
        for semantics, raw_value in (
            ("period_minimum", minimum),
            ("period_average", average),
            ("period_maximum", maximum),
        ):
            value = _number(raw_value)
            if value is not None:
                claims.append(_claim(
                    source_id=source_id, label=year, semantics=semantics,
                    value=value, issue_at=issue_at,
                    methodology_version=f"{adapter_id}:rendered_annual_table",
                    gradeability="period",
                ))
    return deduplicate_claims(claims)


def _tradersunion_rendered_claims(
    source_id: str, document: ForecastDocument, *, issue_at: datetime,
    adapter_id: str,
) -> list[dict[str, Any]]:
    text = document.visible_text
    claims: list[dict[str, Any]] = []
    months = {name.lower(): index for index, name in enumerate(calendar.month_name) if name}
    for month_name, year_text, minimum, maximum, average in re.findall(
        r"\b(" + "|".join(calendar.month_name[1:]) + r")\s+(20\d{2})\s+"
        r"\$\s*([0-9.]+)\s+\$\s*([0-9.]+)\s+\$\s*([0-9.]+)",
        text, re.I,
    ):
        year = int(year_text)
        month = months[month_name.lower()]
        last_day = calendar.monthrange(year, month)[1]
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        end = datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc)
        for semantics, raw_value in (
            ("period_minimum", minimum),
            ("period_maximum", maximum),
            ("period_average", average),
        ):
            claim = _claim(
                source_id=source_id, label=f"{month_name} {year}", semantics=semantics,
                value=_number(raw_value), issue_at=issue_at,
                methodology_version=f"{adapter_id}:rendered_monthly_table",
                gradeability="period",
            )
            claim.update({
                "normalizedHorizon": f"{year:04d}-{month:02d}",
                "periodStart": start, "periodEnd": end, "deadline": end,
            })
            claims.append(claim)
    long_term_table = text
    long_term_header = re.search(
        r"Year\s+Price in the middle of the year\s+Price at the end of the year",
        text, re.I,
    )
    if long_term_header:
        long_term_table = text[long_term_header.end():]
        long_term_table = re.split(r"How our forecasts work", long_term_table, maxsplit=1, flags=re.I)[0]
    else:
        long_term_table = ""
    for year, mid_year, year_end in re.findall(
        r"\b(20\d{2})\s+\$\s*([0-9.]+)\s+\$\s*([0-9.]+)", long_term_table
    ):
        claims.append(_claim(
            source_id=source_id, label=year, semantics="mid_year",
            value=_number(mid_year), issue_at=issue_at,
            methodology_version=f"{adapter_id}:rendered_long_term_table",
        ))
        claims.append(_claim(
            source_id=source_id, label=year, semantics="year_end",
            value=_number(year_end), issue_at=issue_at,
            methodology_version=f"{adapter_id}:rendered_long_term_table",
        ))
    return deduplicate_claims(claims)


def _gate_native_currency_claims(
    source_id: str, document: ForecastDocument, *, issue_at: datetime,
    adapter_id: str,
) -> list[dict[str, Any]]:
    """Freeze Gate's CNY table without silently relabeling CNY values as USD."""
    claims: list[dict[str, Any]] = []
    pattern = re.compile(
        r"\b(20\d{2})\s*[\t ]+¥\s*([0-9.]+)\s*[\t ]+¥\s*([0-9.]+)\s*[\t ]+¥\s*([0-9.]+)",
        re.I,
    )
    for match in pattern.finditer(document.visible_text):
        year, minimum, maximum, average = match.groups()
        for semantics, native_value in (
            ("period_minimum", _number(minimum)),
            ("period_maximum", _number(maximum)),
            ("period_average", _number(average)),
        ):
            if native_value is None:
                continue
            claims.append(_claim(
                source_id=source_id, label=year, semantics=semantics,
                issue_at=issue_at, methodology_version=adapter_id,
                gradeability="native_currency_period",
                target_currency="CNY", native_value=native_value,
            ))
    return deduplicate_claims(claims)


def _scenario_calculator_text_claims(
    source_id: str, document: ForecastDocument, *, issue_at: datetime,
    adapter_id: str,
) -> list[dict[str, Any]]:
    text = document.visible_text
    currency = "CAD" if re.search(r"\bCAD scenarios\b", text, re.I) else "USD"
    growth = re.search(r"(?:based on|with)\s+(\d+(?:\.\d+)?)%", text, re.I)
    trigger = (
        f"default {growth.group(1)}% annual growth input shown by source"
        if growth else "default growth input shown by source"
    )
    claims: list[dict[str, Any]] = []
    for year, raw_value in re.findall(r"\b(20\d{2})\s+\$\s*([0-9]+(?:\.[0-9]+)?)", text):
        value = _number(raw_value)
        if value is None or value <= 0:
            continue
        claims.append(_claim(
            source_id=source_id, label=year, semantics="scenario_calculator",
            value=value if currency == "USD" else None,
            native_value=value, target_currency=currency,
            issue_at=issue_at, scenario_class="user_input_growth_calculator",
            methodology_version=f"{adapter_id}:rendered_text",
            conditional_trigger=trigger, gradeability="scenario",
        ))
    if not claims:
        top = re.search(
            r"(?:TAG Target Price\s+|Predicted Price for [^\n]+ in 5 years\s+)\$\s*([0-9]+(?:\.[0-9]+)?)",
            text, re.I,
        )
        value = _number(top.group(1)) if top else None
        if value is not None and value > 0:
            claims.append(_claim(
                source_id=source_id, label="5 years", semantics="scenario_calculator",
                value=value if currency == "USD" else None,
                native_value=value, target_currency=currency,
                issue_at=issue_at, scenario_class="user_input_growth_calculator",
                methodology_version=f"{adapter_id}:rendered_text",
                conditional_trigger=trigger, gradeability="scenario",
            ))
    return deduplicate_claims(claims)


def _document_issue_at(document: ForecastDocument) -> datetime:
    text = document.visible_text
    match = re.search(
        r"Page last updated:\s*(20\d{2}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s*\(UTC([+-]\d{1,2})\)",
        text, re.I,
    )
    if match:
        offset = timezone(timedelta(hours=int(match.group(3))))
        parsed = datetime.strptime(
            f"{match.group(1)} {match.group(2)}", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=offset)
        return parsed.astimezone(timezone.utc)
    dated_article = re.search(
        r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+(\d{1,2}\s+[A-Za-z]+\s+20\d{2})\b",
        text, re.I,
    )
    if dated_article:
        try:
            return datetime.strptime(dated_article.group(1), "%d %b %Y").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            pass
    # A source without a declared issue/update time is frozen at the exact
    # first-seen retrieval time.  The semantic fingerprint deliberately omits
    # issue/update metadata, so a later identical fetch cannot create a false
    # forecast revision.
    return document.fetched_at.astimezone(timezone.utc)


@dataclass(frozen=True)
class SourceAdapter:
    adapter_id: str
    domains: tuple[str, ...]
    source_class: str
    default_cadence_seconds: int
    scenario_calculator: bool = False
    parser_kind: str = "generic"

    def parse(
        self, *, source_id: str, document: ForecastDocument,
        source_issue_at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if self.scenario_calculator and "prediction" not in urlsplit(document.url).path.lower():
            return []
        identity_text = document.visible_text.lower()
        exact_contract_visible = TAG_CONTRACT in identity_text
        tagger_name_visible = bool(re.search(r"\btagger\b", identity_text))
        if not (exact_contract_visible or tagger_name_visible):
            return []
        effective_issue_at = source_issue_at or _document_issue_at(document)
        if self.parser_kind == "walletinvestor":
            return _walletinvestor_claims(
                source_id, document, issue_at=effective_issue_at,
                adapter_id=self.adapter_id,
            )
        if self.parser_kind == "cmc_ai":
            return _cmc_ai_claims(source_id, document, adapter_id=self.adapter_id)
        if self.parser_kind == "gate_native":
            return _gate_native_currency_claims(
                source_id, document, issue_at=effective_issue_at,
                adapter_id=self.adapter_id,
            )
        if self.parser_kind == "annual_rendered":
            return _annual_rendered_min_avg_max_claims(
                source_id, document, issue_at=effective_issue_at,
                adapter_id=self.adapter_id,
            )
        if self.parser_kind == "tradersunion":
            structured = _structured_claims(
                source_id, document.structured_data, issue_at=effective_issue_at,
                adapter_id=self.adapter_id,
            )
            if structured:
                return structured
            tabular = _table_claims(
                source_id, document.table_rows, issue_at=effective_issue_at,
                adapter_id=self.adapter_id,
            )
            if tabular:
                return tabular
            return _tradersunion_rendered_claims(
                source_id, document, issue_at=effective_issue_at,
                adapter_id=self.adapter_id,
            )
        claims = _structured_claims(
            source_id, document.structured_data, issue_at=effective_issue_at,
            adapter_id=self.adapter_id,
        )
        if not claims:
            claims = _table_claims(
                source_id, document.table_rows, issue_at=effective_issue_at,
                adapter_id=self.adapter_id,
            )
        if self.scenario_calculator and not claims:
            claims.extend(_paired_header_table_claims(
                source_id, document.table_rows, issue_at=effective_issue_at,
                adapter_id=self.adapter_id,
            ))
        if self.scenario_calculator and not claims:
            claims.extend(_scenario_calculator_text_claims(
                source_id, document, issue_at=effective_issue_at,
                adapter_id=self.adapter_id,
            ))
        if not claims and not self.scenario_calculator:
            claims = _conservative_labeled_claims(
                source_id, document.visible_text, issue_at=effective_issue_at,
                adapter_id=self.adapter_id,
            )
        if self.scenario_calculator:
            for claim in claims:
                claim["targetSemantics"] = "scenario_calculator"
                claim["gradeability"] = "scenario"
                claim["scenarioClass"] = "user_input_growth_calculator"
        return deduplicate_claims(claims)


ADAPTERS: tuple[SourceAdapter, ...] = (
    SourceAdapter("coincodex_tagger_v2", ("coincodex.com",), "algorithmic_forecast", 86_400),
    SourceAdapter("pricepredictions_ai_tagger_v2", ("pricepredictions.ai",), "machine_learning_forecast", 2_592_000),
    SourceAdapter("pricepredictions_com_tagger_v2", ("pricepredictions.com",), "algorithmic_forecast", 2_592_000),
    SourceAdapter("beincrypto_tagger_rendered_v3", ("beincrypto.com",), "technical_analysis_article", 2_592_000, parser_kind="annual_rendered"),
    SourceAdapter("tradersunion_tagger_rendered_v3", ("tradersunion.com",), "algorithmic_forecast", 1_209_600, parser_kind="tradersunion"),
    SourceAdapter("coinarbitragebot_tagger_v2", ("coinarbitragebot.com",), "algorithmic_forecast", 2_592_000),
    SourceAdapter("midforex_tagger_v2", ("midforex.com",), "machine_learning_forecast", 2_592_000),
    SourceAdapter("bitscreener_tagger_v2", ("bitscreener.com",), "algorithmic_forecast", 1_209_600),
    SourceAdapter("digitalcoinprice_tagger_v2", ("digitalcoinprice.com",), "algorithmic_forecast", 2_592_000),
    SourceAdapter("walletinvestor_tagger_v2", ("walletinvestor.com",), "algorithmic_forecast", 1_209_600, parser_kind="walletinvestor"),
    SourceAdapter("blockspot_tagger_v2", ("blockspot.io",), "algorithmic_forecast", 2_592_000),
    SourceAdapter("cryptoticker_tagger_v1", ("cryptoticker.io",), "technical_analysis_article", 1_209_600),
    SourceAdapter("dmcnews_tagger_v1", ("dmcnews.org",), "algorithmic_forecast", 2_592_000),
    SourceAdapter("coindataflow_tagger_v1", ("coindataflow.com",), "algorithmic_forecast", 2_592_000),
    SourceAdapter("coincheckup_tagger_v1", ("coincheckup.com",), "algorithmic_forecast", 2_592_000),
    SourceAdapter("govcapital_tagger_v1", ("gov.capital",), "algorithmic_forecast", 2_592_000),
    SourceAdapter("hexn_tagger_v1", ("hexn.io",), "algorithmic_forecast", 2_592_000),
    SourceAdapter("coinmarketcap_ai_tagger_v2", ("coinmarketcap.com",), "ai_summary", 1_209_600, parser_kind="cmc_ai"),
    SourceAdapter("gate_tagger_native_currency_v3", ("miniapp.gate.com",), "algorithmic_forecast", 1_209_600, parser_kind="gate_native"),
    SourceAdapter("exchange_scenario_calculator_v3", ("mexc.com", "coinbase.com", "bitget.com", "tapbit.com", "gate.com"), "scenario_calculator", 2_592_000, True, "exchange_scenario"),
)


def adapter_for_url(url: str) -> SourceAdapter | None:
    host = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    for adapter in ADAPTERS:
        if any(host == domain or host.endswith("." + domain) for domain in adapter.domains):
            return adapter
    return None


def deduplicate_claims(claims: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in claims:
        claim = dict(raw)
        key = json.dumps({
            "horizon": claim.get("normalizedHorizon"),
            "original": claim.get("originalHorizonLabel"),
            "semantics": claim.get("targetSemantics"),
            "target": claim.get("targetPrice"),
            "low": claim.get("targetLow"), "high": claim.get("targetHigh"),
            "currency": claim.get("targetCurrency"),
            "nativeTarget": claim.get("targetNativePrice"),
            "nativeLow": claim.get("targetNativeLow"), "nativeHigh": claim.get("targetNativeHigh"),
            "direction": claim.get("direction"), "probability": claim.get("probability"),
        }, sort_keys=True, default=str)
        if key not in seen:
            seen.add(key)
            result.append(claim)
    return result


def semantics_period_deadline(claim: Mapping[str, Any]) -> datetime | None:
    semantics = str(claim.get("targetSemantics") or "")
    period_start = _utc(claim.get("periodStart"))
    period_end = _utc(claim.get("periodEnd"))
    deadline = _utc(claim.get("deadline"))
    if semantics == "mid_year" and period_start:
        year = period_start.year
        return datetime(year, 6, 30, 23, 59, 59, tzinfo=timezone.utc)
    if semantics in {"period_average", "period_minimum", "period_maximum", "range_for_period"}:
        return period_end
    return deadline or period_end
