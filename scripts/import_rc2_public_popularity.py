"""Build reproducible source popularity from public rank datasets only."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.terminal_database import (
    TagNextExternalSourceRow,
    TagNextSourcePopularityComponentRow,
    json_dumps,
    session_scope,
)


VERSION = "tagnext-public-popularity-v1"
EVIDENCE_DIR = Path(__file__).resolve().parents[2] / "rc2_evidence"
TRANCO = EVIDENCE_DIR / "tranco-top-1m.csv.zip"
MAJESTIC = EVIDENCE_DIR / "majestic_million.csv"


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash(value: object) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode()).hexdigest()


def _id(prefix: str, value: object) -> str:
    return f"{prefix}_{_hash(value)[:24]}"


def _domain_candidates(host: str) -> set[str]:
    host = host.lower().strip(".").removeprefix("www.")
    parts = host.split(".")
    candidates = {host}
    if len(parts) > 2:
        candidates.add(".".join(parts[-2:]))
    return candidates


def _score(rank: int | None, maximum: int = 1_000_000) -> float:
    if rank is None or rank <= 0 or rank > maximum:
        return 0.0
    return max(0.0, min(1.0, 1.0 - math.log(rank) / math.log(maximum)))


def _tranco_rows() -> Iterable[tuple[int, str]]:
    with zipfile.ZipFile(TRANCO) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(names) != 1:
            raise RuntimeError("Tranco archive must contain exactly one CSV")
        with archive.open(names[0]) as raw:
            for row in csv.reader(io.TextIOWrapper(raw, encoding="utf-8", newline="")):
                if len(row) >= 2 and row[0].isdigit():
                    yield int(row[0]), row[1].strip().lower()


def _majestic_rows() -> Iterable[tuple[int, str]]:
    with MAJESTIC.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            raw_rank = str(row.get("GlobalRank") or "")
            domain = str(row.get("Domain") or "").strip().lower()
            if raw_rank.isdigit() and domain:
                yield int(raw_rank), domain


def _find_ranks(
    rows: Iterable[tuple[int, str]], candidate_to_sources: dict[str, set[str]],
) -> dict[str, int]:
    ranks: dict[str, int] = {}
    for rank, domain in rows:
        for source_id in candidate_to_sources.get(domain, set()):
            ranks[source_id] = min(rank, ranks.get(source_id, rank))
    return ranks


def _set_popularity_summary(
    session: Session, source_id: str, payload: dict[str, object],
) -> None:
    """Persist a summary on an object attached to the active write session."""

    source = session.get(TagNextExternalSourceRow, source_id)
    if source is None:
        raise RuntimeError(f"External source disappeared during popularity import: {source_id}")
    source.popularity_json = json_dumps(payload)


def main() -> int:
    for path in (TRANCO, MAJESTIC):
        if not path.is_file():
            raise FileNotFoundError(path)
    with session_scope() as session:
        sources = list(session.scalars(select(TagNextExternalSourceRow)))
    candidate_to_sources: dict[str, set[str]] = {}
    source_domains: dict[str, str] = {}
    for source in sources:
        host = (urlsplit(source.canonical_url or "").hostname or "").lower()
        source_domains[source.source_id] = host
        for candidate in _domain_candidates(host):
            candidate_to_sources.setdefault(candidate, set()).add(source.source_id)

    tranco_hash = _sha(TRANCO)
    majestic_hash = _sha(MAJESTIC)
    tranco_ranks = _find_ranks(_tranco_rows(), candidate_to_sources)
    majestic_ranks = _find_ranks(_majestic_rows(), candidate_to_sources)
    observed_at = datetime.fromtimestamp(
        max(TRANCO.stat().st_mtime, MAJESTIC.stat().st_mtime), tz=timezone.utc
    )
    written = 0
    summaries: dict[str, dict[str, object]] = {}
    with session_scope() as session:
        for source in sources:
            components = []
            for component_name, rank, dataset_url, dataset_hash in (
                ("tranco_top_1m", tranco_ranks.get(source.source_id), "https://tranco-list.eu/", tranco_hash),
                ("majestic_million", majestic_ranks.get(source.source_id), "https://majestic.com/reports/majestic-million", majestic_hash),
            ):
                normalized = _score(rank)
                payload = {
                    "sourceId": source.source_id,
                    "component": component_name,
                    "domain": source_domains[source.source_id],
                    "rank": rank,
                    "normalizedScore": round(normalized, 12),
                    "observedAt": observed_at.isoformat(),
                    "datasetSha256": dataset_hash,
                    "formula": "max(0,1-ln(rank)/ln(1000000)); absent=0",
                }
                payload_hash = _hash(payload)
                exists = session.scalar(select(TagNextSourcePopularityComponentRow).where(
                    TagNextSourcePopularityComponentRow.payload_hash == payload_hash
                ))
                if exists is None:
                    session.add(TagNextSourcePopularityComponentRow(
                        component_id=_id("tnspc", payload), source_id=source.source_id,
                        component_name=component_name, source_url=dataset_url,
                        observed_at=observed_at, raw_rank=rank,
                        normalized_score=normalized,
                        confidence="public_top_1m_rank" if rank is not None else "not_ranked_in_public_top_1m",
                        evidence_package_id=None,
                        method_json=json_dumps({
                            "version": VERSION, "formula": payload["formula"],
                            "datasetSha256": dataset_hash,
                            "searchHitCountsUsed": False,
                        }),
                        payload_hash=payload_hash,
                    ))
                    written += 1
                components.append({"name": component_name, "rank": rank, "score": normalized})
            composite = sum(float(row["score"]) for row in components) / len(components)
            popularity_summary = {
                "version": VERSION,
                "compositeScore0To100": round(composite * 100.0, 6),
                "score": round(composite * 100.0, 6),
                "metric": "public_rank_composite_v1",
                "formula": "equal-weight mean of two log-rank normalized public top-1M lists",
                "components": components,
                "accuracyWeight": 0,
                "consensusWeight": 0,
                "searchHitCountsUsed": False,
                "observedAt": observed_at.isoformat(),
            }
            _set_popularity_summary(session, source.source_id, popularity_summary)
            summaries[source.source_id] = {
                "domain": source_domains[source.source_id],
                "compositeScore0To100": round(composite * 100.0, 6),
                "components": components,
            }
    print(json.dumps({
        "version": VERSION, "sources": len(sources), "componentsWritten": written,
        "datasetSha256": {"tranco": tranco_hash, "majestic": majestic_hash},
        "sourceSummaries": summaries,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
