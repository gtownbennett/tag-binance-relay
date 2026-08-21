"""High-confidence release secret and APK endpoint scanner."""
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


SECRET_PATTERNS = {
    "private_key": re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "openai_key": re.compile(rb"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}"),
    "github_token": re.compile(rb"\b(?:ghp_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})"),
    "slack_token": re.compile(rb"\bxox[baprs]-[A-Za-z0-9-]{20,}"),
    "aws_access_key": re.compile(rb"\bAKIA[A-Z0-9]{16}\b"),
    "google_api_key": re.compile(rb"\bAIza[0-9A-Za-z_-]{30,}\b"),
    "jwt": re.compile(rb"\beyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b"),
}
FORBIDDEN_TRACKED_NAMES = re.compile(
    r"(^|/)(?:\.env(?:\..*)?|.*\.(?:jks|keystore|p12|pfx)|cookies?(?:\..*)?|credentials?(?:\..*)?|vault(?:\..*)?)$",
    re.I,
)
MANDATORY_RELEASE_EXCLUSIONS = {".env.example"}
URL_PATTERN = re.compile(rb"https?://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]{4,}")


def _tracked(repo: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "-c", f"safe.directory={repo}", "ls-files", "-z"],
        cwd=repo,
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8", errors="strict")
    return [repo / name for name in result.split("\0") if name]


def _clean_url(raw: bytes) -> str | None:
    value = raw.decode("ascii", errors="ignore").rstrip(".,);]}\"'")
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, action="append", required=True)
    parser.add_argument("--apk-expanded", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    findings: list[dict[str, object]] = []
    forbidden_files: list[str] = []
    mandatory_exclusions: list[str] = []
    scanned_files = 0
    for repo_value in args.repo:
        repo = repo_value.resolve()
        for path in _tracked(repo):
            relative = path.relative_to(repo).as_posix()
            if FORBIDDEN_TRACKED_NAMES.search(relative):
                qualified = f"{repo.name}/{relative}"
                if relative in MANDATORY_RELEASE_EXCLUSIONS:
                    mandatory_exclusions.append(qualified)
                else:
                    forbidden_files.append(qualified)
            if not path.is_file():
                continue
            data = path.read_bytes()
            scanned_files += 1
            for pattern_name, pattern in SECRET_PATTERNS.items():
                for match in pattern.finditer(data):
                    findings.append({
                        "scope": "tracked_source",
                        "path": f"{repo.name}/{relative}",
                        "pattern": pattern_name,
                        "byteOffset": match.start(),
                    })

    apk_root = args.apk_expanded.resolve()
    apk_files = [path for path in apk_root.rglob("*") if path.is_file()]
    endpoints: set[str] = set()
    for path in apk_files:
        data = path.read_bytes()
        scanned_files += 1
        relative = path.relative_to(apk_root).as_posix()
        for pattern_name, pattern in SECRET_PATTERNS.items():
            for match in pattern.finditer(data):
                findings.append({
                    "scope": "compiled_apk",
                    "path": relative,
                    "pattern": pattern_name,
                    "byteOffset": match.start(),
                })
        for match in URL_PATTERN.finditer(data):
            cleaned = _clean_url(match.group(0))
            if cleaned:
                endpoints.add(cleaned)

    forbidden_endpoint_markers = (
        "tag-binance-relay", "tagalysis", "com.eric.tagterminal",
    )
    forbidden_endpoints = [
        endpoint for endpoint in sorted(endpoints)
        if any(marker in endpoint.lower() for marker in forbidden_endpoint_markers)
    ]
    expected_lan = "http://192.168.1.181:8787"
    payload = {
        "scanVersion": "tagnext-release-secret-scan-v1",
        "scannedFileCount": scanned_files,
        "highConfidenceSecretFindingCount": len(findings),
        "forbiddenTrackedFileCount": len(forbidden_files),
        "mandatoryReleaseExclusionCount": len(mandatory_exclusions),
        "forbiddenEndpointCount": len(forbidden_endpoints),
        "expectedValidationLanEndpointPresent": any(
            endpoint.startswith(expected_lan) for endpoint in endpoints
        ),
        "findings": findings,
        "forbiddenTrackedFiles": sorted(forbidden_files),
        "mandatoryReleaseExclusions": sorted(mandatory_exclusions),
        "forbiddenEndpoints": forbidden_endpoints,
        "compiledApkEndpoints": sorted(endpoints),
        "matchContentsIncluded": False,
        "passed": not findings and not forbidden_files and not forbidden_endpoints,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: payload[key] for key in (
        "scannedFileCount", "highConfidenceSecretFindingCount",
        "forbiddenTrackedFileCount", "forbiddenEndpointCount",
        "expectedValidationLanEndpointPresent", "passed",
    )}, sort_keys=True))
    if not payload["passed"]:
        raise SystemExit(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
