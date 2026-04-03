import csv
import hashlib
import io
import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


DEFAULT_DATA_DIR_NAME = "datasets"


def _read_csv_from_path(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_csv_from_text(text: str) -> List[Dict[str, str]]:
    handle = io.StringIO(text)
    return list(csv.DictReader(handle))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _read_rows_from_url(url: str) -> List[Dict[str, str]]:
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=30) as response:
        content_type = (response.headers.get("Content-Type") or "").lower()
        body = response.read().decode("utf-8")

    if "application/json" in content_type or body.strip().startswith("[") or body.strip().startswith("{"):
        parsed = json.loads(body)
        if isinstance(parsed, dict) and "rows" in parsed and isinstance(parsed["rows"], list):
            return parsed["rows"]
        if isinstance(parsed, list):
            return parsed
        raise ValueError(f"Unsupported JSON payload from {url}. Expected array or {{\"rows\": [...]}}")

    return _read_csv_from_text(body)


def load_source_rows_with_meta(source_name: str, default_csv_path: Path) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    env_key = source_name.upper()
    loaded_at = _utc_now_iso()

    path_override = os.getenv(f"SOURCE_{env_key}_PATH", "").strip()
    if path_override:
        source_path = Path(path_override).expanduser().resolve()
        rows = _read_csv_from_path(source_path)
        return rows, {
            "source_name": source_name,
            "source_type": "path_override",
            "source_ref": str(source_path),
            "loaded_at_utc": loaded_at,
            "source_version": _mtime_iso(source_path),
            "row_count": len(rows),
        }

    url_override = os.getenv(f"SOURCE_{env_key}_URL", "").strip()
    if url_override:
        request = urllib.request.Request(url_override, method="GET")
        with urllib.request.urlopen(request, timeout=30) as response:
            content_type = (response.headers.get("Content-Type") or "").lower()
            etag = response.headers.get("ETag")
            last_modified = response.headers.get("Last-Modified")
            body = response.read().decode("utf-8")

        if "application/json" in content_type or body.strip().startswith("[") or body.strip().startswith("{"):
            parsed = json.loads(body)
            if isinstance(parsed, dict) and "rows" in parsed and isinstance(parsed["rows"], list):
                rows = parsed["rows"]
            elif isinstance(parsed, list):
                rows = parsed
            else:
                raise ValueError(f"Unsupported JSON payload from {url_override}. Expected array or {{\"rows\": [...]}}")
        else:
            rows = _read_csv_from_text(body)

        return rows, {
            "source_name": source_name,
            "source_type": "url_override",
            "source_ref": url_override,
            "loaded_at_utc": loaded_at,
            "source_version": etag or last_modified or _sha256_text(body),
            "row_count": len(rows),
            "etag": etag,
            "last_modified": last_modified,
        }

    rows = _read_csv_from_path(default_csv_path)
    return rows, {
        "source_name": source_name,
        "source_type": "default_path",
        "source_ref": str(default_csv_path),
        "loaded_at_utc": loaded_at,
        "source_version": _mtime_iso(default_csv_path),
        "row_count": len(rows),
    }


def get_dataset_dir(root_dir: Path) -> Path:
    override = os.getenv("CONTROL_TOWER_DATA_DIR", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return (root_dir / DEFAULT_DATA_DIR_NAME).resolve()


def load_source_rows(source_name: str, default_csv_path: Path) -> List[Dict[str, str]]:
    rows, _ = load_source_rows_with_meta(source_name, default_csv_path)
    return rows
