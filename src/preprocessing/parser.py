# src/preprocessing/parser.py

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


# ---------- Helpers ----------

def _read_jsonl(path: Path) -> Iterable[Dict]:
    """Yield records from a JSONL file."""
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logger.warning(f"Skipping invalid JSON line in {path}")
                continue


def _to_iso_utc_from_micros(micros_str: str) -> Optional[str]:
    """Convert journald microsecond timestamp string to ISO-8601 UTC."""
    try:
        micros = int(micros_str)
        dt = datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def _to_iso_utc_from_iso_string(ts: str) -> Optional[str]:
    """
    Normalize an ISO-ish string (with 'Z', nanoseconds, etc.) to ISO UTC.
    Examples:
      '2025-12-06T17:08:56.400673015Z'
      '2025-12-06T17:08:56Z'
    """
    if not ts:
        return None

    try:
        # Handle trailing Z
        if ts.endswith("Z"):
            ts = ts.replace("Z", "+00:00")

        # Python's fromisoformat can't handle >6 microseconds; trim if needed
        if "." in ts:
            date_part, frac_part = ts.split(".", 1)
            # frac_part might contain timezone offset, e.g. '400673015+00:00'
            if "+" in frac_part or "-" in frac_part:
                frac, offset = frac_part.replace("+", " +").replace("-", " -").split(" ", 1)
                frac = frac[:6]  # microseconds
                ts = f"{date_part}.{frac}{offset}"
            else:
                frac = frac_part[:6]
                ts = f"{date_part}.{frac}"

        dt = datetime.fromisoformat(ts)
        # Ensure timezone aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


# ---------- Normalizers for each source ----------

def normalize_systemd_record(rec: Dict) -> Optional[Dict]:
    """
    Convert a raw systemd record into a normalized event.
    """
    # journald's microsecond timestamp
    raw_ts = rec.get("__REALTIME_TIMESTAMP")
    ts = _to_iso_utc_from_micros(raw_ts) if raw_ts else None
    if not ts:
        logger.debug("Skipping systemd record without valid timestamp")
        return None

    host = rec.get("host") or rec.get("_HOSTNAME")
    unit = rec.get("unit") or rec.get("UNIT") or rec.get("_SYSTEMD_UNIT")
    message = rec.get("MESSAGE")
    priority = rec.get("PRIORITY")

    return {
        "timestamp": ts,
        "source": rec.get("source", "systemd"),
        "host": host,
        "category": "log",
        "subtype": "systemd",
        "severity": priority,
        "unit": unit,
        "message": message,
        # You can add more fields here later if needed
    }


def normalize_docker_record(rec: Dict) -> Optional[Dict]:
    """
    Convert a raw docker record into a normalized event.
    Expected fields from your collector:
      - timestamp
      - host
      - container_name
      - container_id
      - message
      - source = 'docker'
    """
    raw_ts = rec.get("timestamp")
    ts = _to_iso_utc_from_iso_string(raw_ts) if raw_ts else None
    if not ts:
        logger.debug("Skipping docker record without valid timestamp")
        return None

    return {
        "timestamp": ts,
        "source": rec.get("source", "docker"),
        "host": rec.get("host"),
        "category": "log",
        "subtype": "docker",
        "container_name": rec.get("container_name"),
        "container_id": rec.get("container_id"),
        "message": rec.get("message"),
    }


def normalize_gpu_record(rec: Dict) -> Optional[Dict]:
    """
    Convert a GPU metrics record into a normalized event.
    Expected fields from your collector:
      - collected_at
      - host
      - gpu_index
      - gpu_name
      - temperature_gpu_c
      - utilization_gpu_pct
      - memory_used_mb
      - memory_total_mb
      - source = 'gpu'
    """
    raw_ts = rec.get("collected_at")
    ts = _to_iso_utc_from_iso_string(raw_ts) if raw_ts else None
    if not ts:
        logger.debug("Skipping gpu record without valid timestamp")
        return None

    return {
        "timestamp": ts,
        "source": rec.get("source", "gpu"),
        "host": rec.get("host"),
        "category": "metric",
        "subtype": "gpu",
        "gpu_index": rec.get("gpu_index"),
        "gpu_name": rec.get("gpu_name"),
        "temperature_gpu_c": rec.get("temperature_gpu_c"),
        "utilization_gpu_pct": rec.get("utilization_gpu_pct"),
        "memory_used_mb": rec.get("memory_used_mb"),
        "memory_total_mb": rec.get("memory_total_mb"),
    }


# ---------- Top-level processing functions ----------

@dataclass
class PreprocessConfig:
    ingested_root: Path = Path("data/ingested")
    processed_root: Path = Path("data/processed")
    output_filename: str = "combined_events.jsonl"


def process_all(config: Optional[PreprocessConfig] = None) -> Path:
    """
    Read raw ingested JSONL logs (systemd, docker, gpu),
    normalize them to a unified schema, and write a combined JSONL file.

    Returns:
        Path to the combined JSONL file.
    """
    if config is None:
        config = PreprocessConfig()

    systemd_dir = config.ingested_root / "systemd"
    docker_dir = config.ingested_root / "docker"
    gpu_dir = config.ingested_root / "gpu"

    output_dir = config.processed_root
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / config.output_filename

    logger.info(f"Starting preprocessing. Output -> {output_path}")

    total_written = 0

    with output_path.open("w", encoding="utf-8") as out_f:

        # ---- Systemd logs ----
        if systemd_dir.exists():
            for path in sorted(systemd_dir.glob("*.jsonl")):
                logger.info(f"Processing systemd file: {path}")
                for rec in _read_jsonl(path):
                    norm = normalize_systemd_record(rec)
                    if norm is None:
                        continue
                    out_f.write(json.dumps(norm) + "\n")
                    total_written += 1

        # ---- Docker logs ----
        if docker_dir.exists():
            for path in sorted(docker_dir.glob("*.jsonl")):
                logger.info(f"Processing docker file: {path}")
                for rec in _read_jsonl(path):
                    norm = normalize_docker_record(rec)
                    if norm is None:
                        continue
                    out_f.write(json.dumps(norm) + "\n")
                    total_written += 1

        # ---- GPU metrics ----
        if gpu_dir.exists():
            for path in sorted(gpu_dir.glob("*.jsonl")):
                logger.info(f"Processing gpu file: {path}")
                for rec in _read_jsonl(path):
                    norm = normalize_gpu_record(rec)
                    if norm is None:
                        continue
                    out_f.write(json.dumps(norm) + "\n")
                    total_written += 1

    logger.info(f"Preprocessing completed. Wrote {total_written} events to {output_path}")
    return output_path


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )


if __name__ == "__main__":
    configure_logging()
    process_all()
