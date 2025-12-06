# src/ingestion/gpu_collector.py
import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Dict, Optional

from .remote import run_remote_command

logger = logging.getLogger(__name__)


def collect_gpu_metrics(
    host: str,
    user: str,
    ssh_key_path: Optional[str],
) -> List[Dict]:
    """
    Collect GPU metrics from the remote host using nvidia-smi.

    Uses:
      nvidia-smi --query-gpu=index,name,temperature.gpu,utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits

    Returns:
      List[dict] where each dict contains per-GPU metrics.
    """
    cmd = (
        "nvidia-smi "
        "--query-gpu=index,name,temperature.gpu,utilization.gpu,memory.used,memory.total "
        "--format=csv,noheader,nounits"
    )

    logger.info(f"Collecting GPU metrics from {user}@{host}: {cmd}")

    try:
        raw = run_remote_command(
            command=cmd,
            host=host,
            user=user,
            ssh_key_path=ssh_key_path,
        )
    except Exception as e:
        logger.error(f"Failed to collect GPU metrics from {host}: {e}")
        return []

    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if not lines:
        logger.warning(f"No GPU metrics returned from {host}")
        return []

    collected_at = datetime.now(timezone.utc).isoformat()
    metrics: List[Dict] = []

    for line in lines:
        # Example line (no units):
        # 0, NVIDIA GeForce RTX 3090, 35, 3, 1234, 24576
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 6:
            logger.warning(f"Unexpected nvidia-smi line format: {line}")
            continue

        index_str, name, temp_str, util_str, mem_used_str, mem_total_str = parts

        try:
            gpu_record: Dict = {
                "host": host,
                "collected_at": collected_at,
                "gpu_index": int(index_str),
                "gpu_name": name,
                "temperature_gpu_c": float(temp_str),
                "utilization_gpu_pct": float(util_str),
                "memory_used_mb": float(mem_used_str),
                "memory_total_mb": float(mem_total_str),
            }
            metrics.append(gpu_record)
        except ValueError as ve:
            logger.warning(f"Failed to parse GPU metrics line '{line}': {ve}")
            continue

    logger.info(f"Collected GPU metrics for {len(metrics)} GPU(s) from {host}")
    return metrics


def save_gpu_metrics(
    metrics: List[Dict],
    output_dir: str = "data/ingested/gpu",
) -> str:
    """
    Save GPU metrics to a JSONL file with a timestamped filename.

    Returns:
      Path to the written file.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"gpu_metrics_{timestamp}.jsonl")

    with open(output_path, "w", encoding="utf-8") as f:
        for rec in metrics:
            f.write(json.dumps(rec) + "\n")

    logger.info(f"Saved GPU metrics for {len(metrics)} GPU(s) to {output_path}")
    return output_path
