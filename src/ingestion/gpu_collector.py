# src/ingestion/gpu_collector.py
import json
import logging
from typing import List, Dict

from .remote import run_remote_command

logger = logging.getLogger(__name__)


def collect_gpu_metrics(
    host: str,
    user: str,
    ssh_key_path: str | None,
) -> List[Dict]:
    cmd = (
        "nvidia-smi --query-gpu=index,name,temperature.gpu,utilization.gpu,"
        "memory.total,memory.used,power.draw --format=json"
    )
    raw = run_remote_command(
        command=cmd,
        host=host,
        user=user,
        ssh_key_path=ssh_key_path,
    )
    data = json.loads(raw)
    # nvidia-smi returns {"gpu": [ ... ]}
    return data.get("gpu", [])
