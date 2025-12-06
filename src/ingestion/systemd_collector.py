# src/ingestion/systemd_collector.py
import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional

from .remote import run_remote_command  # note the relative import

logger = logging.getLogger(__name__)


def collect_systemd_logs(
    host: str,
    user: str,
    ssh_key_path: Optional[str],
    unit: str = "docker.service",
    since_hours: int = 24,
) -> List[Dict]:
    """
    Collect systemd logs from a remote host via SSH.

    Args:
        host: Remote host IP or hostname.
        user: SSH username on the remote host.
        ssh_key_path: Path to private key file (or None to use defaults).
        unit: systemd unit name (e.g., 'docker.service').
        since_hours: How far back in time to collect logs.

    Returns:
        A list of dicts, each representing one journal entry.
    """
    since_arg = f"--since '{since_hours} hours ago'"
    cmd = f"journalctl {since_arg} -u {unit} --output=json"
    logger.info(f"Collecting systemd logs from {user}@{host}: {cmd}")

    raw_output = run_remote_command(
        command=cmd,
        host=host,
        user=user,
        ssh_key_path=ssh_key_path,
    )

    logs: List[Dict] = []
    for line in raw_output.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)

            # Enrich each record with metadata
            rec.setdefault("host", host)
            rec.setdefault("unit", unit)
            rec["source"] = "systemd"

            logs.append(rec)
        except json.JSONDecodeError:
            logger.warning("Skipping non-JSON line from journalctl output") 

    logger.info(f"Collected {len(logs)} log entries from unit '{unit}' on {host}")
    return logs


def save_logs(logs: List[Dict], output_dir: str = "data/ingested/systemd") -> str:
    """
    Save logs to a JSONL file with timestamped filename.

    Returns:
        The path to the file that was written.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"systemd_logs_{timestamp}.jsonl")

    with open(output_path, "w", encoding="utf-8") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")

    logger.info(f"Saved {len(logs)} logs to {output_path}")
    return output_path
