# src/ingestion/docker_collector.py
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

from .remote import run_remote_command

logger = logging.getLogger(__name__)


def _list_containers(
    host: str,
    user: str,
    ssh_key_path: Optional[str],
) -> List[Tuple[str, str]]:
    """
    Return a list of (container_id, container_name) tuples from the remote host.
    """
    cmd = "docker ps --format '{{.ID}} {{.Names}}'"
    logger.info(f"Listing Docker containers on {user}@{host}: {cmd}")

    raw = run_remote_command(
        command=cmd,
        host=host,
        user=user,
        ssh_key_path=ssh_key_path,
    )

    containers: List[Tuple[str, str]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=1)
        if len(parts) == 2:
            cid, name = parts
        else:
            # Some images might not have a name; use ID as name as fallback
            cid = parts[0]
            name = parts[0]
        containers.append((cid, name))

    logger.info(f"Found {len(containers)} running containers on {host}")
    return containers


def collect_docker_logs(
    host: str,
    user: str,
    ssh_key_path: Optional[str],
    since_minutes: int = 60,
    containers: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Collect Docker logs from one or more containers on the remote host.

    Args:
        host: Remote host IP/hostname.
        user: SSH username.
        ssh_key_path: Path to SSH private key (or None to use defaults).
        since_minutes: How far back in time to collect logs.
        containers: Optional explicit container name/ID list.
                    If None, all running containers are used.

    Returns:
        List[dict] where each dict represents one log line with metadata.
    """
    # If no explicit containers provided, discover all running containers
    discovered: List[Tuple[str, str]] = _list_containers(host, user, ssh_key_path)
    if containers is None:
        target_containers = discovered
    else:
        # Filter discovered list to only containers the user requested
        discovered_map = {cid: name for cid, name in discovered}
        name_map = {name: cid for cid, name in discovered}
        target_containers: List[Tuple[str, str]] = []
        for c in containers:
            if c in discovered_map:
                target_containers.append((c, discovered_map[c]))
            elif c in name_map:
                target_containers.append((name_map[c], c))
            else:
                logger.warning(f"Requested container {c} not found on {host}")

    if not target_containers:
        logger.warning("No containers to collect logs from.")
        return []

    # docker logs --since expects an RFC3339/ISO8601 timestamp
    since_dt = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
    since_ts = since_dt.isoformat()

    all_logs: List[Dict] = []

    for cid, name in target_containers:
        cmd = f"docker logs --since {since_ts} --timestamps {cid}"
        logger.info(
            f"Collecting Docker logs from container {name} ({cid}) on {host}: {cmd}"
        )

        try:
            raw = run_remote_command(
                command=cmd,
                host=host,
                user=user,
                ssh_key_path=ssh_key_path,
            )
        except Exception as e:
            logger.error(
                f"Failed to collect logs for container {name} ({cid}) on {host}: {e}"
            )
            continue

        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue

            # docker logs --timestamps format is:
            # 2025-12-05T22:35:22.123456789Z message ...
            # So we split on first whitespace
            try:
                ts_str, msg = line.split(" ", 1)
            except ValueError:
                ts_str, msg = None, line

            rec: Dict = {
                "source": "docker",
		"host": host,
		"container_id": cid,
		"container_name": name,
		"timestamp": ts_str,
		"message": msg,
            }
            all_logs.append(rec)

    logger.info(
        f"Collected {len(all_logs)} Docker log lines from {len(target_containers)} container(s) on {host}"
    )
    return all_logs


def save_docker_logs(
    logs: List[Dict],
    output_dir: str = "data/ingested/docker",
) -> str:
    """
    Save Docker logs to a JSONL file with a timestamped filename.

    Returns:
        Path to the written file.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"docker_logs_{timestamp}.jsonl")

    with open(output_path, "w", encoding="utf-8") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")

    logger.info(f"Saved {len(logs)} Docker log lines to {output_path}")
    return output_path
