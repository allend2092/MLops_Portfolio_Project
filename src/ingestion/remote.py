# src/ingestion/remote.py
import logging
from typing import Optional

import paramiko

logger = logging.getLogger(__name__)


def run_remote_command(
    command: str,
    host: str,
    user: str,
    ssh_key_path: Optional[str] = None,
) -> str:
    """
    Execute a command on a remote host via SSH and return stdout.

    Args:
        command: Command to execute on the remote host.
        host: Remote hostname or IP.
        user: SSH username.
        ssh_key_path: Path to private key (optional; if None, Paramiko will
                      fall back to its default key loading behavior).

    Returns:
        Raw stdout from the command as a string.

    Raises:
        RuntimeError if the command exits with non-zero status.
        Any Paramiko-related exceptions if SSH connection fails.
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        logger.info(f"Connecting to {user}@{host} via SSH")
        client.connect(
            hostname=host,
            username=user,
            key_filename=ssh_key_path,
        )

        logger.debug(f"Running remote command: {command}")
        stdin, stdout, stderr = client.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()

        stdout_data = stdout.read().decode()
        stderr_data = stderr.read().decode()

        if exit_status != 0:
            logger.error(
                f"Remote command failed on {host} with exit code {exit_status}: {stderr_data}"
            )
            raise RuntimeError(
                f"Command failed on {host} (exit {exit_status}): {stderr_data}"
            )

        return stdout_data

    except Exception as e:
        logger.error(f"SSH execution failed for {user}@{host}: {e}")
        raise

    finally:
        client.close()
