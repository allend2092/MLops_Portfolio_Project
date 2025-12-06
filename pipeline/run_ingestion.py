# pipeline/run_ingestion.py
import logging
import os

from src.ingestion.systemd_collector import collect_systemd_logs, save_logs

logger = logging.getLogger(__name__)


CONFIG = {
    "ai_host": "172.16.0.20",
    "ai_user": "daryl",
    "ssh_key": "/home/daryl/.ssh/id_ed25519",  # <--- key path
    "output_base": "data/ingested",
}



def configure_logging() -> None:
    """Configure root logging once for the whole pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )


def main() -> None:
    """Main ingestion pipeline runner."""
    configure_logging()
    logger.info("Starting ingestion pipeline")

    os.makedirs(CONFIG["output_base"], exist_ok=True)

    try:
        logs = collect_systemd_logs(
            host=CONFIG["ai_host"],
            user=CONFIG["ai_user"],
            ssh_key_path=CONFIG["ssh_key"],
            unit="docker.service",
            since_hours=24,
        )
        if logs:
            save_logs(logs)
        else:
            logger.warning("No logs collected; nothing to save.")

        # TODO: later:
        # - collect_docker_logs(...)
        # - collect_gpu_metrics(...)

        logger.info("Ingestion pipeline completed successfully")

    except Exception as e:
        logger.error(f"Ingestion pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
