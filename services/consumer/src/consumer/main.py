"""
Entrypoint for ColdChainIQ Ingestion Consumer.
Sets up structured JSON logging, OS signal handlers, lightweight HTTP /health server,
and launches the streaming pipeline.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import os
import signal
import sys
import threading
from typing import Any
from pythonjsonlogger import jsonlogger

from .config import ConsumerSettings
from .pipeline import IngestionPipeline

logger = logging.getLogger("coldchainiq.consumer")


def setup_logging(log_level: str = "INFO") -> None:
    """Configures structured JSON logging format."""
    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))


class HealthHTTPHandler(BaseHTTPRequestHandler):
    """
    Lightweight HTTP health endpoint handler for Kubernetes/Docker container probes.
    """
    pipeline: Any = None

    def do_GET(self) -> None:
        if self.path in ("/health", "/healthz", "/ready"):
            pg_ok = False
            redis_ok = False
            kafka_ok = False
            metrics_summary = {}

            if self.pipeline:
                pg_ok = self.pipeline.repository.ping()
                redis_ok = self.pipeline.redis_store.ping()
                kafka_ok = self.pipeline.running
                metrics_summary = self.pipeline.metrics.get_summary()

            is_ready = pg_ok and redis_ok and kafka_ok
            status_code = 200 if is_ready else 503

            resp_payload = {
                "status": "UP" if is_ready else "DOWN",
                "checks": {
                    "postgres": pg_ok,
                    "redis": redis_ok,
                    "kafka_consumer": kafka_ok,
                },
                "metrics": metrics_summary,
            }

            resp_bytes = json.dumps(resp_payload).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(resp_bytes)))
            self.end_headers()
            self.wfile.write(resp_bytes)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress standard HTTP access logs to keep stdout clean
        pass


def start_health_server(port: int, pipeline: IngestionPipeline) -> HTTPServer:
    """Starts background daemon HTTP server on configured health port."""
    HealthHTTPHandler.pipeline = pipeline
    server = HTTPServer(("0.0.0.0", port), HealthHTTPHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    logger.info(f"Health server listening on http://0.0.0.0:{port}/health")
    return server


def main() -> None:
    settings = ConsumerSettings()
    setup_logging(settings.LOG_LEVEL)

    logger.info(
        f"Starting ColdChainIQ Consumer: broker={settings.KAFKA_BOOTSTRAP}, "
        f"topic={settings.KAFKA_TOPIC}, group={settings.KAFKA_GROUP_ID}, "
        f"batch_size={settings.BATCH_SIZE}"
    )

    pipeline = IngestionPipeline(settings)

    # Signal handlers for clean shutdown
    def _sig_handler(signum: int, frame: Any) -> None:
        sig_name = signal.Signals(signum).name
        logger.info(f"Received signal {sig_name}. Requesting pipeline stop...")
        pipeline.stop()

    signal.signal(signal.SIGINT, _sig_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _sig_handler)

    health_server = start_health_server(settings.HEALTH_PORT, pipeline)

    try:
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught in main.")
    finally:
        health_server.shutdown()
        logger.info("ColdChainIQ Consumer process exiting cleanly.")


if __name__ == "__main__":
    main()
