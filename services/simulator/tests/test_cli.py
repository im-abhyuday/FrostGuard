"""
Unit tests for CLI configuration, environment variable parsing, and flag precedence.
"""

from unittest.mock import patch, MagicMock
from typer.testing import CliRunner
from simulator.cli import app

runner = CliRunner()


def test_cli_env_var_pickup_and_flag_precedence(monkeypatch):
    """
    Verifies that SIM_KAFKA_BOOTSTRAP environment variable is picked up
    when no explicit flag is passed, and that an explicit CLI flag overrides it.
    """
    # 1. Test environment variable pickup over default
    monkeypatch.setenv("SIM_KAFKA_BOOTSTRAP", "env-broker:9092")
    monkeypatch.setenv("SIM_TRUCKS", "3")

    with patch("simulator.cli.KafkaSink") as mock_sink_cls, patch("simulator.cli.Fleet") as mock_fleet_cls:
        mock_fleet = MagicMock()
        mock_fleet_cls.return_value = mock_fleet

        result = runner.invoke(app, ["live", "--duration", "0.1"])
        assert result.exit_code == 0, f"CLI exited with error: {result.stdout}"

        # Verify KafkaSink was initialized with env-broker:9092
        mock_sink_cls.assert_called_once()
        _, kwargs = mock_sink_cls.call_args
        assert kwargs.get("bootstrap_servers") == "env-broker:9092", (
            f"Expected env-broker:9092 from env var, got {kwargs.get('bootstrap_servers')}"
        )

    # 2. Test explicit CLI flag overrides environment variable
    with patch("simulator.cli.KafkaSink") as mock_sink_cls, patch("simulator.cli.Fleet") as mock_fleet_cls:
        mock_fleet = MagicMock()
        mock_fleet_cls.return_value = mock_fleet

        result = runner.invoke(
            app,
            ["live", "--kafka-bootstrap", "explicit-flag-broker:9092", "--duration", "0.1"],
        )
        assert result.exit_code == 0, f"CLI exited with error: {result.stdout}"

        mock_sink_cls.assert_called_once()
        _, kwargs = mock_sink_cls.call_args
        assert kwargs.get("bootstrap_servers") == "explicit-flag-broker:9092", (
            f"Expected explicit-flag-broker:9092 from flag, got {kwargs.get('bootstrap_servers')}"
        )
