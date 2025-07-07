from pathlib import Path
from unittest.mock import Mock, patch

from bblocks.datacommons_tools.cli import main


def test_upload_command_invokes_pipeline(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    directory.mkdir()
    with (
        patch("bblocks.datacommons_tools.cli.common.get_kg_settings") as get,
        patch("bblocks.datacommons_tools.cli.upload.upload_to_cloud_storage") as upload,
    ):
        get.return_value = Mock()
        exit_code = main(
            ["upload", "--settings-file", "s.json", "--directory", str(directory)]
        )
        assert exit_code == 0
        get.assert_called_once_with(source="json", file=Path("s.json"))
        upload.assert_called_once_with(settings=get.return_value, directory=directory)


def test_dataload_command_invokes_pipeline() -> None:
    with (
        patch("bblocks.datacommons_tools.cli.common.get_kg_settings") as get,
        patch("bblocks.datacommons_tools.cli.data_load.run_data_load") as run,
    ):
        get.return_value = Mock()
        exit_code = main(["dataload", "--timeout", "5", "--env-file", "e"])
        assert exit_code == 0
        get.assert_called_once_with(env_file=Path("e"))
        run.assert_called_once_with(settings=get.return_value, timeout=5)


def test_redeploy_command_invokes_pipeline() -> None:
    with (
        patch("bblocks.datacommons_tools.cli.common.get_kg_settings") as get,
        patch("bblocks.datacommons_tools.cli.redeploy.redeploy_service") as run,
    ):
        get.return_value = Mock()
        exit_code = main(["redeploy", "--timeout", "9"])
        assert exit_code == 0
        get.assert_called_once_with(env_file=None)
        run.assert_called_once_with(settings=get.return_value, timeout=9)


def test_pipeline_command_runs_all(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    with (
        patch("bblocks.datacommons_tools.cli.common.get_kg_settings") as get,
        patch(
            "bblocks.datacommons_tools.cli.data_load_pipeline.upload_to_cloud_storage"
        ) as upload,
        patch("bblocks.datacommons_tools.cli.data_load_pipeline.run_data_load") as load,
        patch(
            "bblocks.datacommons_tools.cli.data_load_pipeline.redeploy_service"
        ) as red,
    ):
        get.return_value = Mock()
        exit_code = main(
            [
                "pipeline",
                "--settings-file",
                "s.json",
                "--directory",
                str(directory),
                "--load-timeout",
                "7",
                "--deploy-timeout",
                "3",
            ]
        )
        assert exit_code == 0
        get.assert_called_once_with(source="json", file=Path("s.json"))
        upload.assert_called_once_with(settings=get.return_value, directory=directory)
        load.assert_called_once_with(settings=get.return_value, timeout=7)
        red.assert_called_once_with(settings=get.return_value, timeout=3)
