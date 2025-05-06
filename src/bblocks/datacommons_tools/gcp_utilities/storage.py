from pathlib import Path
from typing import Iterable

from google.cloud.storage import Bucket

from bblocks.datacommons_tools.logger import logger

_SKIP_IN_SUBDIR = {".json"}


def _iter_local_files(directory: Path) -> Iterable[Path]:
    """Yield all the files to be uploaded (excluding the skipped ones in subdirectories)

    Args:
        directory (Path): The directory to iterate through.

    """
    for path in directory.rglob("*"):
        if not path.is_file():
            continue
        if path.parent != directory and path.suffix in _SKIP_IN_SUBDIR:
            continue
        yield path


def upload_directory_to_gcs(
    bucket: Bucket, directory: Path, gcs_folder_name: str
) -> None:
    """Upload a local directory to Google Cloud Storage. Folder structures
    is maintained in the GCS bucket in a specified base folder

    Args:
        bucket (Bucket): GCS bucket instance.
        directory (Path): Local directory to upload.
        gcs_folder_name (str): Name of the base folder in the GCS bucket to store the data

    Raises:
        FileNotFoundError: If the specified directory does not exist.
    """
    if not directory.exists():
        raise FileNotFoundError(f"The directory {directory} does not exist.")

    files_uploaded = 0

    for local_path in _iter_local_files(directory):
        remote_path = f"{gcs_folder_name}/{local_path.relative_to(directory)}"
        bucket.blob(remote_path).upload_from_filename(str(local_path))
        logger.info(f"Uploaded {local_path} to {remote_path}")
        files_uploaded += 1

    logger.info(
        f"Uploaded {files_uploaded} files to {gcs_folder_name} in GCS bucket {bucket.name}"
    )
