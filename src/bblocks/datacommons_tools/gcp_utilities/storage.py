from pathlib import Path
from typing import Iterable

from bblocks.datacommons_tools.custom_data.models.config_file import Config

from google.cloud.storage import Bucket

from bblocks.datacommons_tools.logger import logger

_SKIP_IN_SUBDIR = {".json"}
_VALID_EXTENSIONS = {".csv", ".json", ".mcf"}

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
        if local_path.suffix not in _VALID_EXTENSIONS:
            logger.warning(f"Skipping unsupported file type: {local_path}")
            continue
        remote_path = f"{gcs_folder_name}/{local_path.relative_to(directory)}"
        bucket.blob(remote_path).upload_from_filename(str(local_path))
        logger.info(f"Uploaded {local_path} to {remote_path}")
        files_uploaded += 1

    logger.info(
        f"Uploaded {files_uploaded} files to {gcs_folder_name} in GCS bucket {bucket.name}"
    )


def list_bucket_files(bucket: Bucket, gcs_folder_name: str) -> list[str]:
    """Return the list of blob names in ``gcs_folder_name``.

    Args:
        bucket (Bucket): GCS bucket instance.
        gcs_folder_name (str): Folder path prefix in the bucket.

    Returns:
        list[str]: Blob names found under the given prefix.
    """

    blobs = bucket.list_blobs(prefix=gcs_folder_name)
    return [blob.name for blob in blobs]


def get_unregistered_csv_files(
    bucket: Bucket, gcs_folder_name: str, config: Config
) -> list[str]:
    """Identify CSV files in the bucket not referenced in ``config``.

    Args:
        bucket (Bucket): GCS bucket instance.
        gcs_folder_name (str): Folder path prefix in the bucket.
        config (Config): Parsed configuration object.

    Returns:
        list[str]: CSV file names present in the bucket but missing from
            ``config.inputFiles``.
    """

    blob_names = list_bucket_files(bucket=bucket, gcs_folder_name=gcs_folder_name)
    csv_files = [Path(name).name for name in blob_names if Path(name).suffix == ".csv"]

    registered = set(config.inputFiles.keys())
    return [name for name in csv_files if name not in registered]


def delete_bucket_files(bucket: Bucket, blob_names: Iterable[str]) -> None:
    """Delete the specified blobs from ``bucket``.

    Args:
        bucket (Bucket): GCS bucket instance.
        blob_names (Iterable[str]): Names of the blobs to delete.
    """

    for name in blob_names:
        bucket.blob(name).delete()
        logger.info(f"Deleted {name} from bucket {bucket.name}")
