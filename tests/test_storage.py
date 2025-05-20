from unittest.mock import Mock

from bblocks.datacommons_tools.gcp_utilities.storage import (
    list_bucket_files,
    get_unregistered_csv_files,
    delete_bucket_files,
    get_bucket_files,
)
from bblocks.datacommons_tools.custom_data.models.config_file import Config
from bblocks.datacommons_tools.custom_data.models.data_files import (
    ImplicitSchemaFile,
    ObservationProperties,
)
from bblocks.datacommons_tools.custom_data.models.sources import Source


def _minimal_config() -> Config:
    input_files = {
        "a.csv": ImplicitSchemaFile(
            provenance="prov",
            entityType="Country",
            observationProperties=ObservationProperties(),
        )
    }
    sources = {"src": Source(url="http://s", provenances={"prov": "http://p"})}
    return Config(inputFiles=input_files, sources=sources)


def test_list_bucket_files_with_prefix():
    bucket = Mock()
    blob_a = Mock()
    blob_a.name = "folder/a.csv"
    blob_b = Mock()
    blob_b.name = "folder/b.csv"
    bucket.list_blobs.return_value = [blob_a, blob_b]
    assert list_bucket_files(bucket, "folder") == ["folder/a.csv", "folder/b.csv"]
    bucket.list_blobs.assert_called_once_with(prefix="folder")


def test_list_bucket_files_root():
    bucket = Mock()
    blob_a = Mock()
    blob_a.name = "a.csv"
    bucket.list_blobs.return_value = [blob_a]
    assert list_bucket_files(bucket) == ["a.csv"]
    bucket.list_blobs.assert_called_once_with()


def test_get_unregistered_csv_files():
    bucket = Mock()
    blob_a = Mock()
    blob_a.name = "folder/a.csv"
    blob_extra = Mock()
    blob_extra.name = "folder/extra.csv"
    bucket.list_blobs.return_value = [blob_a, blob_extra]
    cfg = _minimal_config()
    missing = get_unregistered_csv_files(bucket, cfg, "folder")
    assert missing == ["extra.csv"]


def test_delete_bucket_files():
    bucket = Mock()
    blobs: dict[str, Mock] = {}

    def blob_side(name: str):
        b = Mock()
        blobs[name] = b
        return b

    bucket.blob.side_effect = blob_side

    delete_bucket_files(bucket, ["a.csv", "b.csv"])

    assert set(blobs.keys()) == {"a.csv", "b.csv"}
    for b in blobs.values():
        b.delete.assert_called_once()


def test_get_bucket_files():
    bucket = Mock()
    blob = Mock()
    blob.download_as_bytes.return_value = b"data"
    bucket.blob.return_value = blob

    result = get_bucket_files(bucket, "a.csv")

    bucket.blob.assert_called_once_with("a.csv")
    blob.download_as_bytes.assert_called_once_with()
    assert result == {"a.csv": b"data"}
