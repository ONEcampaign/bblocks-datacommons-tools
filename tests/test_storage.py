from unittest.mock import Mock

import pandas as pd

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


def test_get_bucket_files_csv_single():
    bucket = Mock()
    blob = Mock()
    blob.download_as_bytes.return_value = b"a,b\n1,2\n"
    bucket.blob.return_value = blob

    result = get_bucket_files(bucket, "a.csv")

    bucket.blob.assert_called_once_with("a.csv")
    blob.download_as_bytes.assert_called_once_with()
    expected = pd.DataFrame({"a": [1], "b": [2]})
    pd.testing.assert_frame_equal(result, expected)


def test_get_bucket_files_multiple_types():
    bucket = Mock()
    blob_csv = Mock()
    blob_json = Mock()
    blob_mcf = Mock()

    blob_csv.download_as_bytes.return_value = b"a,b\n1,2\n"
    blob_json.download_as_bytes.return_value = b'{"x": 1}'
    blob_mcf.download_as_bytes.return_value = b'Node: n\nname: "N"\ntypeOf: T\n\n'

    def blob_side(name: str):
        return {"a.csv": blob_csv, "b.json": blob_json, "c.mcf": blob_mcf}[name]

    bucket.blob.side_effect = blob_side

    result = get_bucket_files(bucket, ["a.csv", "b.json", "c.mcf"])

    expected_df = pd.DataFrame({"a": [1], "b": [2]})
    pd.testing.assert_frame_equal(result["a.csv"], expected_df)
    assert result["b.json"] == {"x": 1}
    assert result["c.mcf"].nodes[0].Node == "n"
