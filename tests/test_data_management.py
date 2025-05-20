import pandas as pd
import pytest

from bblocks.datacommons_tools import CustomDataManager
from bblocks.datacommons_tools.custom_data.data_management import DEFAULT_GROUP_NAME
from bblocks.datacommons_tools.custom_data.models.config_file import Config


def test_custom_data_manager_add_provenance_and_override():
    """
    Verifies provenance addition logic in CustomDataManager.
    """
    manager = CustomDataManager()
    # Missing source_url should fail
    with pytest.raises(ValueError):
        manager.add_provenance(
            provenance_name="pA", provenance_url="http://prov", source_name="new_source"
        )
    # Provide source_url to succeed
    manager.add_provenance(
        provenance_name="pA",
        provenance_url="http://prov",
        source_name="new_source",
        source_url="http://src",
    )

    # Adding same provenance without override should error
    with pytest.raises(ValueError):
        manager.add_provenance(
            provenance_name="pA",
            provenance_url="http://prov2",
            source_name="new_source",
        )

    # Override existing provenance URL
    manager.add_provenance(
        provenance_name="pA",
        provenance_url="http://prov2",
        source_name="new_source",
        override=True,
    )
    # Confirm update
    src = manager._config.sources["new_source"]
    assert src.provenances["pA"].unicode_string() == "http://prov2/"


def test_custom_data_manager_add_variable_to_config_and_override():
    """
    Ensures variables are registered and override works in config.
    """
    manager = CustomDataManager()
    manager.add_variable_to_config(statVar="v1", name="Var1")
    # Duplicate without override errors
    with pytest.raises(ValueError):
        manager.add_variable_to_config(statVar="v1", name="Var1New")
    # Override succeeds
    manager.add_variable_to_config(statVar="v1", name="Var1New", override=True)
    assert manager._config.variables["v1"].name == "Var1New"


def test_add_implicit_and_explicit_schema_file_registration_and_override(tmp_path):
    """
    Verifies implicit/explicit schema file registration in config and data,
    and override/error behaviors.
    """
    manager = CustomDataManager()
    manager.add_provenance("p1", "http://prov", "s1", source_url="http://src")

    df1 = pd.DataFrame({"A": [1, 2]})
    manager.add_implicit_schema_file(
        file_name="imp.csv",
        provenance="p1",
        data=df1,
        entityType="Country",
        observationProperties={"unit": "U"},
    )
    assert "imp.csv" in manager._config.inputFiles
    assert "imp.csv" in manager._data

    df2 = pd.DataFrame({"A": [3, 4]})
    with pytest.raises(ValueError):
        manager.add_implicit_schema_file(
            "imp.csv", provenance="p1", entityType="Country"
        )

    manager.add_implicit_schema_file(
        file_name="imp.csv",
        provenance="p1",
        data=df2,
        override=True,
        entityType="State",
    )
    pd.testing.assert_frame_equal(manager._data["imp.csv"], df2)

    df3 = pd.DataFrame({"entity": ["e1"], "Year": [2020], "Value": [100]})
    manager.add_explicit_schema_file(
        file_name="exp.csv",
        provenance="p1",
        data=df3,
        columnMappings={"entity": "entity", "date": "Year", "value": "Value"},
    )
    assert "exp.csv" in manager._config.inputFiles
    assert "exp.csv" in manager._data

    df4 = pd.DataFrame({"X": [1]})
    with pytest.raises(ValueError):
        manager.add_data(df4, "no_file.csv")


def test_add_explicit_schema_file_without_column_mappings():
    """Ensure missing columnMappings defaults to empty dict without error."""
    manager = CustomDataManager()
    manager.add_provenance("p1", "http://prov", "s1", source_url="http://src")

    df = pd.DataFrame({"A": [1]})
    manager.add_explicit_schema_file(
        file_name="exp.csv",
        provenance="p1",
        data=df,
    )

    assert "exp.csv" in manager._config.inputFiles
    mappings = manager._config.inputFiles["exp.csv"].columnMappings
    assert mappings.model_dump(exclude_none=True) == {}


def test_export_methods(tmp_path):
    """
    Exercises export_config, export_data, and export_mcf_file.
    """
    manager = CustomDataManager()
    manager.add_provenance("p1", "http://prov", "s1", source_url="http://src")
    df = pd.DataFrame({"A": [1]})
    manager.add_implicit_schema_file(
        file_name="data.csv",
        provenance="p1",
        data=df,
        entityType="Country",
        observationProperties={"unit": "U"},
    )
    manager.add_variable_to_mcf(Node="vX", name="VX")

    manager.export_config(tmp_path)
    config_file = tmp_path / "config.json"
    assert config_file.exists()
    loaded = Config.from_json(str(config_file))
    assert isinstance(loaded, Config)

    manager.export_data(tmp_path)
    data_file = tmp_path / "data.csv"
    assert data_file.exists()

    manager.export_mfc_file(tmp_path, mcf_file_name="custom_nodes.mcf")
    mcf_file = tmp_path / "custom_nodes.mcf"
    assert mcf_file.exists()
    assert "Node: vX" in mcf_file.read_text()


def test_add_variable_group_to_mcf_and_override():
    """
    Checks StatVarGroup node addition and override behavior.
    """
    manager = CustomDataManager()
    manager.add_variable_group_to_mcf(
        Node="test/g/1", name="Group1", specializationOf="dcid:dc/g/Root"
    )
    groups = manager._mcf_nodes[DEFAULT_GROUP_NAME].nodes
    assert any(
        n.Node == "test/g/1" and n.specializationOf == "dcid:dc/g/Root" for n in groups
    )

    manager.add_variable_group_to_mcf(
        Node="test/g/1", name="Group2", specializationOf="dcid:dc/g/Root", override=True
    )
    updated = manager._mcf_nodes[DEFAULT_GROUP_NAME].nodes
    assert any(n.name == "Group2" for n in updated if n.Node == "test/g/1")


def test_config_round_trip(tmp_path):
    """
    Ensures a Config can be dumped to JSON and loaded back identically.
    """
    cfg = Config(inputFiles={}, sources={})
    path = tmp_path / "cfg.json"
    path.write_text(cfg.model_dump_json())
    loaded = Config.from_json(str(path))
    assert loaded.model_dump() == cfg.model_dump()


def test_custom_data_manager_repr():
    """
    Sanity-check CustomDataManager.__repr__ for correct counts.
    """
    manager = CustomDataManager()
    manager.add_provenance("p1", "http://prov", "s1", source_url="http://src")
    manager.add_variable_to_config(statVar="v1", name="Var1")
    df = pd.DataFrame({"A": [1]})
    manager.add_implicit_schema_file(
        file_name="f.csv",
        provenance="p1",
        data=df,
        entityType="Country",
        observationProperties={"unit": "u"},
    )
    manager.add_variable_to_mcf(Node="vX", name="VX")
    r = repr(manager)
    assert "1 inputFiles" in r
    assert "1 containing data" in r
    assert "1 sources" in r
    assert "2 variables" in r
