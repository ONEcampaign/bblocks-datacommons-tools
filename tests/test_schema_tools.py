from bblocks.datacommons_tools.custom_data.schema_tools import csv_metadata_to_nodes


def test_csv_metadata_to_nodes(tmp_path):
    """
    Validates CSV-to-MCF conversion, ignoring columns and custom mappings.
    """
    content = (
        "Node,name,typeOf,extra\n"
        "n1,Name1,dcid:StatisticalVariable,\n"
        "n2,Name2,dcid:StatisticalVariable,prop\n"
    )
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(content)

    nodes = csv_metadata_to_nodes(str(csv_path))
    assert len(nodes.nodes) == 2

    nodes_ignore = csv_metadata_to_nodes(str(csv_path), ignore_columns=["extra"])
    for node in nodes_ignore.nodes:
        assert not hasattr(node, "extra")

    mapping = {"extra": "searchDescription"}
    nodes_map = csv_metadata_to_nodes(str(csv_path), column_to_property_mapping=mapping)
    for node in nodes_map.nodes:
        assert hasattr(node, "searchDescription")
