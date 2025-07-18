import pandas as pd

from bblocks.datacommons_tools.custom_data.models.stat_vars import StatVarMCFNode
from bblocks.datacommons_tools.custom_data.schema_tools import _rows_to_stat_var_nodes


def test_search_description_serialization_str_and_list():
    sv_str = StatVarMCFNode(Node="dcid:n1", name="Var", searchDescription="A")
    assert 'searchDescription: "A"' in sv_str.mcf

    sv_str = StatVarMCFNode(
        Node="dcid:n1",
        name="Var",
        searchDescription=["A string, or not", "B string, other"],
    )
    assert 'searchDescription: "A string, or not", "B string, other"' in sv_str.mcf

    sv_list = StatVarMCFNode(Node="dcid:n2", name="Var", searchDescription=["A", "B"])
    assert 'searchDescription: "A", "B"' in sv_list.mcf


def test_rows_to_stat_var_nodes_parses_comma_separated():
    df = pd.DataFrame(
        {"Node": ["dcid:n3"], "name": ["Var"], "searchDescription": ["A, B"]}
    )
    nodes = _rows_to_stat_var_nodes(df)
    mcf = nodes.nodes[0].mcf
    assert 'searchDescription: "A", "B"' in mcf


def test_rows_to_stat_var_nodes_parses_spreadsheet_lists():
    df = pd.DataFrame(
        {
            "Node": ["dcid:n3"],
            "name": ["Var"],
            "searchDescription": ['["A list, comma", "second element"]'],
        }
    )
    nodes = _rows_to_stat_var_nodes(df)
    mcf = nodes.nodes[0].mcf
    assert 'searchDescription: "A list, comma", "second element"' in mcf


def test_rows_to_stat_var_nodes_parses_spreadsheet_lists_no_quotes():
    df = pd.DataFrame(
        {
            "Node": ["dcid:n3"],
            "name": ["Var"],
            "memberOf": ['["dcid:oneId", "dcid:twoId"]'],
        }
    )
    nodes = _rows_to_stat_var_nodes(df)
    mcf = nodes.nodes[0].mcf
    assert "memberOf: dcid:oneId, dcid:twoId" in mcf
