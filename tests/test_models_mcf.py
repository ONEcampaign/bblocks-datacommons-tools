import pytest

from bblocks.datacommons_tools.custom_data.models.mcf import MCFNodes, MCFNode


def test_mcfnode_mcf_output_order_and_formatting():
    """
    Ensures MCFNode.mcf outputs properties sorted alphabetically
    after 'Node:' line.
    """
    node = MCFNode(
        Node="dcid:TestNode", name='"My Name"', typeOf="dcid:TypeA", description='"Desc"'
    )
    lines = node.mcf.strip().splitlines()
    assert lines[0] == "Node: dcid:TestNode"
    assert lines[1] == 'name: "My Name"'
    assert lines[2] == "typeOf: dcid:TypeA"
    assert lines[3] == 'description: "Desc"'


def test_mcfnodes_add_override_and_remove():
    """
    Tests adding nodes, override behavior, and removal from MCFNodes.
    """
    nodes = MCFNodes()
    node1 = MCFNode(Node="dcid:n1", name='"First"', typeOf="dcid:T1")
    nodes.add(node1)
    assert nodes._expect_present("dcid:n1") == 0

    # Adding same node without override should error
    node1b = MCFNode(Node="dcid:n1", name='"Second"', typeOf="dcid:T1")
    with pytest.raises(ValueError):
        nodes.add(node1b, override=False)

    # Override replaces existing
    nodes.add(node1b, override=True)
    assert nodes.nodes[0].name == '"Second"'

    # Remove node
    nodes.remove("dcid:n1")
    with pytest.raises(ValueError):
        nodes._expect_present("n1")
