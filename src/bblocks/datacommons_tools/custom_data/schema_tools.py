from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from bblocks.datacommons_tools.custom_data.models.mcf import MCFNodes, MCFNode
from bblocks.datacommons_tools.custom_data.models.stat_vars import (
    StatVarMCFNode,
    StatVarGroupMCFNode,
)


def _rows_to_stat_var_nodes(
    data: pd.DataFrame,
    node_type: Literal["Node", "StatVar", "StatVarGroup"] = "StatVar",
) -> MCFNodes[StatVarMCFNode]:
    """Convert a DataFrame into a collection of ``StatVarMCFNode`` objects.

    Empty/NA values are removed from each row before constructing the node.

    Args:
        data: A pandas ``DataFrame`` where every row describes a StatVar.
        node_type: The type of node to create. Default is "StatVar".

    Returns:
        A ``Nodes`` container with one ``StatVarMCFNode`` per row.
    """

    records = data.to_dict(orient="records")
    nodes = []

    constructor = {
        "Node": MCFNode,
        "StatVar": StatVarMCFNode,
        "StatVarGroup": StatVarGroupMCFNode,
    }

    for record in records:
        record = {k: v for k, v in record.items() if not pd.isna(v) and v != ""}
        nodes.append(constructor[node_type](**record))

    return MCFNodes(nodes=nodes)


def csv_metadata_to_nodes(
    file_path: str | Path,
    *,
    node_type: Literal["Node", "StatVar", "StatVarGroup"] = "StatVar",
    column_to_property_mapping: dict[str, str] = None,
    csv_options: dict[str, Any] = None,
    ignore_columns: list[str] = None,
) -> MCFNodes[StatVarMCFNode]:
    """Read a CSV of StatVar metadata and return the corresponding MCF StatVar nodes.

    Args:
        file_path: Path to the CSV file.
        node_type: The type of node to create. Default is "StatVar".
        column_to_property_mapping: Optional map from CSV column names to
            ``StatVarMCFNode`` attribute names.
        csv_options: Extra keyword arguments forwarded verbatim to
            ``pandas.read_csv``.
        ignore_columns: Optional list of columns to ignore when reading the CSV.

    Returns:
        A ``Nodes`` container populated with ``StatVarMCFNode`` objects.
    """

    if column_to_property_mapping is None:
        column_to_property_mapping = {}

    if csv_options is None:
        csv_options = {}

    if ignore_columns is None:
        ignore_columns = []

    return (
        pd.read_csv(file_path, **csv_options)
        .drop(columns=ignore_columns)
        .rename(columns=column_to_property_mapping)
        .pipe(_rows_to_stat_var_nodes, node_type=node_type)
    )


def to_camelCase(segment: str) -> str:
    """
    Turn a segment like 'Official Development Assistance' into 'officialDevelopmentAssistance'.
    Keep all-upper or already-camel segments (e.g. DAC1, ODA) unchanged.
    """
    seg = segment.strip()

    # All upper case
    if re.fullmatch(r"[A-Z0-9]+", seg):
        return seg

    # Already camel case
    if seg and seg[0].islower() and " " not in seg:
        return seg

    # Split by whitespace and join with camel case
    words = re.split(r"\s+", seg)
    return words[0].lower() + "".join(w.title() for w in words[1:])


def build_stat_var_groups_from_strings(stat_vars, *, groups_namespace: str):
    """
    Build hierarchical StatVarGroup nodes from string-encoded group paths and attach them to StatVar nodes.

    This function reads the `memberOf` attribute of each StatVar node in `stat_vars`, which is
    expected to be a slash-separated string path describing its group hierarchy (e.g.,
    "Economic/Employment/Unemployment"). It generates StatVarGroupMCFNode objects for each
    group level, sets up their parent-child relationships, and updates the original StatVar
    nodes to reference the deepest group DCID.

    Args:
        stat_vars: An MCFNodes container holding StatVarMCFNode objects. Each node must have a
            `memberOf` attribute set to a path string indicating its group hierarchy.
        groups_namespace: The namespace under which group DCIDs will be created (e.g.,
            "one"). The resulting group DCIDs will have the form
            "dcid: {groups_namespace}/g/{{groupSlug}}".

    Returns:
        The same MCFNodes container provided as `stat_vars`, extended in-place with newly
        created StatVarGroupMCFNode objects representing each unique group. Each StatVarMCFNode's
        `memberOf` will be updated to the DCID of its deepest group.
    """

    group_nodes, seen = [], set()
    root = f"dcid: {groups_namespace}/g/"

    for node_idx, node in enumerate(stat_vars.nodes):
        # clean
        raw = node.memberOf
        raw = raw.lstrip("-").strip("/ ")
        parts = [p for p in raw.split("/") if p]
        slug_parts = [to_camelCase(part) for part in parts]

        for idx, part in enumerate(parts):
            group_node = root + f"{slug_parts[idx]}"

            if idx == len(parts) - 1:
                stat_vars.nodes[node_idx].memberOf = group_node

            if group_node in seen:
                continue
            seen.add(group_node)

            if idx == 0:
                parent = "dcid: dc/g/Root"
            else:
                parent = root + f"{slug_parts[idx - 1]}"

            group_nodes.append(
                StatVarGroupMCFNode(Node=group_node, name=part, specializationOf=parent)
            )

    stat_vars.nodes.extend(group_nodes)

    return stat_vars
