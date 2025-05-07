from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from bblocks.datacommons_tools.custom_data.models.mcf import MCFNodes
from bblocks.datacommons_tools.custom_data.models.stat_vars import StatVarMCFNode


def _rows_to_stat_var_nodes(data: pd.DataFrame) -> MCFNodes[StatVarMCFNode]:
    """Convert a DataFrame into a collection of ``StatVarMCFNode`` objects.

    Empty/NA values are removed from each row before constructing the node.

    Args:
        data: A pandas ``DataFrame`` where every row describes a StatVar.

    Returns:
        A ``Nodes`` container with one ``StatVarMCFNode`` per row.
    """

    records = data.to_dict(orient="records")
    nodes = []
    for record in records:
        record = {k: v for k, v in record.items() if not pd.isna(v) and v != ""}
        nodes.append(StatVarMCFNode(**record))

    return MCFNodes(nodes=nodes)


def csv_metadata_to_nodes(
    file_path: str | Path,
    *,
    column_to_property_mapping: dict[str, str] = None,
    csv_options: dict[str, Any] = None,
    ignore_columns: list[str] = None,
) -> MCFNodes[StatVarMCFNode]:
    """Read a CSV of StatVar metadata and return the corresponding MCF StatVar nodes.

    Args:
        file_path: Path to the CSV file.
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
        .pipe(_rows_to_stat_var_nodes)
    )
