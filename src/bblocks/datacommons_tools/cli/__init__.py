"""Command line interface for ``bblocks.datacommons_tools`` package."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from bblocks.datacommons_tools.custom_data.schema_tools import (
    csv_metadata_to_nodes,
    NodeTypes,
    csv_metadata_to_mfc_file,
)

__all__ = ["main"]


def _kv_pair(value: str) -> tuple[str, str]:
    """Parse a key-value strung and return it as a key, value tuple."""
    if "=" not in value:
        raise ValueError(f"Invalid key-value pair: {value}")
    key, val = value.split("=", 1)
    return key.strip(), val.strip()


def _add_csv2mcf_subparser(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``csv2mcf`` subcommand."""

    # Create the subparser for the csv2mcf command
    parser = subparsers.add_parser(
        "csv2mcf", help="Convert a CSV of Node metadata to an MCF file"
    )

    # Positional arguments
    parser.add_argument("csv", type=Path, help="Path to the input CSV file")
    parser.add_argument("mcf", type=Path, help="Path to write the generated MCF")

    # Keyword arguments
    parser.add_argument(
        "--node-type",
        choices=[node_type.value for node_type in NodeTypes],
        default="Node",
        help="Type of node to create (default: %(default)s)",
    )

    parser.add_argument(
        "--column-mapping",
        metavar="CSV_COL=MCF_PROP",
        type=_kv_pair,
        action="append",
        help=(
            "Map CSV column names to MCF properties. "
            "May be used multiple times, eg:"
            "--column-mapping description=searchDescription --column-mapping indicator=Node"
        ),
    )

    parser.add_argument(
        "--csv-option",
        metavar="KEY=VALUE",
        type=_kv_pair,
        action="append",
        help=(
            "Extra keyword arguments forwarded to pandas.read_csv, "
            'e.g. --csv-option delimiter=";" --csv-option encoding=UTF-8'
        ),
    )

    parser.add_argument(
        "--ignore-column",
        metavar="COLUMN",
        action="append",
        help="Name of a CSV column to ignore. May be specified multiple times.",
    )

    parser.add_argument(
        "--override", action="store_true", help="Overwrite the output file if it exists"
    )


def main(argv: Iterable[str] | None = None) -> int:
    """Entry point for the command line interface."""

    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="Utilities for working with Data Commons files"
    )
    # Add subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Register the csv2mcf subcommand
    _add_csv2mcf_subparser(subparsers)

    # Get the arguments from the command line
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "csv2mcf":
        column_mapping = dict(args.column_mapping) if args.column_mapping else None
        csv_options = dict(args.csv_option) if args.csv_option else None

        csv_metadata_to_mfc_file(
            csv_path=args.csv,
            mcf_path=args.mcf,
            node_type=args.node_type,
            column_to_property_mapping=column_mapping,
            csv_options=csv_options,
            ignore_columns=args.ignore_column,
            override=args.override,
        )
        return 0

    parser.error(f"Unknown command: {args.command}")

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
