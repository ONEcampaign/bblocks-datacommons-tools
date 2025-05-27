"""Command line interface for ``bblocks.datacommons_tools`` package."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from bblocks.datacommons_tools.custom_data.schema_tools import csv_metadata_to_nodes

__all__ = ["main"]


def _add_csv2mcf_subparser(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``csv2mcf`` subcommand."""

    parser = subparsers.add_parser(
        "csv2mcf", help="Convert a CSV of StatVar metadata to an MCF file"
    )
    parser.add_argument("csv", type=Path, help="Path to the input CSV file")
    parser.add_argument("mcf", type=Path, help="Path to write the generated MCF")
    parser.add_argument(
        "--node-type",
        choices=["Node", "StatVar", "StatVarGroup"],
        default="StatVar",
        help="Type of node to create (default: %(default)s)",
    )
    parser.add_argument(
        "--override",
        action="store_true",
        help="Overwrite the output file if it exists",
    )


def _handle_csv2mcf(args: argparse.Namespace) -> int:
    nodes = csv_metadata_to_nodes(args.csv, node_type=args.node_type)
    nodes.export_to_mcf_file(args.mcf, override=args.override)
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    """Entry point for the command line interface."""

    parser = argparse.ArgumentParser(
        description="Utilities for working with Data Commons files"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    _add_csv2mcf_subparser(subparsers)

    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "csv2mcf":
        return _handle_csv2mcf(args)

    parser.error(f"Unknown command: {args.command}")
    return 1


if __name__ == "__main__":  # pragma: no cover - manual invocation
    raise SystemExit(main())
