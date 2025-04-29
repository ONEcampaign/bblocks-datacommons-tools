from __future__ import annotations

from os import PathLike
from pathlib import Path
from typing import Optional, List

from pydantic import BaseModel, ConfigDict, Field

from bblocks.datacommons_tools.custom_data.models.common import QuotedStr


class MCFNode(BaseModel):
    """Represents a general node for Metadata Common Format (MCF).

    Attributes:
        Node: Identifier for the Node.
        name: The human-readable name for the Node.
        typeOf: The DCID representing the typeOf this Node.
        dcid: Optional DCID for uniquely identifying the Node.
        description: Optional human-readable description.
        provenance: Optional provenance information.
        shortDisplayName: Optional human-readable short name for display.
        subClassOf: Optional DCID indicating the 'parent' Node class.
    """

    Node: str
    name: QuotedStr
    typeOf: str
    dcid: Optional[str] = None
    description: Optional[QuotedStr] = None
    provenance: Optional[QuotedStr] = None
    shortDisplayName: Optional[QuotedStr] = None
    subClassOf: Optional[str] = None

    # Allow extra fields since MCF can have arbitrary properties and this
    # class is not comprehensive of all possible MCF properties.
    model_config = ConfigDict(extra="allow")

    @property
    def mcf(self) -> str:
        """Generates an MCF-formatted string representing this node.

        Returns:
            A string formatted according to MCF conventions, sorted alphabetically
                except for 'Node', which appears first.
        """
        data = self.model_dump(exclude_none=True)

        # Pull Node first, then sort for consistent ordering
        lines = [f"Node: {data.pop('Node')}"]
        lines.extend(f"{k}: {v}" for k, v in sorted(data.items()))

        return "\n".join(lines) + "\n\n"


class MCFNodes(BaseModel):
    """Represents a collection of Nodes.

    Attributes:
        nodes: A list of Node instances.
    """

    nodes: List[MCFNode] = Field(default_factory=list)

    def _flush(self, block: dict[str, str]) -> None:
        """Convert the current block into an `MCFNode` and store it."""
        if not block:
            return
        if "Node" not in block:
            raise ValueError(
                f"Missing mandatory 'Node:' line in block starting with "
                f"{next(iter(block.items()))!r}"
            )
        self.nodes.append(MCFNode(**block))
        block.clear()

    def load_from_mcf_file(self, file_path: str | PathLike) -> MCFNodes:
        """Parses MCF nodes from a file and populates the collection.

        Each node block is expected to start with
            ``Node: <identifier>``
        followed by one or more ``key: value`` lines, and is delimited
        by a blank line (or EOF).

        Args:
            file_path: The path of the MCF file to read.
        """

        path = Path(file_path)
        current_block: dict[str, str] = {}

        with path.open(encoding="utf-8") as file_obj:
            for line_no, raw_line in enumerate(file_obj, start=1):
                stripped = raw_line.strip()

                # Blank line means end of current block
                if not stripped:
                    self._flush(current_block)
                    continue

                key, sep, value = stripped.partition(":")
                if not sep or not key or not value:
                    raise ValueError(
                        f"Invalid MCF syntax on line {line_no}: {raw_line!r}"
                    )
                current_block[key.strip()] = value.strip()

        # Handle the final block if the file does not end with a blank line.
        self._flush(current_block)

        return self

    def add(self, node: MCFNode) -> MCFNodes:
        """Adds a new node to the collection.

        Args:
            node: The MCFNode instance to add.
        """
        self.nodes.append(node)

        return self

    def remove(self, node_id: str) -> MCFNodes:
        """Removes a node from the collection by its ID.

        Args:
            node_id: The ID of the node to remove.
        """
        initial_len = len(self.nodes)
        self.nodes = [node for node in self.nodes if node.Node != node_id]

        if len(self.nodes) == initial_len:
            raise ValueError(f"Node with ID {node_id} not found in the collection.")

        return self

    def export_to_mcf_file(
        self, file_path: str | PathLike, *, overwrite: bool = True
    ) -> MCFNodes:
        """Exports the MCF nodes to a file.

        Args:
            file_path: The path of the file to which to export.
            overwrite: If True, overwrite the file if it exists. If False, append to the file.
        """
        mode = "w" if overwrite else "a"

        with open(file_path, mode) as f:
            for node in self.nodes:
                f.write(node.mcf)

        return self
