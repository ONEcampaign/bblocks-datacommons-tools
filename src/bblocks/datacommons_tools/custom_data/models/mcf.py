from typing import Optional

from pydantic import BaseModel, ConfigDict

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

        return "\n".join(lines) + "\n"
