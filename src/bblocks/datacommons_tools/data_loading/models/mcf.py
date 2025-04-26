from typing import Optional, Any

from pydantic import BaseModel, ConfigDict, field_serializer

_PREFIXES = ("dcid:", "dcs:", "schema:")


class MCFNode(BaseModel):
    Node: str
    name: str
    typeOf: str
    dcid: Optional[str] = None
    description: Optional[str] = None
    provenance: Optional[str] = None
    shortDisplayName: Optional[str] = None
    subClassOf: Optional[str] = None

    model_config = ConfigDict(extra="allow")

    def __init__(self, **data: Any):
        super().__init__(**data)

    @field_serializer("*")
    def _quote_long_strings(self, v: Any, _info) -> Any:
        """Wrap any plain string longer than the threshold in double quotes."""
        if (
            isinstance(v, str)
            and " " in v
            and not v.startswith(_PREFIXES)
            and not v.startswith('"')
        ):
            return f'"{v}"'
        return v

    @property
    def mcf(self) -> str:
        data = self.model_dump(exclude_none=True)
        return "\n".join(f"{k}: {v}" for k, v in data.items()) + "\n"


