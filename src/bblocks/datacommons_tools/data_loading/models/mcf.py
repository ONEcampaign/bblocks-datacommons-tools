from typing import Optional, Any

from pydantic import BaseModel, ConfigDict, field_serializer

from bblocks.datacommons_tools.data_loading.models.common import QuotedStr

_PREFIXES = ("dcid:", "dcs:", "schema:")


class MCFNode(BaseModel):
    Node: str
    name: QuotedStr
    typeOf: str
    dcid: Optional[str] = None
    description: Optional[QuotedStr] = None
    provenance: Optional[QuotedStr] = None
    shortDisplayName: Optional[QuotedStr] = None
    subClassOf: Optional[str] = None

    model_config = ConfigDict(extra="allow")

    def __init__(self, **data: Any):
        super().__init__(**data)

    @property
    def mcf(self) -> str:
        data = self.model_dump(exclude_none=True)
        return "\n".join(f"{k}: {v}" for k, v in data.items()) + "\n"
