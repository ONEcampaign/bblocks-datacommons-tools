from enum import StrEnum
from typing import Optional, List, Dict, Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from bblocks.datacommons_tools.data_loading.models.common import (
    QuotedStr,
    QuotedStrList,
)
from bblocks.datacommons_tools.data_loading.models.mcf import MCFNode


class StatType(StrEnum):
    """Enumeration of statistical types"""

    MEASURED_VALUE = "dcid:measuredValue"
    MIN_VALUE = "dcid:minValue"
    MAX_VALUE = "dcid:maxValue"
    MEAN_VALUE = "dcid:meanValue"
    MEDIAN_VALUE = "dcid:medianValue"
    SUM_VALUE = "dcid:sumValue"
    VARIANCE_VALUE = "dcid:varianceValue"
    MARGIN_OF_ERROR = "dcid:marginOfError"
    STANDARD_ERROR = "dcid:stdErr"


class Variable(BaseModel):
    """Representation of the Variables section of the config file
    This section is optional in the config file

    Attributes:
        name: Name of the variable.
        description: Description of the variable.
        searchDescriptions: List of search descriptions for the variable.
        group: Group to which the variable belongs.
        properties: Properties of the variable.
    """

    name: Optional[str] = None
    description: Optional[str] = None
    searchDescriptions: Optional[List[str]] = None
    group: Optional[str] = None
    properties: Optional[Dict[str, str]] = None

    model_config = ConfigDict(extra="forbid")


class StatVarMCFNode(MCFNode):
    """Representation of a StatVar MCF node"""

    statType: Optional[StatType] = StatType.MEASURED_VALUE
    typeOf: Literal["dcid:StatisticalVariable"] = "dcid:StatisticalVariable"
    memberOf: Optional[str] = None
    searchDescription: Optional[QuotedStr | QuotedStrList] = None
    populationType: Optional[str] = None
    measuredProperty: Optional[str] = None
    measurementQualifier: Optional[str] = None
    measurementDenominator: Optional[str] = None

    def __init__(self, **data: Any):
        super().__init__(**data)


class StatVarGroupMCFNode(MCFNode):
    """Representation of a StatVarGroup MCF node"""

    typeOf: Literal["dcid:StatVarGroup"] = "dcid:StatVarGroup"
    specializationOf: str

    def __init__(self, **data: Any):
        super().__init__(**data)

    @field_validator("Node", "specializationOf")
    @classmethod
    def _check_g(cls, v: str) -> str:
        if "g/" not in v:
            raise ValueError("field must contain 'g/'")
        return v

    @field_validator("specializationOf")
    @classmethod
    def _check_specialization_format(cls, v: str) -> str:
        if not v.startswith("dcid:"):
            raise ValueError("specializationOf must start with 'dcid:'")
        return v
