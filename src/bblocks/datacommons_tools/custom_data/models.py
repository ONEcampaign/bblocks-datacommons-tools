from enum import StrEnum
from typing import Optional, List, Literal, Dict, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
    HttpUrl,
    field_serializer,
    field_validator,
)

_PREFIXES = ("dcid:", "dcs:", "schema:")


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


class StatVarMCFNode(MCFNode):
    """Representation of a StatVar MCF node"""

    statType: Optional[StatType] = "dcid:measuredValue"
    memberOf: Optional[str] = None
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

class ObservationProperties(BaseModel):
    """Representation of the ObservationProperties section of the InputFiles section of the config file
    This is for implicit schema only

    Attributes:
        unit: Unit of the observation.
        observationPeriod: Observation period of the data.
        scalingFactor: Scaling factor for the data.
        measurementMethod: Measurement method used for the data.
    """

    unit: Optional[str] = None
    observationPeriod: Optional[str] = None
    scalingFactor: Optional[str] = None
    measurementMethod: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class ColumnMappings(BaseModel):
    """Representation of the ColumnMappings section of the InputFiles section of the config file
    This is for explicit schema only

    Attributes:
        variable: Variable name.
        entity: Entity name.
        date: Date of the observation.
        value: Value of the observation.
        unit: Unit of the observation.
        scalingFactor: Scaling factor for the data.
        measurementMethod: Measurement method used for the data.
        observationPeriod: Observation period of the data.
    """

    variable: Optional[str] = None
    entity: Optional[str] = None
    date: Optional[str] = None
    value: Optional[str] = None
    unit: Optional[str] = None
    scalingFactor: Optional[str] = None
    measurementMethod: Optional[str] = None
    observationPeriod: Optional[str] = None

    model_config = ConfigDict(extra="forbid")


class InputFile(BaseModel):
    """Representation of the InputFiles section of the config file

    Attributes:
        entityType: Type of the entity (implicit schema only).
        ignoreColumns: List of columns to ignore.
        provenance: Provenance of the data.
        data_format: Format of the data (variable per column or variable per row). Accepted values are "variablePerColumn" or "variablePerRow". If using explicit schema, this should be "variablePerRow". If using implicit schema, this should be "variablePerColumn" or omitted. This attribute is represented as "format" in the JSON.
        columnMappings:  If headings in the CSV file does not use the default names, the equivalent names for each column. (explicit schema only).
        observationProperties: Properties of the observation (implicit schema only).
    """

    entityType: Optional[str] = None
    ignoreColumns: Optional[List[str]] = None
    provenance: str
    data_format: Optional[Literal["variablePerColumn", "variablePerRow"]] = Field(
        default=None, alias="format"
    )  # represent the "format" field. Get around the protected name issue
    columnMappings: Optional[ColumnMappings] = None
    observationProperties: Optional[ObservationProperties] = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_schema_choice(self) -> "InputFile":
        """Validate that only one of implicit or explicit schema is used"""

        using_implicit = (
            self.entityType is not None or self.observationProperties is not None
        )
        using_explicit = (
            self.columnMappings is not None or self.data_format == "variablePerRow"
        )

        if using_implicit and using_explicit:
            raise ValueError(
                "Cannot use both implicit and explicit schema fields. Read more about implicit and explicit schemas here: https://docs.datacommons.org/custom_dc/custom_data.html#step-2-choose-between-implicit-and-explicit-schema-definition"
            )

        return self


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


class Source(BaseModel):
    """Representation of the Sources section of the config file

    Attributes:
        url: URL of the source.
        provenances: Dictionary of provenances. Each provenance name maps to a URL.
    """

    url: HttpUrl
    provenances: Dict[str, HttpUrl]  # Each provenance name maps to a URL

    model_config = ConfigDict(extra="forbid")


class Config(BaseModel):
    """Representation of the config file

    Attributes:
        includeInputSubdirs: Include input subdirectories.
        groupStatVarsByProperty: Group stat vars by property.
        inputFiles: Dictionary of input files.
        variables: Dictionary of variables.
        sources: Dictionary of sources.
    """

    includeInputSubdirs: Optional[bool] = None
    groupStatVarsByProperty: Optional[bool] = None
    inputFiles: Dict[str, InputFile]
    variables: Optional[Dict[str, Variable]] = None  # optional section
    sources: Dict[str, Source]

    # model configuration - to allow for extra fields and to populate by name
    # (for the "format" field) and forbid extra fields
    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    @model_validator(mode="after")
    def validate_input_file_keys_are_csv(self) -> "Config":
        """Validate that all input file keys are .csv files"""

        for key in self.inputFiles:
            if not key.lower().endswith(".csv"):
                raise ValueError(f'Input file key "{key}" must be a .csv file name')
        return self

    @model_validator(mode="after")
    def validate_provenance_in_sources(self) -> "Config":
        """Validate that all input file provenances are in the sources section"""

        known_provenances = set()
        for source in self.sources.values():
            known_provenances.update(source.provenances.keys())

        # Validate that each InputFile provenance is among them
        for file_key, input_file in self.inputFiles.items():
            if input_file.provenance not in known_provenances:
                raise ValueError(
                    f'Input file "{file_key}" references unknown provenance "{input_file.provenance}".'
                )

        return self

    def validate_config(self) -> None:
        """Validate the config"""
        Config.model_validate(self.model_dump())

    @classmethod
    def from_json(cls, file_path: str) -> "Config":
        """Read the config from a JSON file

        Args:
            file_path: Path to the JSON file.

        Returns:
            Config: The config object.
        """

        with open(file_path, "r") as f:
            data = f.read()
        return cls.model_validate_json(data)
