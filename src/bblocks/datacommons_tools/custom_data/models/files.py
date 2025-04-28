from typing import Optional, List, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


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
        data_format: Format of the data (variable per column or variable per row).
            Accepted values are "variablePerColumn" or "variablePerRow". If using explicit
            schema, this should be "variablePerRow". If using implicit schema, this should
            be "variablePerColumn" or omitted. This attribute is represented as
            "format" in the JSON.
        columnMappings:  If headings in the CSV file does not use the default names,
             the equivalent names for each column. (explicit schema only).
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
                "Cannot use both implicit and explicit schema fields. Read more about "
                "implicit and explicit schemas here: https://docs.datacommons.org/"
                "custom_dc/custom_data.html#step-2-choose-between-implicit-and-"
                "explicit-schema-definition"
            )

        return self
