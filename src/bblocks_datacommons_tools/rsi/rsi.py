"""Module to work with Data Commons RSI"""

from typing import Optional, Dict, List, Literal
from pydantic import BaseModel, Field, HttpUrl, model_validator
import pandas as pd


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
    data_format: Optional[Literal["variablePerColumn", "variablePerRow"]] = Field(default=None, alias="format") # represent the "format" field. Get around the protected name issue
    columnMappings: Optional[ColumnMappings] = None
    observationProperties: Optional[ObservationProperties] = None

    @model_validator(mode="after")
    def validate_schema_choice(self) -> "InputFile":
        """Validate that only one of implicit or explicit schema is used"""

        using_implicit = self.entityType is not None or self.observationProperties is not None
        using_explicit = self.columnMappings is not None or self.data_format == "variablePerRow"
        print(f"using_implicit: {using_implicit}, using_explicit: {using_explicit}")

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


class Source(BaseModel):
    """Representation of the Sources section of the config file

    Attributes:
        url: URL of the source.
        provenances: Dictionary of provenances. Each provenance name maps to a URL.
    """

    url: HttpUrl
    provenances: Dict[str, HttpUrl]  # Each provenance name maps to a URL


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
    variables: Optional[Dict[str, Variable]] = None # optional section
    sources: Dict[str, Source]

    # model configuration - to allow for extra fields and to populate by name (for the "format" field) and forbid extra fields
    model_config = {
        "populate_by_name": True,
        "extra": "forbid",
    }

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



class RSI:
    """ """

    def __init__(self, config_file: Optional[str] = None):
        # TODO replace with a path to config file
        # TODO: allow passing multiple paths and merging them together into one config object

        self.config = Config.from_json(config_file) if config_file else Config(
            inputFiles={},
            sources={},
        )
        self.data = {}

    def add_provenance(self, provenance_name: str, provenance_url: HttpUrl, source_name: str, source_url: Optional[HttpUrl], override: bool = False) -> None:
        """Add a provenance to the config

        Add a provenance (optionally with a new source) to the sources section of the config file. If the source does not exist, it will be added
        but a source URL must be provided. If the source exists, the provenance will be added to the existing source. If the provenance already exists, it will be overwritten if override is set to True.

        Args:
            provenance_name: Name of the provenance
            provenance_url: URL of the provenance
            source_name: Name of the source
            source_url: URL of the source (optional)
            override: If True, overwrite the existing provenance if it exists. Defaults to False.

        Raises:
            ValueError: If the source does not exist and no source URL is provided, or if the provenance already exists and override is not set to True.
        """

        # if the source does not exist, add it
        if source_name not in self.config.sources:
            # if the source URL is not provided, raise an error
            if source_url is None:
                raise ValueError(f"Source '{source_name}' not found. Please provide a source URL so the source can be added.")
            self.config.sources[source_name] = Source(url=source_url, provenances={provenance_name: provenance_url})

        # if the source exists, add the provenance
        else:
            # check if the provenance already exists
            if provenance_name in self.config.sources[source_name].provenances:
                if not override:
                    raise ValueError(f"Provenance '{provenance_name}' already exists for source '{source_name}'. Use override=True to overwrite it.")
                else:
                    self.config.sources[source_name].provenances[provenance_name] = provenance_url
            # if the provenance does not exist,add it
            else:
                self.config.sources[source_name].provenances[provenance_name] = provenance_url

    def add_variable(self, statVar: str,
                     name: Optional[str] = None,
                     description: Optional[str] = None,
                     searchDescriptions: Optional[List[str]] = None,
                     group: Optional[str] = None,
                     properties: Optional[Dict[str, str]] = None,
                     override: bool = False,
                     ) -> None:
        """Add a variable to the config"""

        # check if the config has a variables section
        if self.config.variables is None:
            self.config.variables = {}

        # check if the variable already exists
        if statVar in self.config.variables:
            if not override:
                raise ValueError(f"Variable '{statVar}' already exists. Use override=True to overwrite it.")

        self.config.variables[statVar] = Variable(name=name, description=description, searchDescriptions=searchDescriptions, group=group, properties=properties)

    def add_data(self,
                 file_name: str,
                 provenance: str,
                 data: Optional[pd.DataFrame] = None,
                 entityType: Optional[str] = None,
                 ignoreColumns: Optional[List[str]] = None,
                    data_format: Optional[Literal["variablePerColumn", "variablePerRow"]] = None,
                    columnMappings: Optional[ColumnMappings] = None,
                    observationProperties: Optional[ObservationProperties] = None,
                 override: bool = False
                 ) -> None:
        """Add and inputFile to the config and optionally register the data as pandas dataframe"""

        # check if the file already exists
        if file_name in self.config.inputFiles:
            if not override:
                raise ValueError(f"File '{file_name}' already exists. Use override=True to overwrite it.")

        # add the file to the config
        self.config.inputFiles[file_name] = InputFile(
            entityType=entityType,
            ignoreColumns=ignoreColumns,
            provenance=provenance,
            format=data_format,
            columnMappings=columnMappings,
            observationProperties=observationProperties,
        )

        # if data is provided, register it
        if data is not None:
            self.data[file_name] = data

    def export_config(self, dir_path: str) -> None:
        """Export the config to a JSON file"""

        # validate the config
        self.config.validate_config()

        # export the config to a JSON file
        with open(dir_path + "/config.json", "w") as f:
            f.write(self.config.model_dump_json(indent=4, exclude_none=True))

    def export_data(self, dir_path: str) -> None:
        """ """

        # check if there is any data
        if not self.data:
            raise ValueError("No data to export")

        # export the data to CSV files
        for file, data in self.data.items():
            data.to_csv(dir_path + "/" + file, index=False)









