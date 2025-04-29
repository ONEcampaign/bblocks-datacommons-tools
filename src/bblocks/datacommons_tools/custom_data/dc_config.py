"""Module to work with Data Commons DCConfigManager"""

from os import PathLike
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
from pydantic import HttpUrl

from bblocks.datacommons_tools.custom_data.models.config_file import Config
from bblocks.datacommons_tools.custom_data.models.data_files import (
    ObservationProperties,
    ImplicitSchemaFile,
    ColumnMappings,
    ExplicitSchemaFile,
)
from bblocks.datacommons_tools.custom_data.models.sources import Source
from bblocks.datacommons_tools.custom_data.models.stat_vars import Variable

DC_DOCS_URL = "https://docs.datacommons.org/custom_dc/custom_data.html"


class DCConfigManager:
    """Class to handle the config json and data for Data Commons

    This class facilitates creating, reading, editing and validating config jsons and data for Data Commons
    # TODO: add functionality to work with MCF files

    Usage:

    To start instantiate the object with or without an existing config json
    >>> config_manager = DCConfigManager()
    or
    >>> config_manager = DCConfigManager(config_file="path/to/config.json")

    To add a provenance to the config, use the add_provenance method
    >>> config_manager.add_provenance(
    >>>     provenance_name="Provenance Name",
    >>>     provenance_url="https://example.com/provenance",
    >>>     source_name="Source Name",
    >>>     source_url="https://example.com/source"
    >>> )

    This will add a provenance and a source in the config. If the already exists,
    you can add another provenance to the existing source
    >>> config_manager.add_provenance(
    >>>    provenance_name="Provenance Name",
    >>>    provenance_url="https://example.com/provenance",
    >>>    source_name="Source Name"
    >>> )

    To add a variable to the config (using the implicit schema), use the add_variable_to_implicit_schema method
    >>> config_manager.add_variable_to_config(
    >>>    "StatVar",
    >>>     name="Variable Name",
    >>>     description="Variable Description",
    >>>     group="Group Name"
    >>>    )

    To add an input file and data to the config, using the implicit (per column) schema,
    use the add_variablePerColumn_input_file method
    >>> config_manager.add_implicit_schema_file(
    >>>    file_name="input_file.csv",
    >>>    provenance="Provenance Name",
    >>>    data=df,
    >>>    entityType="Country",
    >>>    observationProperties={"unit": "USDollar"}
    >>>    )

    To add an input file and data to the config, using the explicit (per row) schema,
    use the add_variablePerRow_input_file method
    >>> config_manager.add_explicit_schema_file(
    >>>    file_name="input_file.csv",
    >>>    provenance="Provenance Name",
    >>>    data=df,
    >>>    columnMappings={"entity": "Country", "date": "Year", "value": "Value"}
    >>>    )

    It isn't a requirement to add the data at the same time as the input file. You can add the data
    later using the add_data method. This is useful when you want to edit the config file
    without needing the data. For example, for the variablePerColumn input file:
    >>> config_manager.add_implicit_schema_file(file_name="input_file.csv",provenance="Provenance Name")

    To add data to the config, you can use the add_input_file and override the information already
    registered, or you can use the add_data method.
    Note: To add data, the input file must already be registered in the config file
    >>> config_manager.add_data(<data>, "input_file.csv")

    To set the includeInputSubdirs and the groupStatVarsByProperty fields of the config, use
    the set_includeInputSubdirs and set_groupStatVarsByProperty methods
    >>> config_manager.set_includeInputSubdirs(True)
    >>> config_manager.set_groupStatVarsByProperty(True)

    Once you are ready to export the config and the data, use the exporter methods.
    Note that while the config is being edited (provenances, variables, input files being added)
    the config may not be valid. If any exporter method is called, the config will be
    validated and an error will be raised if the config is not valid.

    To export the config and the data, use the export_config_and_data method
    >>> config_manager.export_config("path/to/config")

    To export only the config, use the export_config method
    >>> config_manager.export_config("path/to/config")

    or get the config as a dictionary using the config_to_dict method
    >>> config_manager = config_manager.config_to_dict()

    To export only the data, use the export_data method
    >>> config_manager.export_data("path/to/data")
    """

    def __init__(self, config_file: Optional[str | PathLike[str]] = None):

        self._config = (
            Config.from_json(config_file)
            if config_file
            else Config(inputFiles={}, sources={})
        )
        self._data = {}

    def set_includeInputSubdirs(self, set_value: bool) -> "DCConfigManager":
        """Set the includeInputSubdirs attribute of the config"""
        self._config.includeInputSubdirs = set_value
        return self

    def set_groupStatVarsByProperty(self, set_value: bool) -> "DCConfigManager":
        """Set the groupStatVarsByProperty attribute of the config"""
        self._config.groupStatVarsByProperty = set_value
        return self

    def add_provenance(
        self,
        provenance_name: str,
        provenance_url: HttpUrl | str,
        source_name: str,
        source_url: Optional[HttpUrl | str] = None,
        override: bool = False,
    ) -> "DCConfigManager":
        """Add a provenance to the config

        Add a provenance (optionally with a new source) to the sources section of the config
        file. If the source does not exist, it will be added but a source URL must be provided.
        If the source exists, the provenance will be added to the existing source.
        If the provenance already exists, it will be overwritten if override is set to True.

        Args:
            provenance_name: Name of the provenance
            provenance_url: URL of the provenance
            source_name: Name of the source
            source_url: URL of the source (optional)
            override: If True, overwrite the existing provenance if it exists. Defaults to False.

        Raises:
            ValueError: If the source does not exist and no source URL is provided,
                or if the provenance already exists and override is not set to True.
        """

        # if the source does not exist, add it
        if source_name not in self._config.sources:
            # if the source URL is not provided, raise an error
            if source_url is None:
                raise ValueError(
                    f"Source '{source_name}' not found. "
                    "Please provide a source URL so the source can be added."
                )
            self._config.sources[source_name] = Source(
                url=source_url, provenances={provenance_name: provenance_url}
            )

        # if the source exists, add the provenance
        else:
            # check if the provenance already exists
            if provenance_name in self._config.sources[source_name].provenances:
                if not override:
                    raise ValueError(
                        f"Provenance '{provenance_name}' already exists for source '{source_name}'. "
                        "Use override=True to overwrite it."
                    )
            self._config.sources[source_name].provenances[provenance_name] = HttpUrl(
                provenance_url
            )

        return self

    def add_variable_to_config(
        self,
        statVar: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        searchDescriptions: Optional[List[str]] = None,
        group: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        override: bool = False,
    ) -> "DCConfigManager":
        """Add a variable to the config. This only applies to the implicit schema.

        This method registers a variable in the config. If there is no variables section
        defined in the config, it will create one.

        Args:
            statVar: The identifier of the statistical variable. Used as the key in the config.
            name: Name of the variable (Optional)
            description: Description of the variable (Optional)
            searchDescriptions: List of search descriptions (Optional)
            group: Name of the group (Optional)
            properties: Properties of the variable (Optional)
            override: If True, overwrite the existing variable if it exists.
                Defaults to False.
        """

        # check if the config has a variables section
        if self._config.variables is None:
            self._config.variables = {}

        # check if the variable already exists
        if statVar in self._config.variables:
            if not override:
                raise ValueError(
                    f"Variable '{statVar}' already exists. Use override=True to overwrite it."
                )

        self._config.variables[statVar] = Variable(
            name=name,
            description=description,
            searchDescriptions=searchDescriptions,
            group=group,
            properties=properties,
        )
        return self

    def _data_override_check(self, file_name: str, override: bool) -> None:
        """Check if the data already exists and override is not set"""
        if file_name in self._data:
            if not override:
                raise ValueError(
                    f"Data for file '{file_name}' already exists. "
                    "Use a different name or set override as `True`."
                )

    def add_implicit_schema_file(
        self,
        file_name: str,
        provenance: str,
        data: Optional[pd.DataFrame] = None,
        entityType: Optional[str] = None,
        observationProperties: Dict[str, str] = None,
        ignoreColumns: Optional[List[str]] = None,
        override: bool = False,
    ) -> "DCConfigManager":
        f"""Add an inputFile to the config and optionally register the data as pandas DataFrame.

        This method registers an input file in the config. Optionally it also registers the
        data that accompanies the input file registered. The registration of the data is made
        optional in cases where a user wants to edit the config file without the
        accompanying data. The data can be registered later using the add data method.

        This method is for the implicit schema approach (variable per column). Read more about
        implicit and explicit schemas here:
        {DC_DOCS_URL}#step-2-choose-between-implicit-and-explicit-schema-definition

        Args:
            file_name: Name of the file (should be a .csv file)
            provenance: Provenance of the data. This should be the name of the provenance
                in the sources section of the config file. Use add_provenance to add a provenance
                to the config file.
            data: Data to register (optional)
            entityType: Type of the entity (optional)
            observationProperties: Observation properties. Allowed keys
                are [unit, observationPeriod, scalingFactor, measurementMethod]
            ignoreColumns: List of columns to ignore (optional)
            override: If True, overwrite the existing file if it exists. Defaults to False.
        """

        # check if the file already exists
        self._data_override_check(file_name=file_name, override=override)

        # add the file to the config
        self._config.inputFiles[file_name] = ImplicitSchemaFile(
            entityType=entityType,
            ignoreColumns=ignoreColumns,
            provenance=provenance,
            observationProperties=ObservationProperties(**observationProperties),
        )

        # if data is provided, register it
        if data is not None:
            self._data[file_name] = data

        return self

    def add_explicit_schema_file(
        self,
        file_name: str,
        provenance: str,
        data: Optional[pd.DataFrame] = None,
        columnMappings: Dict[str, str] = None,
        ignoreColumns: Optional[List[str]] = None,
        override: bool = False,
    ):
        f"""Add an inputFile to the config and optionally register the data as pandas DataFrame.

        This method registers an input file in the config. Optionally it also registers the
        data that accompanies the input file registered. The registration of the data is made
        optional in cases where a user wants to edit the config file without the
        accompanying data. The data can be registered later using the add data method.

        This method is for the explicit schema approach (variable per row). Read more about
        implicit and explicit schemas here:
        {DC_DOCS_URL}#step-2-choose-between-implicit-and-explicit-schema-definition


        Args:
            file_name: Name of the file (should be a .csv file)
            provenance: Provenance of the data. This should be the name of the provenance
                in the sources section of the config file. Use add_provenance to add a provenance
                to the config file.
            data: Data to register (optional)
            columnMappings: Column mappings. Match the headings in the CSV file to the allowed
                properties. Allowed keys are [entity, date, value, unit,
                scalingFactor, measurementMethod, observationPeriod].
            ignoreColumns: List of columns to ignore (optional)
            override: If True, overwrite the existing file if it exists. Defaults to False.

        """

        # check if the file already exists
        self._data_override_check(file_name=file_name, override=override)

        # add the file to the config
        self._config.inputFiles[file_name] = ExplicitSchemaFile(
            ignoreColumns=ignoreColumns,
            provenance=provenance,
            columnMappings=ColumnMappings(**columnMappings),
        )

        # if data is provided, register it
        if data is not None:
            self._data[file_name] = data

        return self

    def add_data(
        self, data: pd.DataFrame, file_name: str, override: bool = False
    ) -> "DCConfigManager":
        """Add data to the config

        Args:
            data: Data to register
            file_name: Name of the file (should be a .csv file and have been
                registered in the config file)
            override: If True, overwrite the existing data if it exists.
        """

        # check if the file name has been registered in the config file
        if file_name not in self._config.inputFiles:
            raise ValueError(
                f"File '{file_name}' not found in the config file. Please register the "
                "file in the config file before adding data, using the add_input_file method."
            )

        # check if the file already exists and override is not set
        self._data_override_check(file_name=file_name, override=override)

        # add the data to the config
        self._data[file_name] = data
        return self

    def export_config(self, dir_path: str | PathLike[str]) -> None:
        """Export the config to a JSON file

        Before exporting, the config is validated to ensure that all required fields
        are present and that the config is valid.

        Args:
            dir_path: Path to the directory where the config will be exported.

        Raises:
            ValueError: If the config is not valid
        """

        # validate the config
        self._config.validate_config()

        # export the config to a JSON file
        output_path = Path(dir_path) / "config.json"
        with output_path.open("w") as f:
            f.write(self._config.model_dump_json(indent=4, exclude_none=True))

    def config_to_dict(self) -> Dict:
        """Export the config to a dictionary

        Before exporting, the config is validated to ensure that all required fields are
        present and that the config is valid.

        Returns:
            Dict: The config as a dictionary

        Raises:
            ValueError: If the config is not valid
        """

        # validate the config
        self._config.validate_config()

        # export the config to a dictionary
        return self._config.model_dump(mode="json", exclude_none=True)

    def export_data(self, dir_path: str | PathLike[str]) -> None:
        """Export the data to CSV files

        Args:
            dir_path: Path to the directory where the data will be exported.
        """

        # check if there is any data
        if not self._data:
            raise ValueError("No data to export")

        # export the data to CSV files
        for file, data in self._data.items():
            data.to_csv(Path(dir_path) / file, index=False)

    def export_config_and_data(self, dir_path: str | PathLike[str]) -> None:
        """Export the config and data to a directory

        Args:
            dir_path: Path to the directory where the config and data will be exported.
        """

        # export the config
        self.export_config(dir_path)

        # export the data
        self.export_data(dir_path)

    def validate_config(self) -> "DCConfigManager":
        """Validate the config

        This method checks the config for any issues and ensuring all the fields and values are valid. It rai
        an error if there are any issues with the config.

        Raises:
            pydantic.ValidationError if the config is not valid
        """

        # validate the config
        self._config.validate_config()
        return self

    def __repr__(self) -> str:
        input_files_count = len(self._config.inputFiles)
        sources_count = len(self._config.sources)
        variables_count = len(self._config.variables or {})
        dataframes_count = len(self._data)

        include_input_subdirs = self._config.includeInputSubdirs
        group_statvars = self._config.groupStatVarsByProperty

        return (
            f"<DCConfigManager config: "
            f"\n{input_files_count} inputFiles, with {dataframes_count} containing data"
            f"\n{sources_count} sources"
            f"\n{variables_count} variables"
            f"\nflags: includeInputSubdirs={include_input_subdirs}, "
            f"groupStatVarsByProperty={group_statvars}>"
        )

    # TODO: add_config method to add a config to the object either from json file, dictionary or another Config object and merge with existing config
