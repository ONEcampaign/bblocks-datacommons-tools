"""Module to work with Data Commons RSI"""

from dataclasses import dataclass, field
from pathlib import Path
import json


@dataclass
class InputFiles:
    """Representation of the InputFiles section of the config file

    Attributes:
        entityType: Type of the entity.
        provenance: Provenance of the data.
        observationProperties: Properties of the observation.
    """

    entityType: str
    provenance: str
    observationProperties: dict

@dataclass
class Variables:
    """Representation of the Variables section of the config file

    Attributes:
        name: Name of the variable.
        description: Description of the variable.
        searchDescriptions: List of search descriptions for the variable.
        group: Group to which the variable belongs.
        properties: Properties of the variable.
    """

    name: str
    description: str = field(default_factory=str)
    searchDescriptions: list = field(default_factory=list)
    group: str = "ONE/"
    properties: dict = field(default_factory=dict)


@dataclass
class Sources:
    """Representation of the Sources section of the config file"""
    url: str
    provenances: dict


@dataclass
class Config:
    """ """

    includeInputSubdirs: bool | None = None # Representation of the includeInputSubdirs section of the config file
    inputFiles: dict[str, InputFiles] = field(default_factory=dict)
    variables: dict[str, Variables] = field(default_factory=dict)
    sources: dict[str, Sources] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert the config object to a dictionary.
        """

        config_dict = {
            "inputFiles": {k: v.__dict__ for k, v in self.inputFiles.items()},
            "variables": {k: v.__dict__ for k, v in self.variables.items()},
            "sources": {k: v.__dict__ for k, v in self.sources.items()},
        }

        if self.includeInputSubdirs is not None:
            config_dict["includeInputSubdirs"] = self.includeInputSubdirs

        return config_dict

    @classmethod
    def from_json(cls, path: Path) -> "Config":
        """Create a Config object from a JSON file.

        Args:
            path: Path to the JSON file.

        Returns:
            Config object.
        """

        with open(path, "r") as f:
            data = json.load(f)

        config = Config()
        config.includeInputSubdirs = data.get("includeInputSubdirs")

        for key, value in data["inputFiles"].items():
            config.inputFiles[key] = InputFiles(**value)

        for key, value in data["variables"].items():
            config.variables[key] = Variables(**value)

        for key, value in data["sources"].items():
            config.sources[key] = Sources(**value)

        return config

    def to_json(self, path: Path) -> None:
        """Save the config object to a JSON file.

        Args:
            path: Path to save the JSON file.
        """

        config_dict = self.to_dict()

        with open(path, "w") as f:
            json.dump(config_dict, f, indent=4)

