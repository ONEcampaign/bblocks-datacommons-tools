# Preparing data for Data Commons

`bblocks-datacommons-tools` offers convenient functionality to prepare configuration JSON, 
MCF, and custom data files without having to manually edit these files. The `CustomDataManager`
lets you create or edit an existing `config.json` file, work with MCF files, export data stored as pandas
DataFrames in correct CSV formats, and validate all the files ensuring they are correctly structured for
Data Commons.

## Creating a `CustomDataManager`

To start preparing your data, create a `CustomDataManager` instance.

```python
from bblocks.datacommons_tools import CustomDataManager

manager = CustomDataManager()
```

This will create a custom data manager without a blank `config.json` and no MCF files, which you can use if you are
building the files from scratch. If you already have a `config.json` or any MCF files you can specify them at
instantiation.

```python
manager = CustomDataManager(config_file="path/to/config.json", mcf_file="path/to/mcf_file.mcf")
```

You might store several config files in subdirectories. You can create a `CustomDataManager` instance specifying 
the directory containing the `config.json` files which will be merged into a single `config.json` file.

```python
manager = CustomDataManager.from_config_files_in_directory("path/to/directory")
```

## Managing provenances and sources

The `CustomDataManager` allows you to manage the sources and provenances in the `config.json` file.
You can add a provenance and source using the `add_provenance` methods.

```python
manager.add_provenance(provenance_name="Provenance Name",
                       provenance_url="https://example.com/provenance",
                       source_name="Source Name",
                       source_url="https://example.com/source"
                       )
```

If the source already exists in the `config.json`, it will not be added again. But if the source does not exist
yet, you should specify the source URL. If the provenance already exists, you can override it by setting the 
`overwrite` parameter to `True`.

Additional methods exist to manage sources and provenances:

- `remove_source` - removes a source and all its provenances from the `config.json`.
- `remove_provenance` - removes a provenance from the `config.json`.
- `rename_source` - renames a source in the `config.json`.
- `rename_provenance` - renames a provenance in the `config.json`.


## Managing variables

The `CustomDataManager` lets you easily add variables to the `config.json` or MCF files based on whether you are
using the implicit or explicit schema. Read more about implicit and explicit schemas
[here](dc-schemas.md).


If you are using the implicit schema, you can specify details about the variables in the `config.json` file in the
`variables` section. Adding a `variable` section to the `config.json` is optional, but recommended to specify 
variable names and associate properties with the variables. For each variable you can specify:
- `statVar`: The statistical variable ID
- `name`: A human-friendly readable name 
- `description`: A more detailed name or description
- `searchDescriptions`: A comma-separated list of natural-language text descriptions
of the variable used to generate embeddings for the NL query interface
- `group`: The group the variable belongs to
- `properties`: Any properties associated with the variable for example `Gender` - `Male`, `Female`, `Other` etc.

```python
manager.add_variable_to_config(statVar="ghed_che",
                               name="Current health expenditure",
                               description="The total expenditure on health from domestic and foreign sources",
                               group="Health",
                               searchDescriptions=["Total health spending", "Health financing"],
                               properties={"measurementMethod": "WHOEstimate"}
                               )
```

If the variable already exists, you can overwrite it by setting the `override` parameter to `True`.

[//]: # (<--- TODO: Explicit schema adding variables --->)


You can rename variables using the `rename_variable` method, optionally specifying the MCF file
if you are using the explicit schema.


```python
manager.rename_variable("ghed_che", "ghed_current_health_expenditure",
                        mcf_file="path/to/mcf_file.mcf")
```

You can also remove variables from the `config.json` or MCF files using the `remove_variable` method.

```python
manager.remove_variable("ghed_che", mcf_file="path/to/mcf_file.mcf")
```

## Managing the data files

The `CustomDataManager` allows you to manage the data files you want to import into Data Commons. 
**Note:** This doesn't mean formatting or transforming the data, but rather managing the files that will be used
to import the data into Data Commons and registering them according to the schema you are using. 

The `CustomDataManager` provides support for working with pandas DataFrames, allowing you to register
data stored in a DataFrame to the manager which will export the data to a CSV file in the correct format
for Data Commons.

To register a data file using the implicit schema, use the `add_implicit_schema_file` method:

```python
import pandas as pd

df = pd.DataFrame({
    "Country": ["USA", "Canada", "Mexico"],
    "Year": [2020, 2020, 2020],
    "gdp": [21000000, 1700000, 1200000]
})

manager.add_implicit_schema_file(file_name="gdp.csv", 
                                 provenance="IMF", 
                                 data=df,
                                 entityType="Country",
                                 observationProperties={"unit": "USDollar"}
                                 )
```

Adding data when registering a data file is optional. You can also add the data later using the `add_data` method:

```python
manager.add_data(data=df, file_name="gdp.csv")
```

[//]: # (<--- TODO: add explicit schema data file registration here --->)

## Removing variables and data files

You have already seen above how to remove variables and data files using the `remove_variable` 
and `remove_data_file` methods.
You can also remove variables and files based on their associated provenance or source.

```python title="Remove variables by provenance"
manager.remove_by_provenance("World Economic Outlook")
```

```python title="Remove data files by source"
manager.remove_by_source("IMF")
```



## Adding and merging config files

[//]: # (<---TODO: adding and merging config files here --->)





## Additional settings

There are some additional settings you can configure in the `CustomDataManager`:

```python
manager.set_includeInputSubdirs(True)  # Include input subdirectories in the config.json
```

This method will add the `includeInputSubdirs` field to the `config.json` file, which allows you
to place data files in subdirectories inside the main output directory. This is useful for organizing your data files
and keeping them structured. By default, this is not set and the Data Commons importer will expect all data files
to be in the main output directory. If this is set to `True`, You can specify the subdirectory when adding a data file:

```python
manager.add_implicit_schema_file(file_name="economics/gdp.csv", 
                                 ...  # other parameters as before
                                 )
```

You can also set the `groupStatVarsByProperty` field in the `config.json` file, 
which allows you to group statistical variables by their properties. 

```python
manager.set_groupStatVarsByProperty(True)
```



## Validating and exporting files

To ensure that your `config.json` is correctly strucured, you can validate it using the `validate_config` method:

```python
manager.validate_config()
```

The config is automatically validated when you export it. You can export the `config.json` file to disk
or get the config as a dictionary.

```python title="Export config to disk"
manager.export_config("path/to/output/config.json")
```

```python title="Get config as dictionary"
config_dict = manager.config_to_dict()
```

Additionally, you can export the data or MCF files to disk.

```python title="Export data files"
manager.export_data("path/to/output/folder")
```

```python title="Export MCF files"
manager.export_mfc_file("path/to/output/folder")
```

To export all files at once, you can use the `export_all` method:

```python
manager.export_all("path/to/output/folder")
```