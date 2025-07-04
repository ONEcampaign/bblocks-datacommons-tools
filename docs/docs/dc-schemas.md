# Data Commons Schemas

You can choose to structure your data for Data Commons using either an **implicit** or **explicit** schema, each
serving different needs and levels of complexity.

## Implicit schema

The implicit schema is the simpler way of importing data into Data Commons. It
does not require that you write MCF files, but it is more constraining on the structure of your data. 
You don't need to specify details about the variables or specify entities in `DCID` format (they will be
automatically resolved). Naming conventions are loose, and the Data Commons importer will automatically generate `DCIDs`
for your variables based on the column names in your CSV files if they are not specified in the `config.json`.

Using the implicit schema is convenient for simple and small datasets when loading the data quickly is more important 
than having a fully detailed schema.

### Example implicit schema workflow

Here is a very simple example of a workflow using the implicit schema.

Let's say we have some data on GDP for different countries that we want to import into Data Commons. We don't 
have any other data in the knowledge graph (so we are starting from scratch). The data comes from the IMF World
Economic Outlook, and we have already done the hard work of cleaning and formatting it as a pandas DataFrame:

```python
df = pd.DataFrame({
    "Country": ["USA", "Canada", "Mexico"],
    "Year": [2020, 2020, 2020],
    "gdp": [21000000, 1700000, 1200000]
})
```

This dataframe is structured for the implicit schema where there is a variable per columns - in this
case `gdp` - and the other columns are used to specify the entity "Country" and the time period "Year". The entities
and the time period don't need to be `DCIDs` or follow any convention, but the columns must always be specified in the
sequence:

ENTITY, OBSERVATION_DATE, STATISTICAL_VARIABLE1, STATISTICAL_VARIABLE2, ...

Now let's use the `CustomDataManager` to prepare the data for import into Data Commons:

First we need to create a `CustomDataManager` instance:

```python
from bblocks.datacommons_tools import CustomDataManager

manager = CustomDataManager()
```

This creates a new `CustomDataManager` instance with a blank `config.json` file.

We are forward looking data engineers, so we know that our data will grow over time. So we might want to organise the
files in subdirectories. Let's allow subdirectories in the `config.json` file:

```python
manager.set_includeInputSubdirs(True)
```

Next, let's add the source and the provenance of the data to the `config.json` file.

```python
manager.add_provenance(provenance_name="World Economic Outlook (WEO)",
                       provenance_url="https://www.imf.org/en/Publications/WEO",
                       source_name="International Monetary Fund (IMF)",
                       source_url="https://www.imf.org/en/Home"
                       )
```

Now let's register the variable `gdp` in the `config.json` file. This is optional but recommended, as it allows you to
add details about the variable. If this is not done, the Data Commons importer will automatically generate a 
`DCID` for the variable based on the data file.

```python
manager.add_variable_to_config(statVar="gdp",
                               name="Gross Domestic Product",
                               description="Gross Domestic Product (GDP) is the total value of all goods and services produced in a country in a given year.",
                               group="Economy",
                               )
```

Now we can add the data and the data file to the manager object. This will update the `config.json` file with the
data file information and keep track of the data in the `CustomDataManager` instance.

```python
manager.add_implicit_schema_file(file_name="ecomomy/gdp.csv",  # store the data in the gdp file in the ecomomy subdirectory
                                 provenance="World Economic Outlook (WEO)", 
                                 data=df, # pandas DataFrame with the GDP data
                                 entityType="Country",
                                 observationProperties={"unit": "USDollar"} # specify the unit of the variable
                                 )
```

We have added the provenance and source, registered the variable, and added the data file with the GDP data. Now
we are ready to export the files and import them into Data Commons.

```python
manager.export_all("output_directory")
```

The `config.json` and the data file as a CSV file in the `economy` subdirectory will be created in 
the `output_directory`.

Now you are ready to import your implicit schema data into Data Commons! Next, read about the explicit schema
or jump to [loading the data into Data Commons](./loading-data.md).

# Explicit schema

[//]: # (<--- TODO: Add about explicit schema here --->)

### Example explicit schema workflow

[//]: # (<--- TODO: add explicit workflow --->)