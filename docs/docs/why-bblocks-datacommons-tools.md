# Why bblocks-datacommons-tools

Custom [Data Commons](https://docs.datacommons.org/custom_dc/custom_data.html) requires that you provide your data in a specific schema, format, and file structure.

At a high level, you need to provide the following:

- All observations data must be in CSV format, using a predefined schema.
- You must also provide a JSON configuration file, named `config.json`, that specifies how to map and resolve the CSV contents to the Data Commons schema knowledge graph.
- Depending on how you define your statistical variables (metrics), you may need to provide MCF (Meta Content Framework) files.
- You may also need to define new custom entities.

The official [Data Commons](https://docs.datacommons.org/custom_dc/custom_data.html) documentation provides a great overview of the process. The explanations, examples, and suggested process works well for small custom Knowledge Graphs.

However, managing this workflow by hand is tedious and easy to get wrong. It also doesn't scale particularly well - manually managing and validating the contents of `config.json` files with thousands or variables is not feasible.

The `bblocks.datacommons_tools` package streamlines that process. It provides a Python API and command line utilities for building config files, generating MCF from CSV metadata and running the data load pipeline on Google Cloud. 

## Managing `config.json` files

Successfully loading data to a custom Data Commons requires a valid `config.json` file. While the [Data Commons](https://docs.datacommons.org/custom_dc/custom_data.html) docs explain the structure and provides examples, without `bblocks-datacommons-tools` users have no way to programmatically add to manage `config.json` files, or to validate their contents.

`bblocks-datacommons-tools` provides a `CustomDataManager` which deals with creating, updating, structuring, and validating `config.json` files - respecting the official Data Commons schema and requirements.

## Creating `MCF` files
MCF (Meta Content Framework) files provide a lot of flexibility for modelling variables, statistical variable groups, topics, and entities. 

They are fully supported by the Custom Data Commons data loading process, but they must be generated and validated by hand.

`bblocks-datacommons-tools` provides tools (e.g `csv_metadata_to_mfc_file`, `add_variable_to_mcf`...) to create or update `MCF` files using spreadsheets or with python code.

## Managing data
All observations data must be in CSV format, using a predefined schema which is described in the [Data Commons](https://docs.datacommons.org/custom_dc/custom_data.html) docs.

However, users must manually add CSV files to the right places, validate their contents, and list them on the `cofing.json` file. They must also be careful to respect the different requirements for the Implicit and Explicit schemas.

`bblocks-datacommons-tools` provides tools via its `CustomDataManager`, which make it possible to automate this part of the workflow, and validate that the CSV contents and `confi.json` file are aligned.

## Uploading data to the Knowledge Graph
The process for uploading the data to the Knowledge Graph described in the [Data Commons](https://docs.datacommons.org/custom_dc/custom_data.html) docs requires manual steps. This means that it is impossible to develop fully automated pipelines without also developing custom scripts to upload data to the Google Cloud Storage bucket, trigger the data load job, and redeploy the Data Commons instance.

`bblocks-datacommons-tools` provides tools (`upload_to_cloud_storage`,
`run_data_load`, `redeploy_service`) which make it possible to remove manual steps when developing data pipelines.