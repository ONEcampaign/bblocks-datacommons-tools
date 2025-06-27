# bblocks-datacommons-tools

__Manage and load data to custom Data Commons instances__

[![PyPI](https://img.shields.io/pypi/v/bblocks_datacommons_tools.svg)](https://pypi.org/project/bblocks_datacommons_tools/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/bblocks_datacommons_tools.svg)](https://pypi.org/project/bblocks_places/)
[![Docs](https://img.shields.io/badge/docs-bblocks-blue)](https://docs.one.org/tools/bblocks/datacommons_tools/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![codecov](https://codecov.io/gh/ONEcampaign/bblocks-datacommons-tools/graph/badge.svg?token=3ONEA8JQTC)](https://codecov.io/gh/ONEcampaign/bblocks-datacommons-tools)

Custom [Data Commons](https://docs.datacommons.org/custom_dc/custom_data.html) requires that you provide your data in a specific schema, format, and file structure.

At a high level, you need to provide the following:

- All observations data must be in CSV format, using a predefined schema.
- You must also provide a JSON configuration file, named `config.json`, that specifies how to map and resolve the CSV contents to the Data Commons schema knowledge graph.
- Depending on how you define your statistical variables (metrics), you may need to provide MCF (Meta Content Framework) files.
- You may also need to define new custom entities.

Managing this workflow by hand is tedious and easy to get wrong.

The `bblocks.datacommons_tools` package streamlines that process. It provides a Python API and command line utilities for building config files, generating MCF from CSV metadata and running the data load pipeline on Google Cloud. 

Use this package when you want to:

- Manage `config.json` files programmatically.
- Define statistical variables, entities or groups using MCF files.
- Programmatically upload CSVs, MCF files, and the `config.json` file to Cloud Storage, trigger the load job and redeploy your custom Data Commons service with code.

In short, `datacommons-tools` removes much of the manual work involved in setting up and maintaining a custom Data Commons Knowledge Graph.

`bblocks-datacommons-tools` is part of the `bblocks` ecosystem, 
a set of Python packages designed as building blocks for working with data in the international development 
and humanitarian sectors.