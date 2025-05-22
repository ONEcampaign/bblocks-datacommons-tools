# Contributor Guide

## Dev Environment Tips
- This repo is for a package which is part of the `bblocks` namespace. The package would
be accessed as import bblocks.datacommons_tools
- Code should be formatted with black
- Poetry is used to manage dependencies

## Linting instructions
- Format all code with black
- Follow Google Python style but align with the code style and preferences already established for this repo.
- This repo uses Google-style docstrings. All functions and methods should have docstrings.

## Testing Instructions
- The tests are inside the tests folder. From root you would therefore call `poetry run pytest ./tests`
- Fix any test or type errors until the whole suite is green.
- Add or update tests for the code you change, even if nobody asked.