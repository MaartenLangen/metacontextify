# Metacontextify

A Python package for retrieving environmental context and properties for marine sequences from MGnify and ENA.

## Overview

Metacontextify provides a comprehensive pipeline for enriching sequence data with environmental metadata from multiple sources:

- **MGnify**: Marine metagenomics and genomics sequences and assemblies
- **ENA**: European Nucleotide Archive sample metadata

## Features

Retrieve environmental properties for:

- ENA Sample IDs, MGnify Protein, Genome, Assembly, and Sample IDs
- a JSON file with the hits in a MGnify protein similarity search
- a csv with columns lat, lon, sample_date, depth

These environmental properties are temperature, salinity, pH and concentrations of nitrate, oxygen, phosphate and phytoplankton.

The tool provides a command-line interface and can be imported as Python module.

## Installation

### From source

```bash
git clone https://github.com/MaartenLangen/metacontextify.git
cd metacontextify
pip install -e .
```

### With development dependencies

```bash
pip install -e ".[dev]"
```

## Quick Start with CLI

### Setting up Copernicus capabilities

In order to retrieve the environmental properties from the Copernicus Marine Services, credentials are needed. A guide on how to create this for free can be found [here](https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service). Once you have your credentials, you can save them for the CLI with the following command:

```bash
metacontextify login user123 pswrd123
```

### Processing IDs

A txt-file with one ID per line can be parsed with the following code:

```bash
metacontextify id-file input.txt protein output.csv
```
This is the command for MGnify Protein IDs. A list of other supported identifiers and other optional parameters can be listed with 

```bash
metacontextify id-file --help
```

### Processing MGnify similarity search results

The MGnify Protein website supports hmm-based protein similarity search. The results can be downloaded as a JSON file. Metacontextify supports the retrieval of environmental properties directly for this JSON file with the following command:

```bash 
metacontextify simsearch input.json results.csv
```

An overview of additional optional parameters can be obtained by running 

```bash
metacontextify simsearch --help
```

### Processing a collection of locations and dates

In order to make the code broadly applicable, it has the functionality to retrieve environmental properties for a collection of latitudes, longitudes, sample dates and depths. The tool can then be executed as follows:

```bash
metacontextify location-file input.csv output.csv
```

The input csv should have at least the columns `lat, lon, sample_date, depth` (order is not important). Additional columns in the input will be copied to the output (e.g. to keep the identifier together with each entry for subsequent processing steps). Additional optional parameters can be listed by running

```bash
metacontextify location-file --help
```

## Quick Start with Python module

### Setting up Copernicus capabilities

In order to retrieve the environmental properties from the Copernicus Marine Services, credentials are needed. A guide on how to create this for free can be found [here](https://help.marine.copernicus.eu/en/articles/4220332-how-to-sign-up-for-copernicus-marine-service). Once you have your credentials, you can save them for the Python module with the following code:

```python
from metacontextify.data_retrievers.cmems import login

login('user123', 'pswrd123')
```

### Processing IDs

An iterable with IDs can be parsed with Metacontextify. For example, MGnify Protein identifiers can be parsed as follows:

```python
from metacontextify.pipelines import get_properties_for_mgnify_proteins

results_df = get_properties_for_mgnify_proteins(
  protein_ids
)
```
This is the command for MGnify Protein IDs, where `protein_ids` is an iterable with MGnify Protein identifiers. A list of other supported identifiers:

- MGnify Genome: `get_properties_for_mgnify_genomes`
- MGnify Assembly: `get_properties_for_mgnify_assemblies`
- MGnify Sample: `get_properties_for_mgnify_samples`
- ENA Sample: `get_properties_for_ena_samples`

### Processing MGnify similarity search results

The MGnify Protein website supports hmm-based protein similarity search. The results can be downloaded as a JSON file. Metacontextify supports the retrieval of environmental properties directly for this JSON file with the following code:

```python
from metacontextify.pipelines import get_properties_for_mgnify_search_result

results_df = get_properties_for_mgnify_search_results(
  'path/to/json.json',
  nb_hits = 1000
)
```

By using the optional argument `nb_hits`, only the first `n` hits are read. Omitting this argument retrieves properties for all hits. 

### Processing a collection of locations and dates

In order to make the code broadly applicable, it has the functionality to retrieve environmental properties for a collection of latitudes, longitudes, sample dates and depths. This can be done with the following code:

```python
from metacontextify.data_retrievers.cmems import get_properties

results_df = get_properties(
  input_df
)
```

The input dataframe should have at least the columns `lat, lon, sample_date, depth` (order is not important). Additional columns in the input will be copied to the output (e.g. to keep the identifier together with each entry for subsequent processing steps).

## Module Overview

### `pipelines.py`

High-level functions for complete processing workflows:

- `get_properties_for_mgnify_search_results()`: Process MGnify similarity search JSON files
- `get_properties_for_mgnify_proteins()`: Map MGnify protein IDs to environmental data
- `get_properties_for_mgnify_genomes()`: Map MGnify genome IDs to environmental data
- `get_properties_for_mgnify_assemblies()`: Map MGnify assembly IDs to environmental data
- `get_properties_for_mgnify_samples()`: Map MGnify sample IDs to environmental data
- `get_properties_for_ena_samples()`: Map ENA sample IDs to environmental data
- `get_properties_for_id_file()`: Process text files with IDs
- `get_properties_for_locations_file()`: Process CSV files with lat/lon/date/depth

### `utils.parsers`

Input file parsing and data transformation:

- `read_mgnify_similarity_search_json()`: Parse MGnify similarity search JSON results
- `read_id_file()`: Read ID lists from text files
- `parse_dates()`: Parse and standardize date strings

### `data_retrievers.mgnify`

MGnify API interactions:

- `protein_to_assembly_from_file()`: Map proteins to assemblies using local file
- `protein_to_assembly_from_website()`: Map proteins to assemblies via MGnify website
- `assembly_to_sample()`: Map assembly IDs to sample IDs
- `genome_to_sample()`: Map genome IDs to sample IDs
- `get_mgnify_sample_metadata()`: Retrieve sample metadata from MGnify API

### `data_retrievers.ena`

ENA API interactions:

- `get_ena_sample_metadata()`: Retrieve sample metadata from ENA API

### `data_retrievers.cmems`

CMEMS (Copernicus Marine Service) API interactions:

- `login()`: Authenticate with CMEMS and save credentials
- `get_properties()`: Retrieve all environmental properties for locations/dates
- `get_phys()`: Retrieve physical properties (temperature, salinity)
- `get_chem()`: Retrieve biochemical properties (pH, nitrate, oxygen, phosphate, phytoplankton)

### `utils.http`

HTTP utilities with retry logic:

- `http_get()`: Basic HTTP GET wrapper with error handling
- `retry_request()`: HTTP requests with exponential backoff and retry logic
- `validate_json()`: Validate and parse JSON responses
- `handle_http_error()`: Centralized HTTP error logging and handling

### `utils.logging`

Logging configuration:

- `configure_logging()`: Set up logging configuration with custom levels and formats
- `get_logger()`: Get a configured logger instance for a module

## Development

### Running Tests

```bash
pytest
```

### Code Quality

```bash
# Format code
black .

# Sort imports
isort .

# Lint code
flake8 .

# Type checking
mypy metacontextify
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Citation

If you use Metacontextify in your research, please cite:

```bibtex
@software{metacontextify2026,
  title={Metacontextify: Automated environmental annotation of marine metagenomic samples},
  author={Maarten Langen, Vera van Noort},
  year={2026},
  url={https://github.com/MaartenLangen/metacontextify.git}
}
```
