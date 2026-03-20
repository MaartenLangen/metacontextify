from pathlib import Path
from typing import Annotated, Literal

import pandas as pd
import typer

from . import pipelines
from .utils.logging import configure_logging
from .data_retrievers.cmems import login

app = typer.Typer()


@app.callback()
def main(
    log_level: Annotated[
        str,
        typer.Option("--log-level", help="Logging level (e.g., INFO, DEBUG)")
    ] = "INFO",
) -> None:
    """Configure logging for CLI runs."""
    configure_logging(log_level)

@app.command(name="login")
def cmems_login(
        username: str,
        password: str
) -> None:
    """Login to CMEMS with copernicus account."""
    login(username, password)


@app.command()
def id_file(
    input: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            readable=True,
            resolve_path=True,
            help="Path to input file containing one ID per line"
        )
    ],
    id_type: Annotated[
        Literal["protein", "genome", "assembly", "sample", "ena_sample"],
        typer.Argument(help="Type of IDs in the input file (protein/genome/assembly/sample/ena_sample)")
    ],
    output: Annotated[
        Path,
        typer.Argument(
            file_okay=True,
            writable=True,
            resolve_path=True,
            help="Path to output CSV file for environmental properties"
        )
    ],
) -> None:
    """Retrieve environmental properties for a list of MGnify or ENA IDs."""
    result_df = pipelines.get_properties_for_id_file(
        input,
        id_type,
    )
    result_df.to_csv(output, index=False)



@app.command()
def simsearch(
    input: Annotated[
        Path,
        typer.Argument(help="Path to MGnify similarity search results JSON file")
    ], 
    output: Annotated[
        Path,
        typer.Argument(help="Path to output CSV file for environmental properties")
    ],
    nb_hits: Annotated[
        int,
        typer.Option(
            help="Retrieve data for top nb_hits. Set to -1 to retrieve for all hits in json."
        )
    ] = -1,
) -> None:
    """Retrieve environmental properties for MGnify similarity search results."""
    result_df = pipelines.get_properties_for_mgnify_search_results(
        input,
        nb_hits,
    )
    result_df.to_csv(output, index=False)

@app.command()
def location_file(
    input: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            readable=True,
            resolve_path=True,
            help="Path to input csv file containing columns lat,lon,sample_date,depth"
        )
    ],
    output: Annotated[
        Path,
        typer.Argument(
            file_okay=True,
            writable=True,
            resolve_path=True,
            help="Path to output CSV file for environmental properties"
        )
    ],
) -> None:
    """Retrieve environmental properties for a csv file with locations."""
    result_df = pipelines.get_properties_for_locations_file(input)
    result_df.to_csv(output, index=False)

if __name__ == "__main__":
    app()
