from pathlib import Path
from typing import Annotated, Literal

import pandas as pd
import typer

from . import pipelines
from .utils.logging import configure_logging, get_logger
from .utils.parsers import parse_cli_properties
from .data_retrievers.cmems import login

app = typer.Typer()
logger = get_logger("__main__")


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
    try:
        login(username, password)
    except ValueError as e:
        raise typer.BadParameter("The provided combination of username and password did not work.")



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
        Literal["protein", "genome", "assembly", "sample", "ena_sample", "ena_run"],
        typer.Argument(help="Type of IDs in the input file (protein/genome/assembly/sample/ena_sample/ena_run)")
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
    properties: Annotated[
        str,
        typer.Option(
            help="A comma seperated list of the properties for which Copernicus values will be " + \
                "retrieved. If omitted, all values will be retrieved. Possible values: " + \
                "temperature,salinity,nitrate,oxygen,ph,phosphate,phytoplankton"
        )
    ] = 'temperature,salinity,nitrate,oxygen,ph,phosphate,phytoplankton'
) -> None:
    """Retrieve environmental properties for a list of MGnify or ENA IDs."""
    try:
        parsed_properties = parse_cli_properties(properties)
    except ValueError as e:
        raise typer.BadParameter(e)
    result_df, incomplete = pipelines.get_properties_for_id_file(
        input,
        id_type,
        parsed_properties
    )
    result_df.rename(columns={
        'temperature_copernicus': 'temperature_copernicus [C]',
        'salinity': 'salinity [psu]',
        'phosphate': 'phosphate [mmol.m-3]',
        'nitrate': 'nitrate [mmol.m-3]',
        'oxygen': 'oxygen [mmol.m-3]',
        'phytoplankton': 'phytoplankton [mmol.m-3]',
        'ph_copernicus': 'ph_copernicus [1]'
    }, inplace=True)
    result_df.to_csv(output, index=False)

    if (
        isinstance(incomplete, pd.DataFrame)
        and 
        (incomplete.shape[0] > 0)
    ):
        incomplete.to_csv(
            output.with_name(f"{output.stem}.incomplete{output.suffix}"),
            index=False
        )
    elif (
        isinstance(incomplete, list)
        and
        (len(incomplete) > 0)
    ):
        incomplete_output = output.with_name(f"{output.stem}.incomplete.txt")
        with open(incomplete_output, 'w') as f:
            for id in incomplete:
                f.write(f"{id}\n")



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
    properties: Annotated[
        str,
        typer.Option(
            help="A comma seperated list of the properties for which Copernicus values will be " + \
                "retrieved. If omitted, all values will be retrieved. Possible values: " + \
                "temperature,salinity,nitrate,oxygen,ph,phosphate,phytoplankton"
        )
    ] = 'temperature,salinity,nitrate,oxygen,ph,phosphate,phytoplankton',
    nb_hits: Annotated[
        int,
        typer.Option(
            help="Retrieve data for top nb_hits. Set to -1 to retrieve for all hits in json."
        )
    ] = -1,
) -> None:
    """Retrieve environmental properties for MGnify similarity search results."""
    try:
        parsed_properties = parse_cli_properties(properties)
    except ValueError as e:
        raise typer.BadParameter(e)
    result_df, incomplete_df = pipelines.get_properties_for_mgnify_search_results(
        input,
        parsed_properties,
        nb_hits,
    )
    result_df.rename(columns={
        'temperature_copernicus': 'temperature_copernicus [C]',
        'salinity': 'salinity [psu]',
        'phosphate': 'phosphate [mmol.m-3]',
        'nitrate': 'nitrate [mmol.m-3]',
        'oxygen': 'oxygen [mmol.m-3]',
        'phytoplankton': 'phytoplankton [mmol.m-3]',
        'ph_copernicus': 'ph_copernicus [1]'
    }, inplace=True)
    result_df.to_csv(output, index=False)
    if incomplete_df.shape[0] > 0:
        incomplete_df.to_csv(
            output.with_name(f"{output.stem}.incomplete{output.suffix}"),
            index=False
        )

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
    properties: Annotated[
        str,
        typer.Option(
            help="A comma seperated list of the properties for which Copernicus values will be " + \
                "retrieved. If omitted, all values will be retrieved. Possible values: " + \
                "temperature,salinity,nitrate,oxygen,ph,phosphate,phytoplankton"
        )
    ] = 'temperature,salinity,nitrate,oxygen,ph,phosphate,phytoplankton'
) -> None:
    """Retrieve environmental properties for a csv file with locations."""
    try:
        parsed_properties = parse_cli_properties(properties)
    except ValueError as e:
        raise typer.BadParameter(e)
    result_df = pipelines.get_properties_for_locations_file(input, parsed_properties)
    result_df.rename(columns={
        'temperature_copernicus': 'temperature_copernicus [C]',
        'salinity': 'salinity [psu]',
        'phosphate': 'phosphate [mmol.m-3]',
        'nitrate': 'nitrate [mmol.m-3]',
        'oxygen': 'oxygen [mmol.m-3]',
        'phytoplankton': 'phytoplankton [mmol.m-3]',
        'ph_copernicus': 'ph_copernicus [1]'
    }, inplace=True)
    result_df.to_csv(output, index=False)

if __name__ == "__main__":
    app()
