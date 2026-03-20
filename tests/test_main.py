from pathlib import Path
from typer.testing import CliRunner

from metacontextify.__main__ import app

runner = CliRunner()

def test_id_file():
    result = runner.invoke(
        app,
        [
            'id-file',
            './tests/10_samples.txt',
            'sample', 
            './tests/10_samples_output.csv'
        ]
    )
    assert result.exit_code == 0
    assert Path('./tests/10_samples_output.csv').is_file()

def test_simsearch():
    result = runner.invoke(app, ['simsearch', './tests/PF00313_hits.json', './tests/PF00313_10_results.csv', '--nb-hits', '10'])
    assert result.exit_code == 0
    assert Path('./tests/PF00313_10_results.csv').is_file()

def test_functional_csv():
    result = runner.invoke(app, ['location-file', './tests/input_functional_csv.csv', './tests/output_functional_csv.csv' ])
    assert result.exit_code == 0
    assert Path('./tests/output_functional_csv.csv').is_file()

def test_broken_csv():
    result = runner.invoke(app, ['location-file', './tests/input_broken_csv.csv', './tests/output_broken_csv.csv' ])
    assert result.exit_code == 1
    assert not Path('./tests/output_broken_csv.csv').is_file()