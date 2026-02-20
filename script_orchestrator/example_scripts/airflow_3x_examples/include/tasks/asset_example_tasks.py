"""
Asset-based orchestration example tasks.

These tasks demonstrate Airflow 3.x asset-based scheduling,
where one DAG produces an asset and another DAG is triggered when that asset updates.

In Dagster terms:
- update_iss_coordinates produces the "iss_coordinates" asset
- process_iss_coordinates depends on "iss_coordinates"
"""

import json
import tempfile
import requests


def _get_iss_coordinates_file_path() -> str:
    """Get the file path for storing ISS coordinates."""
    return tempfile.gettempdir() + "/iss_coordinates.txt"


def _update_iss_coordinates() -> None:
    """Fetch current ISS coordinates from API and save to file.

    This function produces the 'iss_coordinates' asset.
    In Airflow, this is declared via outlets in the YAML.
    In Dagster, this would be an @asset that materializes iss_coordinates.
    """
    placeholder = {"latitude": "0.0", "longitude": "0.0"}

    try:
        response = requests.get("http://api.open-notify.org/iss-now.json", timeout=5)
        response.raise_for_status()
        data = response.json()
        coordinates = data.get("iss_position", placeholder)
    except Exception:
        coordinates = placeholder

    with open(_get_iss_coordinates_file_path(), "w") as f:
        f.write(json.dumps(coordinates))


def _read_iss_coordinates() -> None:
    """Read and display ISS coordinates from file.

    This function consumes the 'iss_coordinates' asset.
    In Airflow, it's triggered when the asset is updated.
    In Dagster, this would be an @asset that depends on iss_coordinates.
    """
    path = _get_iss_coordinates_file_path()
    with open(path, "r") as f:
        print("::group::ISS Coordinates")
        print(f.read())
        print("::endgroup::")
