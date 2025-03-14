from datetime import date, timedelta

import pytest

from cfa_config_generator.utils.epinow2.constants import (
    all_diseases,
    all_states,
    nssp_states_omit,
)
from cfa_config_generator.utils.epinow2.functions import (
    extract_user_args,
    generate_default_job_id,
    generate_timestamp,
    validate_args,
)


def test_extract_user_args():
    """Tests default set of extract_user_args."""
    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

    default_args = {
        "task_exclusions": None,
        "exclusions": None,
        "state": "all",
        "disease": "all",
        "report_date": report_date,
        "production_date": production_date,
        "reference_dates": [min_reference_date, max_reference_date],
        "data_path": f"gold/{report_date}.parquet",
        "data_container": "nssp-etl",
        "job_id": generate_default_job_id(as_of_date=as_of_date),
        "as_of_date": as_of_date,
        "output_container": "test-container",
    }

    extracted_args = extract_user_args(as_of_date=as_of_date)

    assert extracted_args.keys() == default_args.keys()
    assert "Rt-estimation" in extracted_args["job_id"]


def test_validate_args_default():
    """Tests validate_args with default arguments."""
    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

    default_args = {
        "state": "all",
        "disease": "all",
        "exclusions": None,
        "report_date": report_date,
        "production_date": production_date,
        "reference_dates": [min_reference_date, max_reference_date],
        "data_path": f"gold/{report_date}.parquet",
        "data_container": None,
        "job_id": "test-job-id",
        "as_of_date": as_of_date,
        "output_container": "test-container",
    }

    validated_args = validate_args(**default_args)
    assert validated_args == {
        "state": list(set(all_states) - set(nssp_states_omit)),
        "disease": all_diseases,
        "exclusions": None,
        "reference_dates": [min_reference_date, max_reference_date],
        "report_date": report_date,
        "data_path": f"gold/{report_date}.parquet",
        "data_container": None,
        "production_date": date.today(),
        "job_id": "test-job-id",
        "as_of_date": as_of_date,
        "output_container": "test-container",
    }


def test_invalid_state():
    """Tests that an invalid state raises a ValueError."""
    args = {
        "state": "invalid",
        "disease": "all",
        "report_date": date.today(),
        "reference_dates": [date.today(), date.today()],
        "data_path": "gold/",
        "data_container": None,
        "production_date": date.today(),
    }
    with pytest.raises(ValueError):
        validate_args(**args)


def test_invalid_disease():
    """Tests that an invalid disease raises a ValueError."""
    args = {
        "state": "all",
        "disease": "invalid",
        "report_date": date.today(),
        "reference_dates": [date.today(), date.today()],
        "data_path": "gold/",
        "data_container": None,
        "production_date": date.today(),
    }
    with pytest.raises(ValueError):
        validate_args(**args)


def test_invalid_reference_date_format():
    """Tests that an invalid reference date format raises a ValueError."""
    args = {
        "state": "all",
        "disease": "all",
        "report_date": date.today(),
        "reference_dates": "2024-01-01",
        "data_path": "gold/",
        "data_container": None,
        "production_date": date.today(),
    }
    with pytest.raises(ValueError):
        validate_args(**args)


def test_invalid_reference_date_range():
    """Tests that an invalid reference date range raises a ValueError."""
    args = {
        "state": "all",
        "disease": "all",
        "report_date": date.today(),
        "reference_dates": "2025-01-01,2026-01-01",
        "data_path": "gold/",
        "data_container": None,
        "production_date": date.today(),
    }
    with pytest.raises(ValueError):
        validate_args(**args)


def test_invalid_disease_exclusion():
    """Tests that an invalid disease raises a ValueError."""

    as_of_date = generate_timestamp()
    # valid diseases are 'COVID-19' or 'Influenza'
    task_exclusions = "WA:monkeypox"

    args = {
        "state": "invalid",
        "disease": "all",
        "report_date": date.today(),
        "reference_dates": [date.today(), date.today()],
        "data_path": "gold/",
        "data_container": None,
        "production_date": date.today(),
        "job_id": "test-job-id",
        "as_of_date": as_of_date,
        "task_exclusions": task_exclusions,
    }

    with pytest.raises(ValueError):
        validate_args(**args)

def test_invalid_exclusions_file():
    """Tests that an invalid state raises a ValueError."""
    args = {
        "state": "all",
        "disease": "all",
        "exclusions": "tests/test_exclusions_fails.csv",
        "report_date": date.today(),
        "reference_dates": [date.today(), date.today()],
        "data_path": "gold/",
        "data_container": None,
        "production_date": date.today(),
    }
    with pytest.raises(CustomError):
        generate_tasks_excl_from_data_excl(**args)
