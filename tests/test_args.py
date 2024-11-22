from datetime import date, timedelta

import pytest

from utils.epinow2.constants import all_diseases, all_states, nssp_states_omit
from utils.epinow2.functions import extract_user_args, validate_args


def test_extract_user_args():
    """Tests default set of extract_user_args."""
    report_date = date.today()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

    default_args = {
        "state": "all",
        "disease": "all",
        "report_date": date.today(),
        "production_date": date.today(),
        "reference_dates": [min_reference_date, max_reference_date],
        "data_source": "nssp",
        "data_path": f"gold/{report_date}.parquet",
        "data_container": None,
    }

    assert extract_user_args() == default_args


def test_validate_args_default():
    """Tests validate_args with default arguments."""
    report_date = date.today()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

    default_args = {
        "state": "all",
        "disease": "all",
        "report_date": date.today(),
        "production_date": date.today(),
        "reference_dates": [min_reference_date, max_reference_date],
        "data_source": "nssp",
        "data_path": f"gold/{report_date}.parquet",
        "data_container": None,
    }

    validated_args = validate_args(**default_args)
    assert validated_args == {
        "state": list(set(all_states) - set(nssp_states_omit)),
        "disease": all_diseases,
        "reference_dates": [min_reference_date, max_reference_date],
        "report_date": report_date,
        "data_path": f"gold/{report_date}.parquet",
        "data_container": None,
        "production_date": date.today(),
    }


def test_invalid_state():
    """Tests that an invalid state raises a ValueError."""
    args = {
        "state": "invalid",
        "disease": "all",
        "report_date": date.today(),
        "reference_dates": [date.today(), date.today()],
        "data_source": "nssp",
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
        "data_source": "nssp",
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
        "data_source": "nssp",
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
        "data_source": "nssp",
        "data_path": "gold/",
        "data_container": None,
        "production_date": date.today(),
    }
    with pytest.raises(ValueError):
        validate_args(**args)
