from datetime import date, timedelta

import pytest

from utils.epinow2.functions import (
    generate_task_configs,
    generate_timestamp,
    validate_args,
)


def test_exclusions():
    """Tests that two disease pairs of exclusion generates 100 configs."""
    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)
    task_exclusions = "ID:COVID-19,WA:Influenza"

    default_args = {
        "state": "all",
        "disease": "all",
        "report_date": report_date,
        "reference_dates": [min_reference_date, max_reference_date],
        "data_source": "nssp",
        "data_path": "gold/",
        "data_container": None,
        "production_date": production_date,
        "job_id": "test-job-id",
        "as_of_date": as_of_date,
        "task_exclusions": task_exclusions,
    }
    validated_args = validate_args(**default_args)
    task_configs, _ = generate_task_configs(**validated_args)
    remaining_configs = 100
    assert len(task_configs) == remaining_configs

def test_single_exclusion():
    """Tests that a single disease pair exclusion generates 101 configs."""

    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)
    task_exclusions = "ID:COVID-19"

    default_args = {
        "state": "all",
        "disease": "all",
        "report_date": report_date,
        "reference_dates": [min_reference_date, max_reference_date],
        "data_source": "nssp",
        "data_path": "gold/",
        "data_container": None,
        "production_date": production_date,
        "job_id": "test-job-id",
        "as_of_date": as_of_date,
        "task_exclusions": task_exclusions,
    }
    validated_args = validate_args(**default_args)
    task_configs, _ = generate_task_configs(**validated_args)
    remaining_configs = 101
    assert len(task_configs) == remaining_configs

