import os
from datetime import date, timedelta

from utils.epinow2.constants import all_diseases, all_states, nssp_states_omit
from utils.epinow2.functions import (
    extract_user_args,
    generate_task_configs,
    generate_timestamp,
    validate_args,
)


def test_default_config_set():
    """Tests that the default set of configs is generated correctly."""
    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

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
    }
    validated_args = validate_args(**default_args)

    # Generate task-specific configs
    task_configs, _ = generate_task_configs(**validated_args)
    total_tasks_expected = len(set(all_states).difference(nssp_states_omit)) * len(
        all_diseases
    )
    assert len(task_configs) == total_tasks_expected


def test_single_geo_disease_set():
    """Tests that a single geography-disease combination returns a single task."""
    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

    default_args = {
        "state": "CA",
        "disease": "Influenza",
        "report_date": report_date,
        "reference_dates": [min_reference_date, max_reference_date],
        "data_source": "nssp",
        "data_path": "gold/",
        "data_container": None,
        "production_date": production_date,
        "job_id": "test-job-id",
        "as_of_date": as_of_date,
    }
    validated_args = validate_args(**default_args)

    # Generate task-specific configs
    task_configs, _ = generate_task_configs(**validated_args)
    total_tasks_expected = 1
    assert len(task_configs) == total_tasks_expected


def test_single_exclusion_generates_number_configs():
    """Tests that a single disease pair exclusion generates 101 configs."""

    os.environ["task_exclusions"] = "ID:COVID-19"

    as_of_date = generate_timestamp()

    # Pull run parameters from environment
    user_args = extract_user_args(as_of_date=as_of_date)

    # Validate and sanitize args
    sanitized_args = validate_args(**user_args)

    # Generate task-specific configs
    task_configs, _ = generate_task_configs(**sanitized_args)

    total_tasks_expected = 101
    assert len(task_configs) == total_tasks_expected


def test_double_exclusion_generates_number_configs():
    """Tests that two disease pair exclusions generates 100 configs."""

    os.environ["task_exclusions"] = "ID:COVID-19,WA:Influenza"

    as_of_date = generate_timestamp()

    # Pull run parameters from environment
    user_args = extract_user_args(as_of_date=as_of_date)

    # Validate and sanitize args
    sanitized_args = validate_args(**user_args)

    # Generate task-specific configs
    task_configs, _ = generate_task_configs(**sanitized_args)

    total_tasks_expected = 100
    assert len(task_configs) == total_tasks_expected
