from datetime import date, timedelta

from cfa_config_generator.utils.epinow2.functions import (
    generate_task_configs,
    generate_tasks_excl_from_data_excl,
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


def test_task_exclusion():
    """Tests that exclude data function removes a specific state:disease pair."""

    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)
    task_exclusions = "ID:COVID-19"

    default_args = {
        "state": "ID",
        "disease": "COVID-19",
        "report_date": report_date,
        "reference_dates": [min_reference_date, max_reference_date],
        "data_path": "gold/",
        "data_container": None,
        "production_date": production_date,
        "job_id": "test-job-id",
        "as_of_date": as_of_date,
        "task_exclusions": task_exclusions,
    }
    validated_args = validate_args(**default_args)
    task_configs, _ = generate_task_configs(**validated_args)
    remaining_configs = 0
    assert len(task_configs) == remaining_configs

def test_data_exclusion():
    """Tests that exclude csv only runs task for the tasks specified in csv."""

    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

    default_args = {
        "state": "all",
        "disease": "all",
        "report_date": report_date,
        "reference_dates": [min_reference_date, max_reference_date],
        "data_container": None,
        "production_date": production_date,
        "job_id": "test-job-id",
        "as_of_date": as_of_date,
        "task_exclusions": None,
        "exclusions": "tests/test_exclusions_passes.csv",
    }
    
    task_excl_str = generate_tasks_excl_from_data_excl(**default_args)
    default_args['task_exclusions'] = task_excl_str
    sanitized_args = validate_args(**default_args)

    task_configs, _ = generate_task_configs(**sanitized_args)
    remaining_configs = 2
    assert len(task_configs) == remaining_configs