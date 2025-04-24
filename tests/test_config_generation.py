from datetime import date, timedelta

from cfa_config_generator.utils.epinow2.constants import (
    all_diseases,
    all_states,
    nssp_states_omit,
)
from cfa_config_generator.utils.epinow2.functions import (
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
        # Pass date object
        "report_date": report_date,
        # Pass list[date]
        "reference_dates": [min_reference_date, max_reference_date],
        "data_path": f"gold/{report_date.isoformat()}.parquet",
        "data_container": None,
        # Pass date object
        "production_date": production_date,
        "job_id": "test-job-id",
        "as_of_date": as_of_date,
        # Added for validation
        "output_container": "test-container",
        # Added for validation
        "exclusions": None,
        # Added for validation
        "task_exclusions": None,
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
        # Pass date object
        "report_date": report_date,
        # Pass list[date]
        "reference_dates": [min_reference_date, max_reference_date],
        "data_path": f"gold/{report_date.isoformat()}.parquet",
        "data_container": None,
        # Pass date object
        "production_date": production_date,
        "job_id": "test-job-id",
        "as_of_date": as_of_date,
        # Added for validation
        "output_container": "test-container",
        # Added for validation
        "exclusions": None,
        # Added for validation
        "task_exclusions": None,
    }
    validated_args = validate_args(**default_args)

    # Generate task-specific configs
    task_configs, _ = generate_task_configs(**validated_args)
    total_tasks_expected = 1
    assert len(task_configs) == total_tasks_expected
