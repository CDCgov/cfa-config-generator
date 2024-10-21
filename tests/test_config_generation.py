from datetime import date, timedelta

from utils.epinow2.constants import all_diseases, all_states, nssp_states_omit
from utils.epinow2.functions import (
    generate_task_configs,
    generate_timestamp,
    generate_uuid,
    validate_args,
)


def test_default_config_set():
    """Tests that the default set of configs is generated correctly."""
    report_date = date.today()
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
    }
    validated_args = validate_args(**default_args)
    as_of_date = generate_timestamp()
    job_id = generate_uuid()
    # Generate task-specific configs
    task_configs, _ = generate_task_configs(
        **validated_args, as_of_date=as_of_date, job_id=job_id
    )
    total_tasks_expected = len(list(set(all_states) - set(nssp_states_omit))) * len(
        all_diseases
    )
    assert len(task_configs) == total_tasks_expected


def test_single_geo_disease_set():
    """Tests that a single geography-disease combination returns a single task."""
    report_date = date.today()
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
    }
    validated_args = validate_args(**default_args)
    as_of_date = generate_timestamp()
    job_id = generate_uuid()
    # Generate task-specific configs
    task_configs, _ = generate_task_configs(
        **validated_args, as_of_date=as_of_date, job_id=job_id
    )
    total_tasks_expected = 1
    assert len(task_configs) == total_tasks_expected
