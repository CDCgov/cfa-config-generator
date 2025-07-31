from datetime import date, timedelta

import polars as pl
import pytest

from cfa_config_generator.utils.epinow2.constants import (
    all_diseases,
    all_states,
    nssp_states_omit,
)
from cfa_config_generator.utils.epinow2.functions import (
    generate_ref_date_tuples,
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

    validated_args = validate_args(
        state="all",
        disease="all",
        report_date=report_date,
        reference_dates=[min_reference_date, max_reference_date],
        data_path=f"gold/{report_date.isoformat()}.parquet",
        data_container=None,
        production_date=production_date,
        job_id="test-job-id",
        as_of_date=as_of_date,
        output_container="test-container",
        facility_active_proportion=1.0,
        exclusions=None,
        task_exclusions=None,
    )

    # Generate task-specific configs
    task_configs, _ = generate_task_configs(**validated_args)
    total_tasks_expected = len(set(all_states).difference(nssp_states_omit)) * len(
        all_diseases
    )
    assert len(task_configs) == total_tasks_expected

    # Test that facility_active_proportion is present with default value
    for config in task_configs:
        assert "facility_active_proportion" in config
        assert config["facility_active_proportion"] == 1.0


def test_single_geo_disease_set():
    """Tests that a single geography-disease combination returns a single task."""
    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

    validated_args = validate_args(
        state="CA",
        disease="Influenza",
        report_date=report_date,
        reference_dates=[min_reference_date, max_reference_date],
        data_path=f"gold/{report_date.isoformat()}.parquet",
        data_container=None,
        production_date=production_date,
        job_id="test-job-id",
        as_of_date=as_of_date,
        output_container="test-container",
        facility_active_proportion=1.0,
        exclusions=None,
        task_exclusions=None,
    )

    # Generate task-specific configs
    task_configs, _ = generate_task_configs(**validated_args)
    total_tasks_expected = 1
    assert len(task_configs) == total_tasks_expected

    # Test that facility_active_proportion is present with default value
    config = task_configs[0]
    assert "facility_active_proportion" in config
    assert config["facility_active_proportion"] == 1.0


@pytest.mark.parametrize(
    "report_dates, time_span, expected_ref_dates",
    [
        # Two report dates with a time span of 7 days
        (
            [date(2023, 1, 1), date(2023, 1, 2)],
            "7d",
            [
                (date(2022, 12, 31), date(2022, 12, 25)),
                (date(2023, 1, 1), date(2022, 12, 26)),
            ],
        ),
        # Same again, but with time span specified negatively
        (
            [date(2023, 1, 1), date(2023, 1, 2)],
            "-7d",
            [
                (date(2022, 12, 31), date(2022, 12, 25)),
                (date(2023, 1, 1), date(2022, 12, 26)),
            ],
        ),
        # Single report date with a time span of 14 days
        (
            [date(2023, 1, 1)],
            "14d",
            [(date(2022, 12, 31), date(2022, 12, 18))],
        ),
        # 10 report dates with a time span of 8 weeks (production setup)
        (
            pl.date_range(
                start=date(2025, 5, 14),
                end=date(2025, 7, 23),
                interval="1w",
                eager=True,
            ),
            "8w",
            [
                (date(2025, 5, 13), date(2025, 3, 19)),
                (date(2025, 5, 20), date(2025, 3, 26)),
                (date(2025, 5, 27), date(2025, 4, 2)),
                (date(2025, 6, 3), date(2025, 4, 9)),
                (date(2025, 6, 10), date(2025, 4, 16)),
                (date(2025, 6, 17), date(2025, 4, 23)),
                (date(2025, 6, 24), date(2025, 4, 30)),
                (date(2025, 7, 1), date(2025, 5, 7)),
                (date(2025, 7, 8), date(2025, 5, 14)),
                (date(2025, 7, 15), date(2025, 5, 21)),
                (date(2025, 7, 22), date(2025, 5, 28)),
            ],
        ),
    ],
)
def test_gen_ref_date_tuples(report_dates, time_span, expected_ref_dates):
    """
    Tests that the reference dates are generated correctly for a list of report dates
    and a time span.
    """
    got = generate_ref_date_tuples(report_dates=report_dates, delta=time_span)
    assert expected_ref_dates == got


def test_facility_active_proportion_in_shared_params():
    """Test that facility_active_proportion is included in shared_params with correct default value."""
    from cfa_config_generator.utils.epinow2.constants import shared_params

    assert "facility_active_proportion" in shared_params
    assert shared_params["facility_active_proportion"] == 1.0


def test_facility_active_proportion_modifiable():
    """Test that facility_active_proportion is included in modifiable_params for CLI usage."""
    from cfa_config_generator.utils.epinow2.constants import modifiable_params

    assert "facility_active_proportion" in modifiable_params


def test_sample_task_facility_active_proportion():
    """Test that sample_task includes facility_active_proportion with correct default value."""
    from cfa_config_generator.utils.epinow2.constants import sample_task

    assert "facility_active_proportion" in sample_task
    assert sample_task["facility_active_proportion"] == 1.0
