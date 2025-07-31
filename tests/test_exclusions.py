from datetime import date, timedelta
from io import StringIO
from typing import LiteralString

import hypothesis.strategies as st
import polars as pl
import pytest
from hypothesis import given
from polars.testing.parametric import column, dataframes

from cfa_config_generator.utils.epinow2.constants import all_diseases, nssp_valid_states
from cfa_config_generator.utils.epinow2.functions import (
    generate_task_configs,
    generate_tasks_excl_from_data_excl,
    generate_timestamp,
    validate_args,
)


@pytest.fixture
def good_config() -> LiteralString:
    """
    A string representing a valid data exclusions file contents.
    """
    return """state,disease,report_date,reference_date
NY,COVID-19,2025-01-01,2025-01-01
OH,Influenza,2025-01-01,2025-01-01"""


def test_exclusions():
    """Tests that two disease pairs of exclusion generates 150 configs."""
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
        data_container="test-container",
        production_date=production_date,
        job_id="test-job-id",
        as_of_date=as_of_date,
        output_container="test-container",
        facility_active_proportion=1.0,
        task_exclusions="ID:COVID-19,WA:Influenza,OH:RSV",
        exclusions=None,
    )
    task_configs, _ = generate_task_configs(**validated_args)
    remaining_configs = 153
    assert len(task_configs) == remaining_configs

    # Test that facility_active_proportion is present with default value
    for config in task_configs:
        assert "facility_active_proportion" in config
        assert config["facility_active_proportion"] == 1.0


def test_single_exclusion():
    """Tests that a single disease pair exclusion generates 152 configs."""

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
        data_container="test-container",
        production_date=production_date,
        job_id="test-job-id",
        as_of_date=as_of_date,
        output_container="test-container",
        facility_active_proportion=1.0,
        task_exclusions="ID:COVID-19",
        exclusions=None,
    )
    task_configs, _ = generate_task_configs(**validated_args)
    remaining_configs = 155
    assert len(task_configs) == remaining_configs

    # Test that facility_active_proportion is present with default value
    for config in task_configs:
        assert "facility_active_proportion" in config
        assert config["facility_active_proportion"] == 1.0


def test_task_exclusion():
    """Tests that exclude data function removes a specific state:disease pair."""

    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

    validated_args = validate_args(
        state="ID",
        disease="COVID-19",
        report_date=report_date,
        reference_dates=[min_reference_date, max_reference_date],
        data_path=f"gold/{report_date.isoformat()}.parquet",
        data_container="test-container",
        production_date=production_date,
        job_id="test-job-id",
        as_of_date=as_of_date,
        output_container="test-container",
        facility_active_proportion=1.0,
        task_exclusions="ID:COVID-19",
        exclusions=None,
    )
    task_configs, _ = generate_task_configs(**validated_args)
    remaining_configs = 0
    assert len(task_configs) == remaining_configs


def test_data_exclusion(good_config):
    """Tests that exclude csv only runs task for the tasks specified in csv."""

    report_date = production_date = date.today()
    as_of_date = generate_timestamp()
    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

    task_excl_str = generate_tasks_excl_from_data_excl(
        pl.read_csv(StringIO(good_config))
    )

    sanitized_args = validate_args(
        state="all",
        disease="all",
        report_date=report_date,
        reference_dates=[min_reference_date, max_reference_date],
        data_path=f"gold/{report_date.isoformat()}.parquet",
        data_container="test-container",
        production_date=production_date,
        job_id="test-job-id",
        as_of_date=as_of_date,
        output_container="test-container",
        facility_active_proportion=1.0,
        task_exclusions=task_excl_str,
        exclusions={
            "path": "tests/test_exclusions_passes.csv",
            "blob_storage_container": "test-container",
        },
    )

    task_configs, _ = generate_task_configs(**sanitized_args)
    remaining_configs = 2
    assert len(task_configs) == remaining_configs

    # Test that facility_active_proportion is present with default value
    for config in task_configs:
        assert "facility_active_proportion" in config
        assert config["facility_active_proportion"] == 1.0


@given(
    dataframes(
        cols=[
            column(name="state", strategy=st.sampled_from(nssp_valid_states)),
            column(name="disease", strategy=st.sampled_from(all_diseases)),
            column(
                name="report_date",
                strategy=st.dates(
                    min_value=date(2025, 1, 1),
                    max_value=date(2025, 3, 15),
                ),
            ),
            column(
                name="reference_date",
                strategy=st.dates(
                    min_value=date(2025, 1, 1),
                    max_value=date(2025, 3, 15),
                ),
            ),
        ],
        min_size=1,
        max_size=100,
    )
)
def test_data_exclusions_prop_check(data: pl.DataFrame):
    """
    We have N = 102 state-disease pairs in total.
    For a dataframe of some number of rows, n, test that we always get out exactly N-n
    data exclusions, and the state-disease pairs in the dataframe are **not** in the
    output string.
    """
    got: str = generate_tasks_excl_from_data_excl(data)

    expected_number_of_rows = len(set(zip(data["state"], data["disease"])))
    total_possible_rows = len(nssp_valid_states) * len(all_diseases)

    # We should get out exactly total_possible_rows - expected_number_of_rows
    assert len(got.split(",")) == (total_possible_rows - expected_number_of_rows)

    # We should not get out any of the state-disease pairs in the dataframe
    for state, disease in zip(data["state"], data["disease"]):
        assert f"{state}:{disease}" not in got


@pytest.mark.parametrize(
    "df",
    [
        # Missing state col
        pl.DataFrame(
            {
                "disease": ["COVID-19", "Influenza"],
                "report_date": ["2025-01-01", "2025-01-01"],
                "reference_date": ["2025-01-01", "2025-01-01"],
            }
        ),
        # Missing disease col
        pl.DataFrame(
            {
                "state": ["NY", "OH"],
                "report_date": ["2025-01-01", "2025-01-01"],
                "reference_date": ["2025-01-01", "2025-01-01"],
            }
        ),
        # Missing report_date col
        pl.DataFrame(
            {
                "state": ["NY", "OH"],
                "disease": ["COVID-19", "Influenza"],
                "reference_date": ["2025-01-01", "2025-01-01"],
            }
        ),
        # Missing reference_date col
        pl.DataFrame(
            {
                "state": ["NY", "OH"],
                "disease": ["COVID-19", "Influenza"],
                "report_date": ["2025-01-01", "2025-01-01"],
            }
        ),
    ],
)
def test_invalid_exclusions_data(df):
    """
    Test that generate_tasks_excl_from_data_excl raises a ValueError when given
    a dataframe with bad columns
    """
    with pytest.raises(ValueError):
        generate_tasks_excl_from_data_excl(excl_df=df)
