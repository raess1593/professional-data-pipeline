import os
import subprocess
from pathlib import Path

import dvc
import great_expectations as gx
import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, Output, asset, asset_check
from dotenv import load_dotenv

load_dotenv()


@asset(group_name="data_ingestion")
def ingested_today_data() -> Output[pd.DataFrame]:
    """Ingest today's data from the specified path in the .env file"""
    today_data_path = os.getenv("RAW_DATA_PATH", "data/today_data.csv")
    repo_url = os.getenv("GIT_REPO_URL")

    with dvc.api.open(
        repo=repo_url,
        path=str(today_data_path),
        mode="r",
    ) as f:
        df = pd.read_csv(f)

    try:
        git_hash = (
            subprocess.check_output(["git", "rev-parse", "HEAD"])
            .decode("ascii")
            .strip()
        )
    except subprocess.CalledProcessError:
        git_hash = "unknown_local_state"

    return Output(
        value=df,
        metadata={
            "source_path": today_data_path,
            "git_hash": git_hash,
            "row_count": len(df),
        },
    )


@asset_check(asset="ingested_today_data", severity=AssetCheckSeverity.ERROR)
def check_ingested_data(ingested_today_data: pd.DataFrame) -> AssetCheckResult:
    """Check the quality of the ingested data using Great Expectations"""
    context = gx.get_context(mode="ephemeral")

    suite = context.add_expectation_suite(
        expectation_suite_name="ingested_data_suite", overwrite_existing=True
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="precio_alquiler", min_value=100.0
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="precio_alquiler", type_="float64"
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="habitaciones", min_value=1
        )
    )

    validator = context.sources.pandas_default.read_dataframe(ingested_today_data)

    checkpoint_result = context.add_or_update_checkpoint(
        name="ingested_data_checkpoint",
        validator=validator,
        expectation_suite_name=suite.name,
    ).run()

    is_passed = checkpoint_result.success

    if not is_passed:
        raise ValueError(
            "🚨 QUALITY BLOCK: The scraped data contains apartments "
            "with 0 rooms or unrealistic prices. Pipeline aborted."
        )

    return AssetCheckResult(
        passed=is_passed,
        metadata={"row_count": len(ingested_today_data), "suite_name": suite.name},
    )
