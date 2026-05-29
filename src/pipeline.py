import os
import subprocess
from pathlib import Path

import dvc
import great_expectations as gx
import mlflow
import numpy as np
import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, Output, asset, asset_check
from dotenv import load_dotenv
from evidently.suite import TestSuite
from evidently.tests import TestColumnDrift

load_dotenv()


@asset(group_name="data_ingestion")
def ingested_today_data() -> Output[pd.DataFrame]:
    """Ingest today's data from the specified path in the .env file"""
    today_data_path = os.getenv("RAW_DATA_PATH", "data/today_data.csv")
    repo_url = os.getenv("GIT_REPO_URL")

    try:
        with dvc.api.open(
            repo=repo_url,
            path=str(today_data_path),
            mode="r",
        ) as f:
            df = pd.read_csv(f)
    except Exception as e:
        raise FileNotFoundError(
            f"🚨 ERROR: Could not load today's data from {today_data_path}. "
            f"Details: {str(e)}"
        )

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
        gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=100.0)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(column="price", type_="float64")
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


@asset(
    group_name="monitoring",
)
def radar_price(ingested_today_data: pd.DataFrame) -> str:
    """Execute a radar to detect concept drifta and log results to MLflow"""
    reference_data_path = os.getenv("REFERENCE_DATA_PATH")
    try:
        reference_df = pd.read_csv(reference_data_path)
    except Exception as e:
        raise FileNotFoundError(
            f"🚨 ERROR: Could not load reference data from {reference_data_path}. "
            f"Details: {str(e)}"
        )

    radar_suite = TestSuite(tests=[TestColumnDrift(column_name="price")])

    radar_suite.run(reference_data=reference_df, current_data=ingested_today_data)

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment(os.getenv("MLFLOW_EXPERIMENT_NAME", "Price_Drift_Checks"))

    with mlflow.start_run(run_name="daily_rent_drift_check"):
        results = radar_suite.as_dict()

        parameters_test = results["tests"][0]["parameters"]
        drift_score = parameters_test.get("score", 0.0)
        is_drift_detected = parameters_test.get("drift_detected", False)

        mlflow.log_metric("drift_score_price", drift_score)
        mlflow.log_metric("is_drift_detected", int(is_drift_detected))

        html_path = "radar_report.html"
        radar_suite.save_html(html_path)

        mlflow.log_artifact(html_path, artifact_path="evidently_reports")

        if os.path.exists(html_path):
            os.remove(html_path)

    if is_drift_detected:
        print(
            f" ⚠️ ALERT: Concept drift detected in 'price' column! Score: {drift_score:.4f}."
        )
    else:
        print(
            f" ✅ No concept drift detected in 'price' column. Score: {drift_score:.4f}."
        )
    return "Radar executed and results logged to MLflow"
