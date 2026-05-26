import os
import subprocess
from pathlib import Path

import dvc
import pandas as pd
from dagster import Output, asset
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
