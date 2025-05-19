"""Airflow DAG: Daily Spotify data pipeline."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DBT_DIR = "/opt/airflow/dbt_project"


def extract_and_load(**ctx):
    from extractors import SpotifyClient, ListeningHistoryExtractor, AudioFeaturesExtractor
    from loaders import SnowflakeLoader

    client = SpotifyClient()
    loader = SnowflakeLoader()

    # Extract listening history
    lh = ListeningHistoryExtractor(client)
    plays = lh.extract_recent_plays(max_items=200)
    loader.load_listening_history(plays)

    # Extract audio features for new tracks
    track_ids = lh.extract_unique_track_ids(plays)
    af = AudioFeaturesExtractor(client)
    features = af.extract_features(track_ids)
    loader.load_audio_features(features)

    ctx["ti"].xcom_push(key="plays_count", value=len(plays))
    ctx["ti"].xcom_push(key="features_count", value=len(features))


with DAG(
    "spotify_daily_pipeline",
    default_args=default_args,
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spotify", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_load = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir . --target prod",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir . --target prod",
    )

    end = EmptyOperator(task_id="end")

    start >> extract_load >> dbt_run >> [dbt_test, dbt_snapshot] >> end
