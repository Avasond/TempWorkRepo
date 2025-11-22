from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from udacity.common import final_project_sql_statements


default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    # Optional Airflow niceties, safe to leave as defaults in the UI
    # "email_on_retry": False,
    # "email_on_failure": False,
}

# S3 paths
# These must match where you copied the Udacity data into your own bucket:
#   s3://sparkify-ava-gates/log-data/
#   s3://sparkify-ava-gates/song-data/
#   s3://sparkify-ava-gates/log_json_path.json
S3_BUCKET = "sparkify-ava-gates"
LOG_DATA_KEY = "log-data"
SONG_DATA_KEY = "song-data"
LOG_JSONPATH = f"s3://{S3_BUCKET}/log_json_path.json"


@dag(
    default_args=default_args,
    description="Load and transform Sparkify data in Redshift with Airflow",
    schedule_interval="0 * * * *",  # hourly as required by the rubric
    catchup=False,
    max_active_runs=1,
)
def final_project():
    """
    Main DAG for the Sparkify project.

    Flow:
      * Stage event and song JSON data from S3 into Redshift staging tables
      * Load songplays fact table
      * Load user, song, artist, time dimension tables
      * Run data quality checks on all analytical tables
    """

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket=S3_BUCKET,
        s3_key=LOG_DATA_KEY,
        json_path=LOG_JSONPATH,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket=S3_BUCKET,
        s3_key=SONG_DATA_KEY,
        json_path="auto",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=final_project_sql_statements.songplay_table_insert,
        append_only=False,  # full reload
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="users",
        sql_query=final_project_sql_statements.user_table_insert,
        append_only=False,  # insert delete pattern
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="songs",
        sql_query=final_project_sql_statements.song_table_insert,
        append_only=False,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artists",
        sql_query=final_project_sql_statements.artist_table_insert,
        append_only=False,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        sql_query=final_project_sql_statements.time_table_insert,
        append_only=False,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        # parametrized list of tables to check, matches rubric expectation
        tables=[
            "songplays",
            "users",
            "songs",
            "artists",
            "time",
        ],
    )

    end_operator = DummyOperator(task_id="End_execution")

    # Dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]

    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()