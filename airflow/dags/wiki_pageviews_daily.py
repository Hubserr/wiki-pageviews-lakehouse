from datetime import datetime, timedelta,timezone
import requests
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable, dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from include.wiki_s3 import build_url_and_key, object_exists, upload_from_url
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import DatabricksTokenProfileMapping

DBT_PROJECT_PATH = "/usr/local/airflow/include/dbt/wiki_pageviews"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH)
profile_config = ProfileConfig(
        profile_name="wiki_pageviews",
        target_name="prod", 
        profile_mapping=DatabricksTokenProfileMapping(
            conn_id="databricks_default",
            profile_args={"catalog": "workspace","schema": "gold"},
            ),
        )
execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)
render_config = RenderConfig(should_detach_multiple_parents_tests=True)


@dag(
    schedule="0 6 * * *",
    start_date=datetime(2026,10,1),
    catchup=False,
    default_args={"retries":2,"retry_delay":timedelta(minutes=2),"retry_exponential_backoff": True},
    tags=["wiki-pageviews"]
)
def wiki_pageviews_daily():

    @task
    def resolve_date(**context):
        logical_date =context["logical_date"]
        process_dt = logical_date - timedelta(days=1)
        today_utc = datetime.now(timezone.utc).date()
      
        data_date = process_dt.strftime("%Y-%m-%d")
        if process_dt.date() >= today_utc:
            raise AirflowFailException(
                f"Data date {data_date} is today or in the future (today UTC: {today_utc}). "
                "Only complete past days can be processed."
            )
        print(f"Logical date: {logical_date}, data date: {data_date}")
        
        return data_date
    
    @task(max_active_tis_per_dagrun=2)
    def upload_hour(process_date:str, hour:int):
        bucket = Variable.get("s3_bucket")
        s3 = S3Hook(aws_conn_id="aws_default").get_conn()

        dt = datetime.strptime(process_date,"%Y-%m-%d") +timedelta(hours=hour)
        url, key = build_url_and_key(dt)

        if object_exists(s3, bucket, key):
            print(f">>> Skipping {key} (already in S3)")
            return "skipped"
        try:
            upload_from_url(s3, bucket, url, key)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise AirflowFailException(f"File not found (404) for {process_date} hour {hour:02d} in {url}")
            else:
                raise

        print(f">>> Uploaded {key}")
        return "uploaded"
    
    process_date = resolve_date()
    uploads = upload_hour.partial(process_date=process_date).expand(hour=list(range(24)))

    run_job = DatabricksRunNowOperator(
        databricks_conn_id="databricks_default",
        task_id="run_databricks_job",
        job_name="wiki_pageviews_daily",
        job_parameters={"process_date":process_date},
        deferrable=True,
        retries=1
        )

    

  
    dbt_task_group = DbtTaskGroup(group_id="dbt",
                                  project_config=project_config,
                                  profile_config=profile_config,
                                  execution_config=execution_config,
                                  render_config=render_config,
                                  operator_args={"vars":{"process_date": "{{ti.xcom_pull(task_ids='resolve_date')}}"}}
                                  )
    uploads >> run_job >> dbt_task_group



wiki_pageviews_daily()