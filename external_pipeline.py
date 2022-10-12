from __future__ import annotations

import logging
import sys
import tempfile
import time

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from botocore.exceptions import ClientError

BASE_DIR = tempfile.gettempdir()
lambda_function_name = "demogo-external-crawler"
glue_crawler_name = "demogo-external-raw"
glue_job_name = "demogo_external_curated"

with DAG(
        dag_id='external_pipeline',
        schedule_interval="@daily",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=['demogo'],
) as dag:
    @task(task_id='lambda_crawler_run')
    def lambda_crawler_run(function_name):
        import boto3
        session = boto3.session.Session()
        lambda_client = session.client('lambda')
        payload = Variable.get("naver_apikey")
        logging.info("Invoke Lambda Crawler: {}, Payload: {}".format(function_name, payload))
        response = lambda_client.invoke(
            FunctionName=function_name,
            LogType='Tail',
            Payload=payload
        )
        logging.info("Lambda Crawler Done: {}".format(function_name))
        logging.info(response)

    @task(task_id='glue_crawler_start')
    def glue_crawler_start(crawler_name):
        import boto3
        session = boto3.session.Session()
        glue_client = session.client('glue')
        try:
            logging.info("Initializing AWS Glue Crawler: {}".format(crawler_name))
            response = glue_client.start_crawler(Name=crawler_name)
            return response
        except ClientError as e:
            raise Exception('boto3 client error in start_a_crawler: ' + e.__str__())
        except Exception as e:
            raise Exception('Unexpected error in start_a_crawler: ' + e.__str__())

    @task(task_id='glue_crawler_wait_completion')
    def glue_crawler_wait_completion(crawler_name):
        run_state = 'RUNNING'
        while True:
            if run_state in ['READY', 'STOPPING']:
                return {'CrawlerRunState': run_state}
            else:
                run_state = get_crawler_status(crawler_name)
                time.sleep(10)

    @task(task_id=f'glue_job_run')
    def glue_job_run(job_name):
        import boto3
        session = boto3.session.Session()
        glue_client = session.client('glue')
        job_run = glue_client.start_job_run(JobName=job_name)
        return wait_glue_job(job_name, job_run['JobRunId'])

    lambda_crawler_run_task = lambda_crawler_run(lambda_function_name)
    glue_crawler_raw_start_task = glue_crawler_start(glue_crawler_name)
    glue_crawler_raw_wait_completion_task = glue_crawler_wait_completion(glue_crawler_name)
    glue_job_run_task = glue_job_run(glue_job_name)

    lambda_crawler_run_task >> glue_crawler_raw_start_task >> glue_crawler_raw_wait_completion_task >> glue_job_run_task

def get_crawler_status(crawler_name):
    import boto3
    session = boto3.session.Session()
    glue_client = session.client('glue')
    logging.info(f"Polling for AWS Glue Crawler {crawler_name} current run state")
    crawler_status = glue_client.get_crawler(
        Name=crawler_name
    )
    crawler_run_state = crawler_run_state = (crawler_status['Crawler']['State']).upper()
    logging.info("Crawler Run state is {}".format(crawler_run_state))
    return crawler_run_state

def wait_glue_job(job_name, run_id):
    import boto3
    session = boto3.session.Session()
    glue_client = session.client('glue')
    run_state = 'RUNNING'
    while True:
        if run_state in ['FAILED', 'STOPPED', 'SUCCEEDED']:
            return {'JobRunState': run_state, 'JobRunId': run_id}
        else:
            logging.info(f"Polling for AWS Glue Job {run_id} current run state")
            job_run_status = glue_client.get_job_run(
                JobName=job_name,
                RunId=run_id,
                PredecessorsIncluded=True
            )
            run_state = (job_run_status['JobRun']['JobRunState']).upper()
            logging.info("Crawler Run state is {}".format(run_state))
            time.sleep(10)