from week_two.assignment_prefect_task.assi_flows.ass_etl_load_web_to_gcs import etl_web_to_gcs
from prefect import task, flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import pandas as pd
from pathlib import Path


# @task(name='ingest data into gcs',tags=['second ass part'])
# def call_etl_file(years: list[int] = [2019]
#     ,months: list[int]=[2,3],color: str = 'yellow'):

#     etl_web_to_gcs(years=years,months=months, color=color)


@task(name='download data from gcs',tags=['second ass part'])
def download_file_from_gcs(year, month,color):
    file_path_name = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_bucket = GcsBucket.load('zoom-gcs')

    gcs_bucket.get_directory(from_path=file_path_name, local_path='.')
    return Path(file_path_name)

@task(name='ingest into bigquery',tags=['second ass part'])
def load_into_bigquery(file_path_name):
    gcp_credentials = GcpCredentials.load('zoom-gcp-creds')
    df = pd.read_parquet(file_path_name)
    print('----------------------------------')
    print(f"number of rows is ={len(df)}")
    print('------------------------------------')

    df.to_gbq(project_id='idowudata',
    destination_table='idowu_dataset.ny_taxi_rides',
    chunksize=100_000,
    if_exists='append',
    credentials=gcp_credentials.get_credentials_from_service_account())

@flow(name='subflow for parameters')
def subflow_parameters_load(year:int,month:int,color:str):
    data_path = download_file_from_gcs(year,month,color)
    load_into_bigquery(data_path)
    


@flow(name='parent flow for ingesting')
def etl_gcs_to_bq_main_flow(years: list[int],months:list[int],color:str):
    if len(years) == 1 and len(months) >1:
        for i in range(len(months)-1):
            years.append(years[0])
    else:
        pass
    for year, month in zip(years,months):
        subflow_parameters_load(year,month,color)


if __name__ == '__main__':
    etl_gcs_to_bq_main_flow(years=[2019],months=[2,3],color='yellow')
