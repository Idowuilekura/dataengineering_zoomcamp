from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
import os
from pathlib import Path
import pandas as pd
from typing import List
gcp_cloud_storage_block = GcsBucket.load('zoom-gcs')


@task(name='task_extract_data',tags=['green taxi loads'],retries=3)
def extract_data(csv_url : str, csv_file_name: str,color:str) -> Path:
    """extract the data from github"""
    path_to_file = Path(f"./data/{color}").mkdir(parents=True, exist_ok=True)
    path_to_file = f"./data/{color}"
    get_cwd = os.getcwd()
    os.chdir(path_to_file)
    os.system(f"wget {csv_url}")
    os.chdir(get_cwd)
    return path_to_file

@task(name = 'transform the data',tags=['green taxis loads'])
def transform_the_data(path_to_csv:str, csv_file_name:str):
    get_cwd = os.getcwd()
    os.chdir(path_to_csv)
    data = pd.read_csv(csv_file_name)
    # if 'green' in csv_file_name: 
    #     data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
    #     data['lpep_dropoff_datetime'] = pd.to_datetime(data['lpep_dropoff_datetime'])
    # else:
    #      data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
    #      data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])
    # print(f"dataset rows: {len(data)}")
    data['tpep_pickup_datetime'] = pd.DataFrame(data['tpep_pickup_datetime'])
    data['tpep_dropoff_datetime'] = pd.DataFrame(data['tpep_dropoff_datetime'])
    print(f"dataset rows: {len(data)}")
    os.chdir(get_cwd)
    return data

@task(name="write to local system",tags=['green taxi loads'])
def write_to_local(df:pd.DataFrame,path_to_dir:str, csv_file_name) -> None:
    get_cwd = os.getcwd()
    os.chdir(path_to_dir)
    df.to_parquet(csv_file_name.replace('.csv.gz','')+'.parquet', compression='gzip',engine='fastparquet')
    os.remove(csv_file_name)
    os.chdir(get_cwd)


@task(name='load_data_to_google_cloud_storage',tags=['green taxi loads'])
def load_to_gcs(path_to_csv:Path,csv_file_name) -> None:
    gcp_cloud_storage_block.upload_from_path(
        from_path = path_to_csv+'/'+csv_file_name.replace('.csv.gz','')+'.parquet',
        to_path = path_to_csv+'/'+csv_file_name.replace('.csv.gz','')+'.parquet',
    )
    get_cwd = os.getcwd()
    os.chdir(path_to_csv)
    os.remove(csv_file_name.replace('.csv.gz','')+'.parquet')
    os.chdir(get_cwd)

@flow(name='subflow ingest_green taxi data')
def subflow_ingest(year:int, month:int, color: str):
    csv_file_name = f"{color}_tripdata_{year}-{month:02}.csv.gz"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{csv_file_name}"
    path_to_csv_file = extract_data(url,csv_file_name,color)
    print(path_to_csv_file)
    data = transform_the_data(path_to_csv_file,csv_file_name)
    write_to_local(data,path_to_csv_file,csv_file_name)
    load_to_gcs(path_to_csv_file,csv_file_name)


@flow(name="etl_main_flow")
def etl_web_to_gcs(years: list[int] =
    [2020,2021],months : list[int] = [1,2,3]
    ,color:str = 'green'):
    if len(years) == 1 and len(months) >1:
        for i in range(len(months)-1):
            years.append(years[0])
    else:
        pass
    for year, month in zip(years,months):
        subflow_ingest(year,month,color)


if __name__ == "__main__":
    etl_web_to_gcs(years=[202],months=[1],color='green')

