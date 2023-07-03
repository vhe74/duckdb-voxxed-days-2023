# %%
import os
import sys
import time
from dotenv import load_dotenv
import boto3
import boto3.session
from botocore.exceptions import ClientError
import duckdb
import pandas

print("__RUNNING__")

try:
    # %%
    load_dotenv(dotenv_path="/root/work/vhe/env.txt")
    ACCESS_KEY_ID=os.getenv("ACCESS_KEY_ID")
    SECRET_ACCESS_KEY=os.getenv("SECRET_ACCESS_KEY")

    # %%
    # getting the file list
    print("getting the file list")

    s3 = boto3.session.Session().client(
        service_name='s3',
        region_name='fr-par',
        use_ssl=True,
        endpoint_url='http://s3.fr-par.scw.cloud',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY
    )

    prefix = 'crypto/parquet/'
    response = s3.list_objects(Bucket='crypto-histo',Prefix=prefix)
    parquetFiles = []
    for on in [c['Key'] for c in response['Contents']]:
        parquetFiles.append(on)
    if 'NextMarker' in response:
        next = response['NextMarker']
    else :
        next = ""

    while next != '':
        response = s3.list_objects(Bucket='crypto-histo',Prefix=prefix, Marker=next)
        newObjects = [c['Key'] for c in response['Contents']]
        for on in newObjects:
            parquetFiles.append(on)
        
        if 'NextMarker' in response:
            next = response['NextMarker']
        else :
            next = ""
    
    print(f"{len(parquetFiles)} files to process")
    # %%
    # setup datalake S3 connection for duckDB
    print("setup datalake S3 connection for duckDB")

    duckdb.install_extension("httpfs")
    duckdb.load_extension("httpfs")
    duckdb.sql(f"SET s3_region='fr-par';")
    duckdb.sql(f"SET s3_access_key_id='{ACCESS_KEY_ID}';")
    duckdb.sql(f"SET s3_secret_access_key='{SECRET_ACCESS_KEY}';")
    duckdb.sql(f"SET s3_endpoint='s3.fr-par.scw.cloud';")

    S3_BUCKET = os.environ.get("S3_BUCKET", "crypto-histo")
    PARQUET_FOLDER = os.environ.get("PARQUET_FOLDER", "parquet")

    # %%
    # Get the count by symbol and save it to S3
    print("get the count by symbol and save it to S3")

    query = f"""
    SELECT
        string_split(string_split(filename, '/')[-1], '.')[1] symbol, COUNT(*) cnt

    FROM
        read_parquet({[f"s3://{S3_BUCKET}/{f}" for f in parquetFiles]}, filename=True)
    GROUP BY symbol
    """
    #res = duckdb.sql(query).df()
    duckdb.sql(f"COPY ({query}) TO 's3://{S3_BUCKET}/res_count_by_symbol.parquet' (FORMAT PARQUET);")

    # %%
    # Get one line per day and symbol and save it to S3
    print("get one line per day and symbol and save it to S3")

    query = f"""
    SELECT
        string_split(string_split(filename, '/')[-1], '.')[1] symbol,
        date_trunc('day', opentime) d,
        MIN(high) min,
        MAX(high) max
    FROM
        read_parquet({[f"s3://{S3_BUCKET}/{f}" for f in parquetFiles]}, filename=True)
    GROUP BY symbol, d
    """

    res = duckdb.sql(query)

    duckdb.sql(f"COPY res TO 's3://{S3_BUCKET}/crypto/index.parquet' (FORMAT PARQUET);")
    # %%

    print("__SUCCEEDED__")
except Exception as e:
    print(e)
    print("__FAILED__")

#Sleep for 10 seconds to give the logger time to send the output of stdout and stderr
time.sleep(10)
