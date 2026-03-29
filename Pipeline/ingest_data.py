#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine

year = 2021
month = 1



prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'


# In[3]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates
)




get_ipython().system('uv add sqlalchemy "psycopg[binary,pool]"')






engine = create_engine('postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}: {pg_port}/{pg_db}',pool_pre_ping=True)





# In[12]:

def run():
    pg_user = 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_port = 5432
    pg_db = 'ny_taxi'
    chunksize=100000

    df_iter = pd.read_csv(
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True
    for df_chunk in df_iter:
        if first:
            df.head(n=0).to_sql(
                name='yellow_taxi_data', 
                con=engine, 
                if_exists='replace')
            first = False
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    print("Table created")

    # Insert chunk
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append"
    )

    print("Inserted:", len(df_chunk))


# In[ ]:




