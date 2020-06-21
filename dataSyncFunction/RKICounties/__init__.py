import datetime
import logging
import pandas as pd
from sqlalchemy.sql import text as sa_text
import sqlalchemy
import urllib
from sqlalchemy import create_engine
import os


import azure.functions as func
from urllib.request import urlopen
import json
import pyodbc
import gzip

# sys.path.append(path.dirname(path.dirname(__file__)))

# (explicit relative)
#from ..shared import cleanup_df, country_col, province_col, district_col, date_col
from .. import shared



def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    url = "https://opendata.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0.geojson"

    response = urlopen(url)
    if response.info().get('Content-Encoding') == 'gzip':
        data = gzip.decompress(response.read())
    else:
        data = response.read()
    jsonObj = json.loads(data)

    df_result = pd.DataFrame(jsonObj['features'])
    df_result = pd.json_normalize(df_result.properties)
    rename_dict = {'county':'county','GEN': 'countyName', 'BEZ': 'type', 'BL': 'federalState', 'EWZ': 'population',
                   'cases': 'infections','deaths':'deaths' ,'SHAPE_Area': 'shapearea', 'SHAPE_Length': 'shapelength'}
    df_result = df_result.rename(columns=rename_dict)
    df_result = df_result[rename_dict.values()].copy()
    df_result['federalState'] = df_result['federalState'].apply(
        shared.helpers.translate_county)
    df_result['date'] = datetime.date.today()

    username = os.environ.get('keyvault_db_username')
    password = os.environ.get('keyvault_db_password')

    params = urllib.parse.quote_plus(
        'Driver={ODBC Driver 17 for SQL Server};Server=tcp:covid19dbserver.database.windows.net,1433;Database=covid19db;Uid='+username
        + '@covid19dbserver;Pwd='+password
        + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=False)

    assert df_result.duplicated().sum() == 0

    table_name = "RKICounties"
    table_name_updates = f"{table_name}_updates"

    try:
        _ = pd.read_sql(
            f"select Top(1) * from dbo.{table_name_updates}", engine)
        engine.execute(sa_text(
            f'TRUNCATE TABLE {table_name_updates}').execution_options(autocommit=True))
    except Exception as e:
        print(e)
        pass

    dtype_dict = {}
    for col in shared.helpers.string_cols:
        if col in df_result.columns and df_result[col].notnull().sum() > 0:
            # print(col)
            df_result.loc[df_result[col].notnull(
            ), col] = df_result.loc[df_result[col].notnull(), col].str.slice(start=0, stop=99)
            dtype_dict[col] = sqlalchemy.types.NVARCHAR(length=100)
    df_result.to_sql(table_name_updates,
                     engine,
                     if_exists='append', schema='dbo',
                     index=False, chunksize=100,
                     method='multi', dtype=dtype_dict)


    merge_statement = f'''
MERGE INTO dbo.{table_name} AS Target 
USING (SELECT county, countyname, type, federalstate, population, infections, deaths, shapearea, shapelength, date
FROM dbo.{table_name_updates}) AS Source
 ON Target.federalstate = Source.federalstate AND Target.county = Source.county AND Target.date = Source.date 
 WHEN MATCHED THEN 
 UPDATE SET Target.countyname = Source.countyname, 
 Target.type = Source.type, 
 Target.population = Source.population, 
 Target.infections = Source.infections, 
 Target.deaths = Source.deaths, 
 Target.shapearea = Source.shapearea, 
 Target.shapelength = Source.shapelength 
 WHEN NOT MATCHED BY TARGET THEN 
 INSERT (county, countyname, type, federalstate, population, infections, deaths, shapearea, shapelength, date) 
 VALUES (Source.county, Source.countyname, Source.type, Source.federalstate, Source.population, Source.infections, Source.deaths, Source.shapearea, Source.shapelength, Source.date);
    '''

    engine.execute(sa_text(merge_statement).execution_options(autocommit=True))
    


    logging.info('Python timer trigger function ran at %s', utc_timestamp)

