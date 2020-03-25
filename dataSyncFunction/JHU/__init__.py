import datetime
import logging
import pandas as pd
from sqlalchemy.sql import text as sa_text
import sqlalchemy
import urllib
from sqlalchemy import create_engine
import requests
import os

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    today = datetime.datetime.now().date()
    for i in range(10):
        try:
            logging.info(f"Trying {today}...")
            download_insert_hopkins(today)
            logging.info(f"Success with {today}.")
            break
        except Exception as e:
            logging.info(e)
            logging.info(f"No data for date {today}, yet.")
            today -= datetime.timedelta(days=1)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)



base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"

def download_insert_hopkins(date):
    df = pd.read_csv(base_url+date.strftime("%m-%d-%Y")+".csv")
    df = df.rename(columns={'Country_Region':'Country/Region','Province_State':'Province/State','Last_Update':'Last Update'})
    df['date'] = pd.to_datetime(df['Last Update']).dt.date

    possibly_missing = ["Admin2","FIPS",'Lat','Long_']
    for col in possibly_missing:
        if col not in df.columns:
            df[col] = pd.NA

    df_result = df[['Country/Region','Province/State',"Admin2","FIPS",'Lat','Long_','Confirmed','Deaths','Recovered','date']].rename(columns={'Admin2':'District','Long_':'Long','Confirmed':'infections','Deaths':'deaths','Recovered':'recovered'})
    
    #for col in ['infections', 'deaths', 'recovered']:
    #    df_result[col] = df_result[col].fillna(0)

    username = os.environ.get('keyvault_db_username')
    password = os.environ.get('keyvault_db_password')

    params = urllib.parse.quote_plus(
        'Driver={ODBC Driver 17 for SQL Server};Server=tcp:covid19dbserver.database.windows.net,1433;Database=covid19db;Uid='+username
        +'@covid19dbserver;Pwd='+password
        +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=False)



    assert df_result.duplicated().sum() == 0

    table_name = "Hopkins"
    table_name_updates = f"{table_name}_updates"

    try:
        df_temp = pd.read_sql(f"select Top(1) * from dbo.{table_name_updates}", engine)
        engine.execute(sa_text(f'TRUNCATE TABLE {table_name_updates}').execution_options(autocommit=True))
    except Exception as e:
        print(e)
        pass
    
    country_col = 'Country/Region'
    province_col = 'Province/State'
    district_col = 'District'

    dtype_dict = {}
    for col in [country_col, province_col, district_col]:
        if df_result[col].notnull().sum() > 0:
            #print(col)
            df_result.loc[df_result[col].notnull(), col] = df_result.loc[df_result[col].notnull(), col].str.slice(start=0, stop=99)
            dtype_dict[col] = sqlalchemy.types.NVARCHAR(length=100)


    df_result.to_sql(table_name_updates, 
                engine,
                if_exists='append',schema='dbo',
                    index=False, chunksize=100,
                    method='multi', dtype=dtype_dict)

    

    merge_statement = f'''
    MERGE INTO dbo.{table_name} AS Target 
    USING 
        (
            SELECT [Country/Region], [Province/State], District, infections, deaths, recovered,FIPS, Lat, Long, date 
            FROM dbo.{table_name_updates}
        ) AS Source 
    ON Target.[Country/Region] = Source.[Country/Region] 
        AND COALESCE(Target.[Province/State], '') = COALESCE(Source.[Province/State], '')
        AND COALESCE(Target.[District], '') = COALESCE(Source.[District], '')
        AND Target.date = Source.date 
    WHEN MATCHED THEN 
        UPDATE SET 
        Target.infections = Source.infections, 
        Target.deaths = Source.deaths, 
        Target.recovered = Source.recovered,
        Target.FIPS = Source.FIPS,
        Target.Lat = Source.Lat,
        Target.Long = Source.Long
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT ([Country/Region], [Province/State],District, FIPS, Lat, Long, infections, deaths, recovered, date)
        VALUES (Source.[Country/Region], Source.[Province/State],Source.District,Source.FIPS, Source.Lat, Source.Long, Source.infections,Source.deaths,Source.recovered, Source.date);
    '''
    engine.execute(sa_text(merge_statement).execution_options(autocommit=True))