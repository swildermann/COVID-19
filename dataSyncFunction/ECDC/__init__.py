import datetime
import logging
from sqlalchemy.sql import text as sa_text
import sqlalchemy
import urllib
from sqlalchemy import create_engine
import pandas as pd
import requests
import os
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    today = datetime.datetime.now().date()
    date = today
    while True:
        try:
            df = pd.read_excel("https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-%s.xlsx"%date.strftime("%Y-%m-%d"))
            logging.info("date: %s"%date)
            break
        except Exception as e:
            logging.info(e)
            date -= datetime.timedelta(days=1)
    df['DateRep'] = pd.to_datetime(df['DateRep']).dt.date

    country_col = 'Country/Region'

    df = df.rename(columns={'Countries and territories': country_col,
                            'DateRep': 'date', 'Deaths': 'deaths', 'Cases': 'infections'})

    df = df[['date', 'infections', 'deaths', country_col]]

    df = df.sort_values(by=['date', country_col])

    df[df[country_col] == 'Germany'].groupby(by=[country_col]).cumsum()

    df_cumsum = df.groupby(by=[country_col]).cumsum()

    df_result = df[['date', country_col]].join(df_cumsum)
    
    username = os.environ.get('keyvault_db_username')
    password = os.environ.get('keyvault_db_password')

    params = urllib.parse.quote_plus(
        'Driver={ODBC Driver 17 for SQL Server};Server=tcp:covid19dbserver.database.windows.net,1433;Database=covid19db;Uid='+username
        +'@covid19dbserver;Pwd='+password
        +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=False)

    assert df_result.duplicated().sum() == 0

    table_name = "ecdc_updates"
    try:
        pd.read_sql("select Top(1) * from dbo.%s" %
                              table_name, engine)
        engine.execute(sa_text('''TRUNCATE TABLE %s''' %
                               table_name).execution_options(autocommit=True))
    except:
        pass

    for col in [country_col]:
        df_result[col] = df_result[col].str.slice(start=0, stop=99)

    df_result = df_result[['Country/Region','infections','deaths','date']]

    df_result.to_sql(table_name,
                     engine,
                     if_exists='append', schema='dbo',
                     index=False, chunksize=100,
                     method='multi',                dtype={country_col: sqlalchemy.types.NVARCHAR(length=100)})

    merge_statement = '''
    MERGE INTO dbo.ECDC AS Target 
    USING 
        (
            SELECT [Country/Region], infections, deaths, date 
            FROM dbo.ECDC_updates
        ) AS Source 
    ON Target.[Country/Region] = Source.[Country/Region] 
        AND Target.date = Source.date 
    WHEN MATCHED THEN 
        UPDATE SET 
        Target.infections = Source.infections, 
        Target.deaths = Source.deaths
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT ([Country/Region], infections, deaths, date)
        VALUES (Source.[Country/Region], Source.infections, Source.deaths, Source.date);

    '''
    engine.execute(sa_text(merge_statement).execution_options(autocommit=True))

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
