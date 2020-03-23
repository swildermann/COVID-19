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

    # %%

    # %%
    confirmed = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"

    # %%
    deaths = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv"

    # %%
    recovered = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"

    # %%
    inputs = {'deaths': deaths, 'infections': confirmed, 'recovered': recovered}
    df_result = None
    country_col = 'Country/Region'
    province_col = 'Province/State'

    for qualifier, url in inputs.items():
        df = pd.read_csv(url)

        df = df.drop(['Lat', 'Long'], axis=1)
        melt_cols = list(df.columns)
        melt_cols.remove(country_col)
        melt_cols.remove(province_col)
        df_melt = pd.melt(
            df, id_vars=[country_col, province_col], value_vars=melt_cols)
        df_melt = df_melt.rename(
            columns={'value': qualifier, 'variable': 'date'})
        df_melt['date'] = pd.to_datetime(df_melt['date']).dt.date

        if df_result is None:
            df_result = df_melt
        else:
            df_result = pd.merge(left=df_result, right=df_melt, on=[
                country_col, province_col, 'date'], how='outer')

    username = os.environ.get('keyvault_db_username')
    password = os.environ.get('keyvault_db_password')

    params = urllib.parse.quote_plus(
        'Driver={ODBC Driver 17 for SQL Server};Server=tcp:covid19dbserver.database.windows.net,1433;Database=covid19db;Uid='+username
        +'@covid19dbserver;Pwd='+password
        +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=False)


    # %%
    assert df_result.duplicated().sum() == 0

    table_name = "Hopkins_updates"
    try:
        pd.read_sql("select Top(1) * from dbo.%s" %
                              table_name, engine)
        engine.execute(sa_text('''TRUNCATE TABLE %s''' %
                               table_name).execution_options(autocommit=True))
    except:
        pass

    # %%
    for col in [country_col, province_col]:
        df_result[col] = df_result[col].str.slice(start=0, stop=99)

    df_result = df_result[['Country/Region','Province/State','infections','recovered','deaths','date']]

    # %%
    df_result.to_sql(table_name,
                     engine,
                     if_exists='append', schema='dbo',
                     index=False, chunksize=100,
                     method='multi',
                     dtype={country_col: sqlalchemy.types.NVARCHAR(length=100),
                            province_col: sqlalchemy.types.NVARCHAR(length=100)})
    merge_statement = '''
    MERGE INTO dbo.Hopkins AS Target 
    USING 
        (
            SELECT [Country/Region], [Province/State], infections, recovered, deaths, date 
            FROM dbo.Hopkins_updates
        ) AS Source 
    ON Target.[Country/Region] = Source.[Country/Region] 
        AND COALESCE(Target.[Province/State], '') = COALESCE(Source.[Province/State], '')
        AND Target.date = Source.date 
    WHEN MATCHED THEN 
        UPDATE SET 
        Target.infections = Source.infections, 
        Target.recovered = Source.recovered,
        Target.deaths = Source.deaths
    WHEN NOT MATCHED BY TARGET THEN 
        INSERT ([Country/Region], [Province/State], infections, recovered, deaths, date)
        VALUES (Source.[Country/Region], Source.[Province/State], Source.infections, Source.recovered, Source.deaths, Source.date);
    '''
    engine.execute(sa_text(merge_statement).execution_options(autocommit=True))

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
