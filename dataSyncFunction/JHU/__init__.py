import datetime
import logging
import pandas as pd
from sqlalchemy.sql import text as sa_text
import sqlalchemy
import urllib
from sqlalchemy import create_engine
import os
import sys
from os import path

#sys.path.append(path.dirname(path.dirname(__file__)))

# (explicit relative)
#from ..shared import cleanup_df, country_col, province_col, district_col, date_col
from .. import shared

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


key_cols = [shared.helpers.country_col, shared.helpers.province_col, shared.helpers.district_col, shared.helpers.date_col]


def download_insert_hopkins(date):
    df = pd.read_csv(base_url+date.strftime("%m-%d-%Y")+".csv")
    df = df.rename(columns={'Country_Region': 'Country/Region',
                            'Province_State': 'Province/State', 'Last_Update': 'Last Update'})
    df['date'] = pd.to_datetime(df['Last Update']).dt.date

    possibly_missing = ["Admin2", "FIPS", 'Lat', 'Long_']
    for col in possibly_missing:
        if col not in df.columns:
            df[col] = pd.NA

    df_result = df[['Country/Region', 'Province/State', "Admin2", "FIPS", 'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'date']
                   ].rename(columns={'Admin2': 'District', 'Long_': 'Long', 'Confirmed': 'infections', 'Deaths': 'deaths', 'Recovered': 'recovered'})
    df_result = shared.helpers.cleanup_df(df_result, key_cols=key_cols)

    username = os.environ.get('keyvault_db_username')
    password = os.environ.get('keyvault_db_password')

    params = urllib.parse.quote_plus(
        'Driver={ODBC Driver 17 for SQL Server};Server=tcp:covid19dbserver.database.windows.net,1433;Database=covid19db;Uid='+username
        + '@covid19dbserver;Pwd='+password
        + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=False)

    assert df_result.duplicated().sum() == 0
    assert df_result.duplicated(subset=key_cols).sum() == 0

    table_name = "Hopkins"
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

# def cleanup_df(df_in):
#     df = df_in.copy()
#     # replace nan with 0 in numeric cols
#     fillna_dict = {'infections': 0, 'deaths': 0, 'recovered': 0}
#     df = df.fillna(fillna_dict)
#     # replace "None" with pd.NA
#     for str_col in string_cols:
#         if df[str_col].notnull().sum() > 0:
#             df.loc[df[str_col].str.lower() == "none", str_col] = pd.NA

#     # If Province == Recoverd  -> Province = pd.NA
#     df.loc[df[province_col] == "Recovered", province_col] = pd.NA
#     # replace certain countries based on province string match
#     if df[province_col].notnull().sum() > 0:
#         province_country_replace_dict = {'Hong Kong': 'China', 'Macau': 'China', 'Taiwan': 'Taiwan', 'Grand Princess': 'Cruise Ship',
#                                          'Diamond Princess': 'Cruise Ship'
#                                          }
#         for key, value in province_country_replace_dict.items():
#             # print(key, value)
#             df.loc[df[province_col].fillna(
#                 '').str.contains(key), country_col] = value

#     string_col_replacement = {key: "None" for key in string_cols}
#     df = df.fillna(string_col_replacement)

#     # replace certain countries based on country string match
#     if df[country_col].notnull().sum() > 0:
#         country_country_replace_dict = {'Gambia': 'Gambia', 'Congo': 'Congo', 'China': 'China', 'Czech': 'Czechia',
#                                         'Dominica': 'Dominican Republic', 'UK': 'United Kingdom', 'Bahamas': 'Bahamas',
#                                         'US': 'United States', 'Korea, South': 'South Korea', 'Taiwan': 'Taiwan',
#                                         'Iran': 'Iran', 'Viet nam': 'Vietnam', 'Russia': 'Russia', 'Republic of Korea': 'South Korea',
#                                         'Diamond Princess': 'Cruise Ship', 'Grand Princess': 'Cruise Ship'
#                                         }
#         # This command should work, but somehow it does not
#         # df.loc[df[country_col].str.lower().str.contains(key), country_col] = value
#         # --> Stack Overflow
#         for key, value in country_country_replace_dict.items():
#             df.loc[df[country_col].str.contains(
#                 key, case=False), country_col] = value

#     df = df.groupby(by=key_cols).agg({'FIPS': 'max', 'Lat': 'max', 'Long': 'max',
#                                       'infections': 'sum', 'deaths': 'sum', 'recovered': 'sum'}).reset_index()
#     for col in string_cols:
#         df.loc[df[col].str.contains("None"), col] = pd.NA

#     return df
