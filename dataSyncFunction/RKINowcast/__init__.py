import datetime
import logging
import pandas as pd
from sqlalchemy.sql import text as sa_text
import urllib
from sqlalchemy import create_engine
import os
import requests

import azure.functions as func

import io

# sys.path.append(path.dirname(path.dirname(__file__)))

# (explicit relative)
#from ..shared import cleanup_df, country_col, province_col, district_col, date_col
from .. import shared


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    url = "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Projekte_RKI/Nowcasting_Zahlen.xlsx?__blob=publicationFile"
    req = requests.get(url, stream=True)
    df_result = pd.read_excel(io.BytesIO(req.content), sheet_name='Nowcast_R')
    df_result.columns = ["date", "infections_wo_smoothing", "infections_wo_smoothing_lb_95", "infections_wo_smoothing_ub_95",
                         "infections", "infections_lb_95", "infections_ub_95",
                         "R", "R_lb_95", "R_ub_95", "R_7d", "R_7d_lb_95", "R_7d_ub_95"]
    df_result['date'] = pd.to_datetime(df_result['date'])

    username = os.environ.get('keyvault_db_username')
    password = os.environ.get('keyvault_db_password')

    params = urllib.parse.quote_plus(
        'Driver={ODBC Driver 17 for SQL Server};Server=tcp:covid19dbserver.database.windows.net,1433;Database=covid19db;Uid='+username
        + '@covid19dbserver;Pwd='+password
        + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=False)

    assert df_result.duplicated().sum() == 0

    table_name = "RKINowcast"
    table_name_updates = f"{table_name}_updates"

    try:
        _ = pd.read_sql(
            f"select Top(1) * from dbo.{table_name_updates}", engine)
        engine.execute(sa_text(
            f'TRUNCATE TABLE {table_name_updates}').execution_options(autocommit=True))
    except Exception as e:
        print(e)
        pass

    df_result.to_sql(table_name_updates,
                     engine,
                     if_exists='append', schema='dbo',
                     index=False, chunksize=100,
                     method='multi')

    merge_statement = f'''
    MERGE INTO dbo.{table_name} AS Target 
    USING (SELECT date, infections_wo_smoothing, infections_wo_smoothing_lb_95, infections_wo_smoothing_ub_95,
                    infections, infections_lb_95, infections_ub_95,
                    R, R_lb_95, R_ub_95, R_7d, R_7d_lb_95, R_7d_ub_95
    FROM dbo.{table_name_updates}) AS Source
    ON Target.date = Source.date 
    WHEN MATCHED THEN 
    UPDATE SET Target.infections_wo_smoothing = Source.infections_wo_smoothing, 
    Target.infections_wo_smoothing_lb_95 = Source.infections_wo_smoothing_lb_95, 
    Target.infections_wo_smoothing_ub_95 = Source.infections_wo_smoothing_ub_95, 
    Target.infections = Source.infections, 
    Target.infections_lb_95 = Source.infections_lb_95, 
    Target.infections_ub_95 = Source.infections_ub_95, 
    Target.R = Source.R, 
    Target.R_lb_95 = Source.R_lb_95, 
    Target.R_ub_95 = Source.R_ub_95, 
    Target.R_7d = Source.R_7d, 
    Target.R_7d_lb_95 = Source.R_7d_lb_95, 
    Target.R_7d_ub_95 = Source.R_7d_ub_95 
    WHEN NOT MATCHED BY TARGET THEN 
    INSERT (date, infections_wo_smoothing, infections_wo_smoothing_lb_95, infections_wo_smoothing_ub_95,
                    infections, infections_lb_95, infections_ub_95,
                    R, R_lb_95, R_ub_95, R_7d, R_7d_lb_95, R_7d_ub_95) 
    VALUES (Source.date, Source.infections_wo_smoothing, Source.infections_wo_smoothing_lb_95, Source.infections_wo_smoothing_ub_95,
                    Source.infections, Source.infections_lb_95, Source.infections_ub_95,
                    Source.R, Source.R_lb_95, Source.R_ub_95, Source.R_7d, Source.R_7d_lb_95, Source.R_7d_ub_95);
    '''

    engine.execute(sa_text(merge_statement).execution_options(autocommit=True))

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
