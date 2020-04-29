import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine
import urllib
import numpy as np
from scipy.optimize import curve_fit
import sqlalchemy

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')




    params = urllib.parse.quote_plus(
        r'Driver={ODBC Driver 17 for SQL Server};Server=tcp:covid19dbserver.database.windows.net,1433;Database=covid19db;Uid=serveradmin@covid19dbserver;Pwd=pzaGuPujnkUnDqZFbWt5;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine = create_engine(conn_str, echo=False)
    key_cols_candidates = ['Country/Region',
                           'Province/State', 'District', 'federalstate', 'date']

    training_period = 21
    forecasting_days = 3
    forecast_col = 'infections'

    for table_name in ['Hopkins', 'ECDC', 'HopkinsTS', 'RKI']:
        logging.info(f"Processing table {table_name}...")
        # for table_name in ['RKI']:
        df = pd.read_sql_table(table_name, engine)
        df = df.drop(['Province/State', 'District', 'FIPS', 'Lat',
                      'Long', 'deaths', 'recovered', 'ID'], axis=1, errors='ignore')
        string_cols = df.dtypes[df.dtypes == 'object'].index
        string_col_replacement = {key: "None" for key in string_cols}
        df = df.fillna(string_col_replacement)
        key_cols = [col for col in key_cols_candidates if col in df.columns]
        df = df.groupby(by=key_cols).sum().reset_index()
        for col in string_cols:
            if col in df.columns:
                df.loc[df[col].str.contains("None"), col] = pd.NA

        country_col = 'Country/Region'
        if table_name == 'RKI':
            country_col = 'federalstate'
        for country in df[country_col].unique():
            logging.debug(f"Computing forecasts for country {country}")
            df_country = df[df[country_col] == country]
            y = df_country.sort_values(
                by='date')[forecast_col].values[-training_period:]
            if (y < 10).all():
                # All values < 10. No good forecast possible
                continue
            if len(y) < training_period:
                # Did not find a lot of datapoints
                continue
            x = range(len(y))
            x_forecast = range(len(y), len(y)+forecasting_days)
            try:
                (a_scipy, b_scipy), _ = curve_fit(
                    lambda t, a, b: a*np.exp(b*t),  x,  y)
            except Exception as e:
                logging.info(table_name, country, y)
                logging.error(e)
                continue

            def exp_scipy(x): return a_scipy * np.exp(b_scipy*x)
            y_forecast = exp_scipy(x_forecast)
            df_result = pd.DataFrame()
            df_result['forecast_infections'] = y_forecast
            one_day_delta = pd.Timedelta(value=1, unit='d')
            df_result['date'] = pd.date_range(
                start=df_country.date.max()+one_day_delta, periods=forecasting_days, freq='d')
            df_result['forecast_infections'] = y_forecast
            df_result[country_col] = country
            today = datetime.datetime.now()
            df_result['forecasting_date'] = today

            dtype_dict = {}
            for str_col in string_cols:
                if (str_col in df_result.columns and df_result[str_col].notnull().sum() > 0):
                    # print(col)
                    df_result.loc[df_result[str_col].notnull(
                    ), str_col] = df_result.loc[df_result[str_col].notnull(), str_col].str.slice(start=0, stop=99)
                    dtype_dict[str_col] = sqlalchemy.types.NVARCHAR(length=100)
            logging.debug("Computed forecasts.")
            logging.debug("Writing forecast to database...")
            
            df_result.to_sql(f"{table_name}_forecast", engine,
                             if_exists='append', index=False, dtype=dtype_dict)
            logging.debug("Wrote forecast to database.")
            # TODO: Write merge statement to update into f{table_name}_forecast or just run once a day

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
