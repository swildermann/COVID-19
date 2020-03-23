import datetime 
import logging
import azure.functions as func
from urllib.request import urlopen
import json
import pyodbc
import os

def main(mytimer: func.TimerRequest) -> None:


    if mytimer.past_due:
        logging.info('The timer is past due!')

    url = "https://opendata.arcgis.com/datasets/ef4b445a53c1406892257fe63129a8ea_0.geojson"
    
    data = urlopen(url) # it's a file like object and works just like a file
    jsonObj = json.load(data)

    features = jsonObj["features"]

    username = os.environ.get('keyvault_db_username')
    password = os.environ.get('keyvault_db_password')

    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                      "Server=covid19dbserver.database.windows.net;"
                      "Database=covid19db;"
                      "Uid=%s;"   #removed
                      "Pwd=%s;"   #removed
                      "Trusted_Connection=no;"%(username, password))

    cursor=cnxn.cursor()

    deleteQuery = "TRUNCATE TABLE RKI_updates;"
    cursor.execute(deleteQuery)
    cnxn.commit()

    for feature in features:
        properties = feature["properties"]
        #print(properties)

        objectId = properties["OBJECTID_1"]
        print(objectId)
        
        federalState = properties["LAN_ew_GEN"]
        if federalState == "Bayern":
            federalState = "Bavaria"
        elif federalState == "Hessen":
            federalState = "Hesse"
        elif federalState == "Niedersachsen":
            federalState = "Lower Saxony"
        elif federalState == "Mecklenburg-Vorpommern":
            federalState = "Mecklenburg-Western Pomerania"
        elif federalState == "Nordrhein-Westfalen":
            federalState = "North Rhine-Westphalia"
        elif federalState == "Rheinland-Pfalz":
            federalState = "Rhineland-Palatinate"
        elif federalState == "Sachsen":
            federalState = "Saxony"
        elif federalState == "Sachsen-Anhalt":
            federalState = "Saxony-Anhalt"
        elif federalState == "Thüringen":
            federalState = "Thuringia"

        print(federalState)
        
        infections = properties["Fallzahl"]
        print(infections)
        
        death = properties["Death"]
        print(death)        
        
        insertQuery = "INSERT INTO RKI_updates VALUES('%s','%s','%s','%s')"%(federalState, infections, death, datetime.date.today())
        cursor.execute(insertQuery)
        cnxn.commit()
        
    mergeQuery = "MERGE INTO dbo.RKI AS Target  USING (SELECT federalstate, infections, deaths, date FROM dbo.RKI_updates) AS Source ON Target.federalstate = Source.federalstate AND Target.date = Source.date WHEN MATCHED THEN  UPDATE SET Target.infections = Source.infections, Target.deaths = Source.deaths WHEN NOT MATCHED BY TARGET THEN INSERT (federalstate, infections, deaths, date) VALUES (Source.federalstate, Source.infections, Source.deaths, Source.date);"
    cursor.execute(mergeQuery)
    cnxn.commit()


