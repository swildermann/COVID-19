import logging
import azure.functions as func
from urllib.request import urlopen
import json
import pyodbc
from datetime import date
import os

def main(mytimer: func.TimerRequest) -> None:
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    url = "https://opendata.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0.geojson"
    
    data = urlopen(url) # it's a file like object and works just like a file
    jsonObj = json.load(data)

    features = jsonObj["features"]

    username = os.environ.get('keyvault_db_username')
    password = os.environ.get('keyvault_db_password')

    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                        "Server=covid19dbserver.database.windows.net;"
                        "Database=covid19db;"
                        "Uid="+username   
                      +";Pwd="+password   
                      +";Trusted_Connection=no;")
                      
    cursor=cnxn.cursor()

    deleteQuery = "TRUNCATE TABLE RKICounties_updates;"
    cursor.execute(deleteQuery)
    cnxn.commit()

    for feature in features:
        properties = feature["properties"]
        print(properties)

        objectId = properties["OBJECTID"]
        print(objectId)

        county = properties["county"]
        print(county)
        
        countyName = properties["GEN"]
        print(countyName)

        countyType = properties["BEZ"]
        print(countyType)

        federalState = properties["BL"]
        print(federalState)

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

        population = properties["EWZ"]
        print(population)

        infections = properties["cases"]
        print(infections)

        deaths = properties["deaths"]
        print(deaths)

        shapearea = properties["Shape__Area"]
        print(shapearea)        

        shapelength = properties["Shape__Length"]
        print(shapelength)

        insertQuery = "INSERT INTO RKICounties_updates VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(county, countyName, countyType, federalState, population, infections, deaths, shapearea, shapelength, date.today())
        cursor.execute(insertQuery)
        cnxn.commit()

    mergeQuery = "MERGE INTO dbo.RKICounties AS Target USING (SELECT county, countyname, type, federalstate, population, infections, deaths, shapearea, shapelength, date FROM dbo.RKICounties_updates) AS Source ON Target.federalstate = Source.federalstate AND Target.county = Source.county AND Target.date = Source.date WHEN MATCHED THEN UPDATE SET Target.countyname = Source.countyname, Target.type = Source.type, Target.population = Source.population, Target.infections = Source.infections, Target.deaths = Source.deaths, Target.shapearea = Source.shapearea, Target.shapelength = Source.shapelength WHEN NOT MATCHED BY TARGET THEN INSERT (county, countyname, type, federalstate, population, infections, deaths, shapearea, shapelength, date) VALUES (Source.county, Source.countyname, Source.type, Source.federalstate, Source.population, Source.infections, Source.deaths, Source.shapearea, Source.shapelength, Source.date);"
    cursor.execute(mergeQuery)
    cnxn.commit()   
    