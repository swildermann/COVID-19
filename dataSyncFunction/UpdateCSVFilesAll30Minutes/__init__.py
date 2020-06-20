import logging
import azure.functions as func
import datetime
import pyodbc
import csv
from azure.storage.blob import BlockBlobService
import os
import tempfile

def main(mytimer: func.TimerRequest) -> None:

    if mytimer.past_due:
        logging.info('The timer is past due!')

    username = os.environ.get('keyvault_db_username')
    password = os.environ.get('keyvault_db_password')
    blobaccountname = os.environ.get('blobaccountname')
    blob_account_key = os.environ.get('blobaccountkey')

    block_blob_service = BlockBlobService(
        account_name=blobaccountname, account_key=blob_account_key)

    tempDir = tempfile.gettempdir()

    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                          "Server=covid19dbserver.database.windows.net;"
                          "Database=covid19db;"
                          "Uid="+username
                          + ";Pwd="+password
                          + ";Trusted_Connection=no;")

    cursor = cnxn.cursor()
    # FEDERALVIEW
    selectQuery = "SELECT * FROM vRKI ORDER BY [date],[federalstate];"
    rows = cursor.execute(selectQuery)
    fileName = 'covid19-germany-federalstates.csv'
    tempFile = os.path.join(tempDir, fileName)
    with open(tempFile, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([x[0] for x in cursor.description])  # column headers
        for row in rows:
            writer.writerow(row)

    block_blob_service.create_blob_from_path(
        'rki', fileName, tempFile)

    # COUNTIES VIEW
    selectQueryCounties = "SELECT * FROM vRKICounties ORDER BY [date], [county];"
    rows = cursor.execute(selectQueryCounties)

    fileName = 'covid19-germany-counties.csv'
    tempFile = os.path.join(tempDir, fileName)
    with open(tempFile, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([x[0] for x in cursor.description])  # column headers
        for row in rows:
            writer.writerow(row)

    block_blob_service.create_blob_from_path(
        'rki', fileName, tempFile)

    # HOPKINS VIEW
    selectQueryHopkins = "SELECT * FROM vHopkins ORDER BY [date], [Country/Region];"
    rows = cursor.execute(selectQueryHopkins)

    fileName = 'covid19-hopkins.csv'
    tempFile = os.path.join(tempDir, fileName)

    with open(tempFile, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([x[0] for x in cursor.description])  # column headers
        for row in rows:
            writer.writerow(row)

    block_blob_service.create_blob_from_path(
        'hopkins', fileName, tempFile)

    # ECDC VIEW
    selectQueryECDC = "SELECT * FROM vECDC ORDER BY [date],[Country/Region];"
    rows = cursor.execute(selectQueryECDC)

    fileName = 'covid19-ECDC.csv'
    tempFile = os.path.join(tempDir, fileName)

    with open(tempFile, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([x[0] for x in cursor.description])  # column headers
        for row in rows:
            writer.writerow(row)

    block_blob_service.create_blob_from_path(
        'ecdc', fileName, tempFile)

    # RKI Counties Nuts View
    selectQueryRKICountiesNuts = "SELECT * FROM vRKICountiesNUTS ORDER BY  [date], [NUTS3];"
    rows = cursor.execute(selectQueryRKICountiesNuts)

    fileName = 'covid19-germany-counties-nuts3.csv'
    tempFile = os.path.join(tempDir, fileName)

    with open(tempFile, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([x[0] for x in cursor.description])  # column headers
        for row in rows:
            writer.writerow(row)

    block_blob_service.create_blob_from_path(
        'rki', fileName, tempFile)
