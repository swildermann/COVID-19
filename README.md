# COVID-19
This is a public accessible dataset for COVID-19. We made the data for Germany from the Robert Koch Insititute and worldwide data from Johns Hopkins CSSE and European Centre for Disease Prevention available on a daily base with daily updates.

## Robert Koch Institute (RKI) - dataset for Germany
As the Robert Koch Institute does not publish data in a computer consumable format, we decided to grab the information from their website and make this dataset public available. 

This dataset is going to be updated daily, as soon as the information from the Robert Koch Institute is available. 

Source: https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Situationsberichte/Archiv.html

Source: https://npgeo-corona-npgeo-de.hub.arcgis.com/datasets/917fc37a709542548cc3be077a786c17_0/data?selectedAttribute=GEN

### use the provided csv file
Just download the files from here and use them as they are. We are providing a CSV format. 

https://covid19publicdata.blob.core.windows.net/rki/covid19-germany-federalstates.csv
https://covid19publicdata.blob.core.windows.net/rki/covid19-germany-counties.csv
https://covid19publicdata.blob.core.windows.net/rki/covid19-germany-counties-nuts3.csv (joined with NUTS3 data)

The URL will stay the same even when data is updated. So feel free to grab this url in an automated manner. 

## Johns Hopkins CSSE - dataset worldwide
We also provide the global data from Johns Hopkins CSSE in this database. This dataset is going to be updated daily, as soon as the information is available.

Source: https://github.com/CSSEGISandData/COVID-19

### use the provided csv file
Just download the csv from here and use them as they are. We are providing a CSV format.

https://covid19publicdata.blob.core.windows.net/hopkins/covid19-hopkins.csv

Or use the Database (see below).

## European Centre for Disease Prevention and Control (ECDC) - dataset worldwide
Data of European Centre for Disease Prevention and Control is also available here. This dataset is going to be updated daily, as soon as the information is available.

Source: https://www.ecdc.europa.eu/en/coronavirus

### use the provided csv file
Just download the csv from here and use them as they are. We are providing a CSV format.

https://covid19publicdata.blob.core.windows.net/ecdc/covid19-ECDC.csv

Or use the Database (see below).

## Image data collection
If you are interested in Covid-19 image data, please have a look here: https://github.com/ieee8023/covid-chestxray-dataset
This dataset provides cases with chest X-ray or CT images for COVID-19 cases as well as MERS, SARS, and ARDS.

## Database Access
If you would like to connect to our database, feel free to use this information for login:
```
Server: covid19dbserver.database.windows.net
Authentication type: SQL Login
username: datareader
password: eg4?%bKrY.T#SpBhEBk8DmH9
database: covid19db
tables: [dbo].[RKI], [dbo].[RKICounties], [dbo].[Hopkins], [dbo].[HopkinsTS], [dbo].[ECDC]
views: [dbo].[vRKI], [dbo].[vRKICounties], [dbo].[vHopkins], [dbo].[vHopkinsTS], [dbo].[vECDC]
```

## Reports
We also provide some reports we created on top of this data set.

Link to the reports: https://lnkd.in/dH2uvh5 

![alt text](https://github.com/swildermann/COVID-19/blob/master/img/Hopkins_Dashboard.jpg)

You can also find the original Power BI file in the repo: https://github.com/swildermann/COVID-19/tree/master/reporting

## Changelog
- 2020-03-27 Johns Hopkins CSSE changed the time series exports. They provide again time series data for recovered cases. 
- 2020-03-27 Bugfix in [dbo].[HopkinsTS] (for UK and US data).
- 2020-03-26 We now provide two different tables for the Johns Hopkins CSSE data. The table "Hopkins" is loaded based on the daily reports. The table HopkinsTS is loaded based on the time series export data.
- 2020-03-23 Johns Hopkins CSSE changed the time series exports. They now only provide time series data for infections and deaths.
- 2020-03-22 Added NUTS3 dataset for Germany
- 2020-03-17 RKI reporting changed. As of today, epidemiological evaluations in the COVID-19 situation report are based only on electronically transmitted data available to the Robert Koch Institute (RKI) at the time of the data closure (11 pm).
