# This small Azure functions updates the database on a regular basis



## How it works

It takes this dataset 
'''
https://opendata.arcgis.com/datasets/ef4b445a53c1406892257fe63129a8ea_0.geojson
'''
and merges all the new information into our database. 

All the magic can be found in this file:
'''
__init__.py
''' 

