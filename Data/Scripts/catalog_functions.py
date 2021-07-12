import geopandas
import pandas


import os
#wgs84 all data



def catog(name):
    name2 = os.path.basename(os.path.normpath(name))
    name2 = name2.split(".")[0]
    arrname = name2.split("-")
    if "stop" and "search" in arrname:
        return handleSearch(name)
    if "outcomes" in arrname:
        return handleOutcome(name)
    if "street" in arrname:
        return handleStreet(name)



def handleSearch(fp):
    df = pandas.read_csv(fp, parse_dates=['Date'])
    df = df.drop(["Part of a policing operation", "Policing operation","Legislation"], axis=1)

    gdf = toGDF(df)
    return gdf

def handleOutcome(fp):
    df = pandas.read_csv(fp, parse_dates=['Month'])

    #df["Month"] = df["Month"].apply(lambda x: x+"-01")
    #df["Month"] = pandas.to_datetime(df["Month"], format='%Y-%m-%d')
    gdf = toGDF(df)
    return gdf

def handleStreet(fp):
    df = pandas.read_csv(fp, parse_dates=['Month'])
    df = df.drop(["Context"], axis=1)
    gdf = toGDF(df)
    return gdf


def toGDF(df):
    df = df[df["Longitude"].notna()]

    gdf = geopandas.GeoDataFrame(
        df, crs="EPSG:4326", geometry=geopandas.points_from_xy(df.Longitude, df.Latitude))

    return gdf
