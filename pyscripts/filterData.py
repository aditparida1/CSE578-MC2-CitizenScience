import pandas as pd
import os
import numpy as np
#import matplotlib
import matplotlib.pyplot as plt
import geojson
from shapely.geometry import shape, Point
import math
import pyproj

def readCSV(url):
    return pd.read_csv(url)

def fillMap(df, map, isMobile):
    sensoridLs = df.sensorid.unique()
    for id in sensoridLs:
        if(isMobile):
            map[id] = "MobileSensor" + str(id)
        else:
            map[id] = "StaticSensor" + str(id)

# def getMin(df):
#     min = -1
#     for index, row in df.iterrows():
#         if(row["val"] != 0):
#             if(min == -1):
#                 min = row["val"]
#             else:
#                 if(min > row["val"]):
#                     min = row["val"]
    # return min
def process(url):
    map = dict()
    df = readCSV(url)
    # print(df.head())

    if(url.find("MobileSensorReadings") != -1):
        df.columns = ["timestamp", "sensorid", "long", "lat", "val", "units", "userid"]
        fillMap(df, map, True)
    else:
        df.columns = ["timestamp", "sensorid", "val", "units"]
        fillMap(df, map, False)
    filepath = os.path.dirname( os.path.abspath(__file__))
    filepath = os.path.join(filepath, "./../data/processed").replace("\\", "/")
    
    if(not os.path.isdir(filepath)):
        # print(os.path.isdir(filepath))
        # print(filepath)
        os.mkdir(filepath)

    # max = df["val"].max()

    # min = df["val"].min()

    # print(max)
    # print(min)
    df["convLong"] = df["long"].apply(lon2x)
    df["convLat"] = df["lat"].apply(lat2y)
    conditions = [
    (df['val'] < 100),
    (df['val'] >= 100) & (df['val'] <= 150),
    (df['val'] > 150)
    ]

    df["level"] = np.select(conditions, ["low", "medium", "high"])
    #print(df[df["level"] == "low"])
    #df["level"] = "high" if df["val"] > secondAnchor else df["level"]
    # print(df[df["val"] < firstAnchor])
    # print(max)
    # print(min)
    # print(len(df))
    # a = list(np.arange(744000)) #3315711, 744000
    # df['idx'] = a
    # df.plot.scatter(x = 'idx', y = 'val', c = 'blue')
    # plt.show()
    #print(map)
    for key in map:
        fileName = map[key] + ".csv"
        childDf = df[df["sensorid"] == key]
        #print(df[df["sensorid"] == key])
        newPath = os.path.join(filepath, fileName).replace("\\", "/")
        childDf.to_csv(newPath, index = False, header = True)
    levels = ["low", "medium", "high"]
    for level in levels:
        fileName = level+ ".csv"
        childDf = df[df["level"] == level]
        newPath = os.path.join(filepath, fileName).replace("\\", "/")
        childDf.to_csv(newPath, index = False, header = True)
    df["date"] = df["timestamp"].apply(lambda x : pd.Timestamp.date(pd.Timestamp(x)))
    map = dict()
    if(url.find("MobileSensorReadings") != -1):
        fillMapDates(df, map, True)
    else:
        fillMapDates(df, map, False)

    for date in map:
        fileName = map[date] + ".csv"
        childDf = df[df['date'] == date]
        newPath = os.path.join(filepath, fileName).replace("\\", "/")
        childDf.to_csv(newPath, index = False, header = True)


def fillMapDates(df, map, isMobile):
    dates = df.date.unique()
    for date in dates:
        if(isMobile):
            map[date] = "Mobile" + str(date)
        else:
            map[date] = "Static" + str(date)

def geoJson(url, url2):
    data = None
    with open(url) as f:
        data = geojson.load(f)
    # print(data)
    features = data['features']
    # print(features)
    geoMap = dict()
    for obj in features:
        # print(obj)
        geo = obj["geometry"]
        properties = obj["properties"]
        #id = str(properties["Id"])
        geoMap[str(properties["Id"])] = geo
    # print(geoMap)
    #-119.83035,0.14007
    geoPoly = dict()
    for i in range(1,20):
        geoPoly[str(i)] = shape(geoMap[str(i)])
    # print(geoPoly)
    point = Point(-13339453.54,15592.54)
    for i in range(1, 20):
        #print(geoPoly[str(i)].contains(point))
        if(geoPoly[str(i)].contains(point)):
            print("Got it" + str(i))
            break
    df = readCSV(url2)
    map = dict()
    if(url2.find("MobileSensorReadings") != -1):
        df.columns = ["timestamp", "sensorid", "long", "lat", "val", "units", "userid"]
        fillMap(df, map, True)
    else:
        df.columns = ["timestamp", "sensorid", "val", "units"]
        fillMap(df, map, False)
    
    filepath = os.path.dirname( os.path.abspath(__file__))
    filepath = os.path.join(filepath, "./../data/processed").replace("\\", "/")
    
    if(not os.path.isdir(filepath)):
        os.mkdir(filepath)

    df["convLong"] = df["long"].apply(lon2x)
    df["convLat"] = df["lat"].apply(lat2y)
    # print(df.head())
    conditions = [
    (df['val'] < 100),
    (df['val'] >= 100) & (df['val'] <= 150),
    (df['val'] > 150)
    ]

    df["level"] = np.select(conditions, ["low", "medium", "high"])
    #df = df.head()    
    df["area"] = df.apply(lambda row: applyArea(row, geoPoly), axis = 1)

    print(df)

    for key in map:
        fileName = map[key] + ".csv"
        childDf = df[df["sensorid"] == key]
        #print(df[df["sensorid"] == key])
        newPath = os.path.join(filepath, fileName).replace("\\", "/")
        childDf.to_csv(newPath, index = False, header = True)
    levels = ["low", "medium", "high"]
    for level in levels:
        fileName = level+ ".csv"
        childDf = df[df["level"] == level]
        newPath = os.path.join(filepath, fileName).replace("\\", "/")
        childDf.to_csv(newPath, index = False, header = True)
    df["date"] = df["timestamp"].apply(lambda x : pd.Timestamp.date(pd.Timestamp(x)))
    map = dict()
    if(url2.find("MobileSensorReadings") != -1):
        fillMapDates(df, map, True)
    else:
        fillMapDates(df, map, False)

    for date in map:
        fileName = map[date] + ".csv"
        childDf = df[df['date'] == date]
        newPath = os.path.join(filepath, fileName).replace("\\", "/")
        childDf.to_csv(newPath, index = False, header = True)
    for i in range(1, 21):
        if(i == 20):
            fileName = "none.csv"
            childDf = df[df["area"] == "none"]
            newPath =  os.path.join(filepath, fileName).replace("\\", "/")
            childDf.to_csv(newPath, index = False, header = True)
        else:
            fileName = str(i) + ".csv"
            childDf = df[df["area"] == str(i)]
            newPath = os.path.join(filepath, fileName).replace("\\", "/")
            childDf.to_csv(newPath, index = False, header = True)

# derived from the Java version explained here: http://wiki.openstreetmap.org/wiki/Mercator
RADIUS = 6378137.0 # in meters on the equator

def lat2y(a):
  return math.log(math.tan(math.pi / 4 + math.radians(a) / 2)) * RADIUS

def lon2x(a):
  return math.radians(a) * RADIUS

def applyArea(row, geoPoly):
    for i in range(1, 20):
        #print(geoPoly[str(i)].contains(point))
        point = Point(row["convLong"], row["convLat"])
        if(geoPoly[str(i)].contains(point)):
            return str(i)
    return "none"

def geoJsonforStatic(url, url2):
    data = None
    with open(url) as f:
        data = geojson.load(f)
    # print(data)
    features = data['features']
    # print(features)
    geoMap = dict()
    for obj in features:
        # print(obj)
        geo = obj["geometry"]
        properties = obj["properties"]
        #id = str(properties["Id"])
        geoMap[str(properties["Id"])] = geo
    # print(geoMap)
    #-119.83035,0.14007
    geoPoly = dict()
    for i in range(1,20):
        geoPoly[str(i)] = shape(geoMap[str(i)])
    # print(geoPoly)
    point = Point(-13339453.54,15592.54)
    for i in range(1, 20):
        #print(geoPoly[str(i)].contains(point))
        if(geoPoly[str(i)].contains(point)):
            print("Got it" + str(i))
            break
    df = readCSV(url2)
    #map = dict()
    df.columns = ["sensorid", "lat", "long"]
    df["convLong"] = df["long"].apply(lon2x)
    df["convLat"] = df["lat"].apply(lat2y)
    df["area"] = df.apply(lambda row: applyArea(row, geoPoly), axis = 1)
    print(df)
    
    filepath = os.path.dirname( os.path.abspath(__file__))
    filepath = os.path.join(filepath, "./../data/processed").replace("\\", "/")
    
    if(not os.path.isdir(filepath)):
        os.mkdir(filepath)
    fileName = "StaticSensorLocationsAreaUpdated.csv"
    newPath =  os.path.join(filepath, fileName).replace("\\", "/")
    df.to_csv(newPath, index = False, header = True)

def sankeyParse(url, url2):
    data = None
    with open(url) as f:
        data = geojson.load(f)
    # print(data)
    features = data['features']
    # print(features)
    geoMap = dict()
    for obj in features:
        # print(obj)
        geo = obj["geometry"]
        properties = obj["properties"]
        #id = str(properties["Id"])
        geoMap[str(properties["Id"])] = geo
    geoPoly = dict()
    for i in range(1,20):
        geoPoly[str(i)] = shape(geoMap[str(i)])
    # print(geoPoly)
    # point = Point(-13339453.54,15592.54)
    # for i in range(1, 20):
    #     #print(geoPoly[str(i)].contains(point))
    #     if(geoPoly[str(i)].contains(point)):
    #         print("Got it" + str(i))
    #         break
    df = readCSV(url2)
    map = dict()
    if(url2.find("MobileSensorReadings") != -1):
        df.columns = ["timestamp", "sensorid", "long", "lat", "val", "units", "userid"]
        fillMap(df, map, True)
    else:
        df.columns = ["timestamp", "sensorid", "val", "units"]
        fillMap(df, map, False)
    filepath = os.path.dirname( os.path.abspath(__file__))
    filepath = os.path.join(filepath, "./../data/sankeydata").replace("\\", "/")
    if(not os.path.isdir(filepath)):
        os.mkdir(filepath)
    df["convLong"] = df["long"].apply(lon2x)
    df["convLat"] = df["lat"].apply(lat2y)
    conditions = [(df['val'] < 100), (df['val'] >= 100) & (df['val'] <= 150), (df['val'] > 150)]
    df["level"] = np.select(conditions, ["low", "medium", "high"])
    df["area"] = df.apply(lambda row: applyArea(row, geoPoly), axis = 1)
    df["date"] = df["timestamp"].apply(lambda x : pd.Timestamp.date(pd.Timestamp(x)))
    map = dict()
    if(url2.find("MobileSensorReadings") != -1):
        fillMapDates(df, map, True)
    else:
        fillMapDates(df, map, False)
    fileName = 'MobileData.csv'
    newPath = os.path.join(filepath, fileName).replace("\\", "/")
    df.to_csv(newPath, index = False, header = True)
def sankeyFilter2(url):
    df = pd.read_csv(url, low_memory=False)
    dates = df.date.unique()
    dateSeparated=[]
    for date in dates:
        childDf = df[df["date"] == date]
        dateSeparated.append(childDf)
    for i in range(len(dateSeparated)):
        currDf = dateSeparated[i]
        print("Date is : ")
        print(currDf.iloc[0,11])
        for areaId in range(1, 20):
            childDf = currDf[currDf['area'] == str(areaId)]
            lowDf = childDf[childDf['level'] == 'low']
            medDf = childDf[childDf['level'] == 'medium']
            highDf = childDf[childDf['level'] == 'high']
            
            print("Area is: " + str(areaId))
            print(lowDf.shape)
            print(medDf.shape)
            print(highDf.shape)


def sankeyFilter(url):
    df = pd.read_csv(url, low_memory=False)
    print(df.shape)
    # count = 0
    area = []
    for i in range(1, 20):
        childDf = df[df["area"] == str(i)]
        # print("Shape of " + str(i))
        # print(childDf.shape)
        area.append(childDf)
    
    # childDf = df[df["area"] == "1"]
    # filepath = os.path.dirname( os.path.abspath(__file__))
    # filepath = os.path.join(filepath, "./../data/sankeydata").replace("\\", "/")
    # fileName = "test.csv"
    # newPath =  os.path.join(filepath, fileName).replace("\\", "/")
    # childDf.to_csv(newPath, index = False, header = True)
    datesDist = [[] for i in range(len(area))]

    dates = df.date.unique()
    count = 0
    for i in range(len(area)):
        dates = area[i].date.unique()
        currDf = area[i]
        count = 0
        for date in dates:
            childDf = currDf[currDf["date"] == date]
            datesDist[i].append(childDf)
            # count = 0
        #     if i == 13:
        #         print(childDf.head())
        #         print(childDf.shape)
        #         count += childDf.shape[0]
        # if i == 13:
        #     print(count)
    # for i in range(len(datesDist)):
    #     currLs = datesDist[i]
    #     for j in range(len(currLs)):
    #         print(currLs[j].head())
    #         print("Shape: ")
    #         print(currLs[j].shape)
    #         pass
    #     print("_______NEXT________")
    # print(len(area))

    areaOnlyDf = df[df['area'] != 'none']
    # print(areaOnlyDf.shape)
    dates = areaOnlyDf.date.unique()
    levels = ["low", "medium", "high"]
    count = 0
    for date in dates:
        currDf = areaOnlyDf[areaOnlyDf["date"] == date]
        for level in levels:
            childDf = currDf[currDf['level'] == level]
            print(childDf.head())
            print(childDf.shape)
            count += childDf.shape[0]
    print(count)

def geoJson2(url, url2):
    data = None
    with open(url) as f:
        data = geojson.load(f)
    # print(data)
    features = data['features']
    # print(features)
    geoMap = dict()
    for obj in features:
        # print(obj)
        geo = obj["geometry"]
        properties = obj["properties"]
        #id = str(properties["Id"])
        geoMap[str(properties["Id"])] = geo
    # print(geoMap)
    #-119.83035,0.14007
    geoPoly = dict()
    for i in range(1,20):
        geoPoly[str(i)] = shape(geoMap[str(i)])
    # print(geoPoly)
    point = Point(-13339453.54,15592.54)
    # for i in range(1, 20):
    #     #print(geoPoly[str(i)].contains(point))
    #     if(geoPoly[str(i)].contains(point)):
    #         print("Got it" + str(i))
    #         break
    df = readCSV(url2)
    map = dict()
    if(url2.find("MobileSensorReadings") != -1):
        df.columns = ["timestamp", "sensorid", "long", "lat", "val", "units", "userid"]
        fillMap(df, map, True)
    else:
        df.columns = ["timestamp", "sensorid", "val", "units"]
        fillMap(df, map, False)
    
    filepath = os.path.dirname( os.path.abspath(__file__))
    filepath = os.path.join(filepath, "./../data/processed").replace("\\", "/")
    
    if(not os.path.isdir(filepath)):
        os.mkdir(filepath)

    df["convLong"] = df["long"].apply(lon2x)
    df["convLat"] = df["lat"].apply(lat2y)
    # print(df.head())
    conditions = [
    (df['val'] < 100),
    (df['val'] >= 100) & (df['val'] <= 150),
    (df['val'] > 150)
    ]

    df["level"] = np.select(conditions, ["low", "medium", "high"])
    #df = df.head()    
    df["area"] = df.apply(lambda row: applyArea(row, geoPoly), axis = 1)

    print(df)

    # for key in map:
    #     fileName = map[key] + ".csv"
    #     childDf = df[df["sensorid"] == key]
    #     #print(df[df["sensorid"] == key])
    #     newPath = os.path.join(filepath, fileName).replace("\\", "/")
    #     childDf.to_csv(newPath, index = False, header = True)
    # levels = ["low", "medium", "high"]
    # for level in levels:
    #     fileName = level+ ".csv"
    #     childDf = df[df["level"] == level]
    #     newPath = os.path.join(filepath, fileName).replace("\\", "/")
    #     childDf.to_csv(newPath, index = False, header = True)
    df["date"] = df["timestamp"].apply(lambda x : pd.Timestamp.date(pd.Timestamp(x)))
    map = dict()
    if(url2.find("MobileSensorReadings") != -1):
        fillMapDates(df, map, True)
    else:
        fillMapDates(df, map, False)

    # for date in map:
    #     fileName = map[date] + ".csv"
    #     childDf = df[df['date'] == date]
    #     newPath = os.path.join(filepath, fileName).replace("\\", "/")
    #     childDf.to_csv(newPath, index = False, header = True)
    for i in range(1, 21):
        if(i == 20):
            fileName = "none.csv"
            childDf = df[df["area"] == "none"]
            # newPath =  os.path.join(filepath, fileName).replace("\\", "/")
            # childDf.to_csv(newPath, index = False, header = True)
        else:
            fileName = str(i) + ".csv"
            childDf = df[df["area"] == str(i)]
            # newPath = os.path.join(filepath, fileName).replace("\\", "/")
            # childDf.to_csv(newPath, index = False, header = True)
            print(str(i) + "Shape")
            print(childDf.shape)

def sankeyParseStatic(url1):
    df = pd.read_csv(url1, low_memory=False)
    #locDf = pd.read_csv(url2, low_memory=False)
    map = dict()
    map["12"] = "3"
    map["13"] = "4"
    map["15"] = "4"
    map["11"] = "9"
    map["6"] = "5"
    map["1"] = "1"
    map["9"] = "3"
    map["14"] = "13"
    map["4"] = "6"
    df.columns = ["timestamp", "sensorid", "val", "units"]
    df["date"] = df["timestamp"].apply(lambda x : pd.Timestamp.date(pd.Timestamp(x)))
    df["area"] = df.apply(lambda row: applyAreaStatic(map, row), axis = 1)
    print(df.sensorid.unique())
    print(df.head())
    filepath = os.path.dirname( os.path.abspath(__file__))
    filepath = os.path.join(filepath, "./../data/sankeydata").replace("\\", "/")
    if(not os.path.isdir(filepath)):
        os.mkdir(filepath)
    fileName = 'StaticData.csv'
    newPath = os.path.join(filepath, fileName).replace("\\", "/")
    df.to_csv(newPath, index = False, header = True)
    pass


def applyAreaStatic(map, row):
    return map[str(row["sensorid"])]
def sankeyFilterStatic2(url):
    df = pd.read_csv(url, low_memory=False)
    conditions = [(df['val'] < 100), (df['val'] >= 100) & (df['val'] <= 150), (df['val'] > 150)]
    df["level"] = np.select(conditions, ["low", "medium", "high"])
    areas = df.area.unique()
    areas = sorted(areas)
    dates = df.date.unique()
    dateSorted = []
    for date in dates:
        childDf = df[df['date'] == date]
        dateSorted.append(childDf)
    for i in range(len(dateSorted)):
        currDf = dateSorted[i]
        currDf = dateSorted[i]
        print("Date is : ")
        print(currDf.iloc[0,4])
        for j in range(len(areas)):
            area = str(areas[j])
            childDf = currDf[currDf['area'] == int(area)]
            lowDf = childDf[childDf['level'] == 'low']
            medDf = childDf[childDf['level'] == 'medium']
            highDf = childDf[childDf['level'] == 'high']
            print("Area is: " + area)
            print(lowDf.shape)
            print(medDf.shape)
            print(highDf.shape)
def sankeyFilterStatic(url):
    df = pd.read_csv(url, low_memory=False)
    areas = df.area.unique()
    areas = sorted(areas)
    # print(df.head())
    areasDf = []
    for area in areas:
        childDf = df[df["area"] == area]
        areasDf.append(childDf)
        # print("Shape of" + str(area))
        # print(childDf.shape)
    datesDist = [[] for i in range(len(areasDf))]
    dates = df.date.unique()
    
    for i in range(len(areasDf)):
        dates = areasDf[i].date.unique()
        currDf = areasDf[i]        
        for date in dates:
            childDf = currDf[currDf["date"] == date]
            datesDist[i].append(childDf)
    conditions = [(df['val'] < 100), (df['val'] >= 100) & (df['val'] <= 150), (df['val'] > 150)]
    df["level"] = np.select(conditions, ["low", "medium", "high"])
    # for i in range(len(datesDist)):
    #     currDf = datesDist[i]
    #     for j in range(len(currDf)):
    #         print(currDf[j].head())
    #         print(currDf[j].shape)
    #         print("__NEXT__")
    dates = df.date.unique()
    dateDf = []
    for date in dates:
        childDf = df[df['date'] == date]
        dateDf.append(childDf)
    levels = ['low', 'medium', 'high']
    count = 0
    for i in range(len(dateDf)):
        currDf = dateDf[i]
        for level in levels:
            childDf = currDf[currDf['level'] == level]
            print(childDf.head())
            print(childDf.shape)
            count += childDf.shape[0]
        print("__NEXT__")
    print(count)
        


def main():
    # process("./../data/MobileSensorReadings.csv")
    # geoJson2("./../data/map.geojson", "./../data/MobileSensorReadings.csv")
    #print('latitude web mercator y: {} longitude web mercator x: {}'.format(lat2y(0.14007 ), lon2x(-119.83035)))
    # geoJsonforStatic("./../data/map.geojson", "./../data/StaticSensorLocations.csv")
    # sankeyParse("./../data/map.geojson", "./../data/MobileSensorReadings.csv")
    # sankeyFilter("./../data/sankeydata/MobileData.csv")
    # sankeyParseStatic("./../data/StaticSensorReadings.csv")
    #sankeyFilterStatic('./../data/sankeydata/StaticData.csv')
    #sankeyFilter2("./../data/sankeydata/MobileData.csv")
    sankeyFilterStatic2('./../data/sankeydata/StaticData.csv')

if __name__ == "__main__":
    main()