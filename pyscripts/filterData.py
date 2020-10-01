import pandas as pd
import os
import numpy as np
#import matplotlib
import matplotlib.pyplot as plt
import geojson

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

def geoJson(url):
    with open(url) as f:
        data = geojson.load(f)
    print(data)

def main():
    #process("./../data/StaticSensorReadings.csv")
    geoJson("./../data/map.geojson")


if __name__ == "__main__":
    main()

