import pandas as pd
import os
import numpy as np
#import matplotlib
import matplotlib.pyplot as plt

def readCSV(url):
    return pd.read_csv(url)

def fillMap(df, map, isMobile):
    sensoridLs = df.sensorid.unique()
    for id in sensoridLs:
        if(isMobile):
            map[id] = "MobileSensor" + str(id)
        else:
            map[id] = "StaticSensor" + str(id)
    
def getMin(df):
    min = -1
    for index, row in df.iterrows():
        if(row["val"] != 0):
            if(min == -1):
                min = row["val"]
            else:
                if(min > row["val"]):
                    min = row["val"]
    return min
def process(url):
    map = dict()
    df = readCSV(url)
    df.columns = ["timestamp", "sensorid", "long", "lat", "val", "units", "userid"]
    if(url.find("MobileSensorReadings")):
        fillMap(df, map, True)
    else:
        fillMap(df, map, False)
    
    filepath = os.path.dirname( os.path.abspath(__file__))
    filepath = os.path.join(filepath, "./../data/processed").replace("\\", "/")
    if(not os.path.isdir(filepath)):
        # print(os.path.isdir(filepath))
        # print(filepath)
        os.mkdir(filepath)
    
    #max = df[df["val"] < 4000 and df["val"] > 1000]["val"].max()
    # print(len(df[df["val"] < 4000 and df["val"] > 1000]))
    # min = df[df["val"] != 0]["val"].min()
    # range = max - min
    # sec = range / 3
    # firstAnchor = sec
    # secondAnchor = sec + firstAnchor
    # print(firstAnchor)
    # print(secondAnchor)
    conditions = [
    (df['val'] < 100),
    (df['val'] >= 100) & (df['val'] <= 150),
    (df['val'] > 150)
    ]

    df["level"] = np.select(conditions, ["low", "medium", "high"])
    print(df[df["level"] == "low"])
    #df["level"] = "high" if df["val"] > secondAnchor else df["level"]
    # print(df[df["val"] < firstAnchor])
    # print(max)
    # print(min)
    # print(len(df))
    # a = list(np.arange(3315711))
    # df['idx'] = a
    # df.plot.scatter(x = 'idx', y = 'val', c = 'blue')
    # plt.show()
    #print(map)
    # for key in map:
    #     fileName = map[key] + ".csv"
    #     childDf = df[df["sensorid"] == key]
    #     #print(df[df["sensorid"] == key])
    #     newPath = os.path.join(filepath, fileName).replace("\\", "/")
    #     childDf.to
        

    




def main():
    process("./../data/MobileSensorReadings.csv")


if __name__ == "__main__":
    
    main()

