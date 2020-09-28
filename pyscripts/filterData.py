import pandas as pd
import os


def readCSV(url):
    return pd.read_csv(url)

def fillMap(df, map):
    sensoridLs = df.sensorid.unique()
    for id in sensoridLs:
        map[id] = "sensor" + str(id)
    

def process(url):
    map = dict()
    df = readCSV(url)
    df.columns = ["timestamp", "sensorid", "long", "lat", "val", "units", "userid"]
    fillMap(df, map)
    filepath = os.path.dirname( os.path.abspath(__file__))
    filepath = os.path.join(filepath, "./../data/processed").replace("\\", "/")
    if(not os.path.isdir(filepath)):
        # print(os.path.isdir(filepath))
        # print(filepath)
        os.mkdir(filepath)
    




def main():
    process("./../data/MobileSensorReadings.csv")


if __name__ == "__main__":
    
    main()

