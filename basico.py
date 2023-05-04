from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
import sys
import datetime

def mapper(line):
    data = json.loads(line)
    u_t = data['user_type']
    u_c = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    time = data['travel_time']
    date = datetime.datetime.fromisoformat(data['unplug_hourTime']["$date"][:-5])
    return u_t, u_c, start, end, time, date

def main(data):
    with SparkContext() as sc:
        spark = SparkSession.builder.getOrCreate()
        rdd_base = sc.textFile(data)
        rdd = rdd_base.map(mapper)
        print(rdd.take(3))
    
    
# if __name__=='__main__':
#     if len(sys.argv)>1:
#         data = sys.argv[1]
#     main(data)