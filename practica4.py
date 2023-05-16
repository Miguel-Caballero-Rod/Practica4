from pyspark import SparkContext
# from pyspark.sql import SparkSession
import json
import sys
import datetime


def mapper_date_time(line):
    data = json.loads(line)
    time = data['travel_time']
    date_total = datetime.datetime.fromisoformat(data['unplug_hourTime']["$date"][:-5])
    date=(date_total.day,date_total.month,date_total.year)
    return date,time


def mapper_stations(line):
    data = json.loads(line)
    start = data['idunplug_station']
    end = data['idplug_station']
    return (min(start,end), max(start,end)),1


def mapper_start_end(line):
    data = json.loads(line)
    start = data['idunplug_station']
    end = data['idplug_station']
    return start,end

def takeMax(x,y):
    if x[1]>y[1]:
        return x
    else:
        return y

def takeMin(x,y):
    if x[1]<y[1]:
        return x
    else:
        return y

def main(data):
    with SparkContext() as sc:
        # spark = SparkSession.builder.getOrCreate()
        rdd_base = sc.textFile(data)        
        
        # CALCULAR QUÉ DÍA HA HABIDO MÁS TIEMPO DE USO
        rdd_dt=rdd_base.map(mapper_date_time).groupByKey().map(lambda x: (x[0],sum(x[1])))
        day_max=rdd_dt.reduce(takeMax)
        print("El día con más tiempo de uso ha sido",day_max[0], "con un uso total de", day_max[1], \
        "unidades de tiempo")
        
        
        
        # CALCULAR ENTRE QUÉ DOS ESTACIONES HA HABIDO MÁS DESPLAZAMIENTOS, ES DECIR, CUÁL HA
        # SIDO EL TRAYECTO MÁS REPETIDO
        rdd_stations=rdd_base.map(mapper_stations).reduceByKey(lambda x,y: x+y)
        route_max=rdd_stations.reduce(takeMax)
        print("La ruta más usada conecta las estaciones",route_max[0][0],"y",\
              route_max[0][1], "y se ha realizado", route_max[1],"veces")
        
        
        
        # CALCULAR ESTACIÓN MÁS DEFICITARIA
        rdd_start_end=rdd_base.map(mapper_start_end)
        rdd_salidas=rdd_start_end.map(lambda x: (x[0],-1)).reduceByKey(lambda x,y: x+y)
        rdd_llegadas=rdd_start_end.map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y)
        rdd_total=rdd_llegadas.join(rdd_salidas).map(lambda x: (x, sum(x[1])))
        most_defficient_station=rdd_total.reduce(takeMin)
        less_defficient_station=rdd_total.reduce(takeMax)
        print("La estación más deficitaria; i.e., en la que la diferencia entre el número de llegadas",
        "menos el número de salidas es menor es la", most_defficient_station[0][0], "con un total de",
        most_defficient_station[0][1][0], "llegadas y", -most_defficient_station[0][1][1],"salidas,",
        "por tanto tiene un deficit de", most_defficient_station[1])
        print("La estación menos deficitaria es la", less_defficient_station[0][0], "con un total de",
        less_defficient_station[0][1][0], "llegadas y", -less_defficient_station[0][1][1],"salidas,",
        "por tanto tiene un deficit de", less_defficient_station[1])

        
        
        

            
    
if __name__=='__main__':
    if len(sys.argv)>1:
        data = sys.argv[1]
    main(data)


