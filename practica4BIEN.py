from pyspark import SparkContext
import json
import sys
import datetime


def mapper_date_time(line):
    data = json.loads(line)
    time = data['travel_time']
    date_total = datetime.datetime.fromisoformat(data['unplug_hourTime']["$date"][:-5])
    date=f"{date_total.day}/{date_total.month}/{date_total.year}"
    return date,time

def mapper_user_type(line):
    data = json.loads(line)
    user_type = data['user_type']
    return user_type,1

def mapper_user_age(line):
    data = json.loads(line)
    user_age = data['ageRange']
    return user_age,1

def mapper_stations(line):
    data = json.loads(line)
    start = data['idunplug_station']
    end = data['idplug_station']
    return (min(start,end), max(start,end)),1

def mapper_stations_2(line):
    data = json.loads(line)
    start = data['idunplug_station']
    end = data['idplug_station']
    return [(start,1), (end,1)]

def mapper_user_day_code(line):
    data = json.loads(line)
    user_code = data['user_day_code']
    date_total = datetime.datetime.fromisoformat(data['unplug_hourTime']["$date"][:-5])
    date=f"{date_total.day}/{date_total.month}/{date_total.year}"
    return (user_code, date), 1


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

def main(fileNames):
    with SparkContext() as sc:
        """
        ******************************************************************************************
        """
       
        
        # PARA CONSEGUIR QUE SE JUNTEN LOS DIFERENTES ARCHIVOS .json DEBERÍA FUNCIONAR:
        rdd_base=sc.emptyRDD()
        for data in fileNames:
            print(data)
            rdd_aux = sc.textFile(data)
            if rdd_base.count()>0:
            	rdd_base = rdd_base.fullOuterJoin(rdd_aux)
            else:
            	rdd_base = rdd_aux
        
        
        """
        ******************************************************************************************
        """
        
        
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
            
        # CALCULAR LA ESTACION QUE MAS SE USA TENIENDO EN CUENTA TANTO LAS 
        #SALIDAS COMO LAS ENTRADAS
        rdd_stations_2=rdd_base.flatMap(mapper_stations_2).reduceByKey(lambda x,y: x+y)
        station_max=rdd_stations_2.reduce(takeMax)
        print("La estación más utilizada es la estación:",station_max[0],
              "y se ha usado", station_max[1],"veces")
        
            
        #CALCULAR EL MAXIMO DE USOS DE UN MISMO USUARIO EN UN MISMO DIA
        rdd_user_code=rdd_base.map(mapper_user_day_code).reduceByKey(lambda x,y: x+y)
        user_max=rdd_user_code.reduce(takeMax)
        print("El dia ",user_max[0][1],"el usuario", user_max[0][0],
               "ha realizado", user_max[1],"viajes, y es el dia que un mismo",
               "usuario ha cogido más veces la bicicleta")
        
        #CALCULAR EL TIPO DE USUARIO QUE MÁS/MENOS HA USADO LA BICICLETA EN GENERAL
        rdd_user_type=rdd_base.map(mapper_user_type).reduceByKey(lambda x,y: x+y)
        type_max=rdd_user_type.reduce(takeMax)
        print("El tipo de usuario que más ha usado la bicicleta ha sido:",type_max[0],
              "y la ha usado un total de", type_max[1], "veces")
        type_min=rdd_user_type.reduce(takeMin)
        print("El tipo de usuario que menos ha usado la bicicleta ha sido:",type_min[0],
              "y la ha usado un total de", type_min[1], "veces")
        
        #CALCULAR EL RANGO DE EDAD DE USUARIO QUE MÁS/MENOS HA USADO LA BICICLETA EN GENERAL
        rdd_user_age=rdd_base.map(mapper_user_age).reduceByKey(lambda x,y: x+y)
        age_max=rdd_user_age.reduce(takeMax)
        print("El rango de edad de los usuarios que más ha usado la bicicleta ha sido:",age_max[0],
              "y la ha usado un total de", age_max[1], "veces")
        age_min=rdd_user_age.reduce(takeMin)
        print("El rango de edad de los usuarios que menos ha usado la bicicleta ha sido:",age_min[0],
              "y la ha usado un total de", age_min[1], "veces")
        
        
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
        fileNames= sys.argv[1:]
    main(fileNames)


"""
******************************************************************************************


EJEMPLO DE EJECUCIÓN EN UNA TERMINAL DE LINUX, EN EL CLÚSTER NO ESTOY SEGURO DE QUE SIRVA

python3 practica4BIEN nombre1.json nombre2.json ... nombreX.json


******************************************************************************************
"""


