from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys

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
    
def condicion_meteorologica(x):
    res = []
    if x["CPFA Granizo"] == "SI":
        res.append(("Granizo",1))
    if x["CPFA Hielo"] == "SI":
        res.append(("Hielo",1))
    if x["CPFA Lluvia"] == "SI":
        res.append(("Lluvia",1))
    if x["CPFA Niebla"] == "SI":
        res.append(("Niebla",1))
    if x["CPFA Seco"] == "SI":
        res.append(("Seco",1))
    if x["CPFA Nieve"] == "SI":
        res.append(("Nieve",1))
    return res

def condicion_suelo(x):
    res = []
    if x["CPSV Mojada"] == "SI":
        res.append(("Mojada",1))
    if x["CPSV Aceite"] == "SI":
        res.append(("Aceite",1))
    if x["CPSV Barro"] == "SI":
        res.append(("Barro",1))
    if x["CPSV Grava Suelta"] == "SI":
        res.append(("Grava Suelta",1))
    if x["CPSV Hielo"] == "SI":
        res.append(("Hielo",1))
    if x["CPSV Seca Y Limpia"] == "SI":
        res.append(("Seca Y Limpia",1))
    return res



def main(csv_file_path):
    conf = SparkConf().setAppName("CSV to RDD")
    with SparkContext(conf=conf) as sc:
        spark = SparkSession(sc)        
        rdd_base = spark.read.format("csv").option("header", "true").load(csv_file_path).rdd
        
        #CALCULAR EL TIPO DE ACCIDENTE MÁS FRECUENTE
        rdd_acc_type = rdd_base.map(lambda x : (x["TIPO ACCIDENTE"].strip(),1)).reduceByKey(lambda x,y: x+y)
        type_acc_max = rdd_acc_type.reduce(takeMax)
        print("El tipo de accidente más común ha sido",type_acc_max[0],"que ha ocurrido",
              type_acc_max[1],"veces")
        
        #CALCULAR LA CONDICIÓN METEOROLOGICA EN LA QUE SE HAN DADO MÁS ACCIDENTES
        rdd_acc_met = rdd_base.flatMap(condicion_meteorologica).reduceByKey(lambda x,y: x+y)
        met_acc_max = rdd_acc_met.reduce(takeMax)
        print("la condición meteorológica en la que ha tenido lugar el mayor número de accidentes ha sido",
              met_acc_max[0],"en la que ha ocurrido un total de ",met_acc_max[1],"veces")
        
        #CALCULAR LA CONDICIÓN DEL SUELO EN LA QUE SE HAN DADO MÁS ACCIDENTES
        rdd_acc_suelo = rdd_base.flatMap(condicion_suelo).countByKey()
        suelo_acc_max = max(rdd_acc_suelo.items(), key=lambda x: x[1])
        print("la condición del suelo en la que ha tenido lugar el mayor número de accidentes ha sido",
              suelo_acc_max[0],"en la que ha ocurrido un total de ",suelo_acc_max[1],"veces")
        
        #CALCULAR LA MEDIA PONDERADA DEL NUMERO DE VICTIMAS POR ACCIDENTE
        rdd_n_victima = rdd_base.map(lambda x : (int(x[19].strip()),1)).reduceByKey(lambda x,y: x+y)
        n_victima = rdd_n_victima.reduce(lambda x,y : ((x[0]*x[1]+y[0]*y[1])/(x[1]+y[1]),x[1]+y[1]))
        print("La media ponderada de victimas por accidente es",n_victima[0])
        
        
    

if __name__=='__main__':
    if len(sys.argv)>1:
        data = sys.argv[1]
    main(data)

