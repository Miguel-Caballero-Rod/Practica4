# Practica4 de Programación Paralela: BiciMAD RDD

#### Trabajo realizado por Manuel Pablo Bejarano Galeano, Miguel Caballero Rodríguez y Pedro Corral Ortiz-Coronado

## Descripción básica de la entrega:
Esta práctica tiene dos partes:


En la primera parte de esta práctica trabajaremos con el archivo `sample_10e4.json` que recoge la información de uso de las bicicletas BiciMAD para unos ciertos días, como por ejemplo, la estación de salida, la de llegada, el tiempo de uso, la fecha o el usuario que la utiliza. Con esta información, nos proponemos medir 7 aspectos diferentes: cuál ha sido el día con más tiempo de uso de todos, cuál es el trayecto más realizado, qué estación ha sido la más utilizada sumando salidas y llegadas, qué usuario ha usado más veces la bici en un mismo día, el tipo de usuario y el rango de edad más habituales, y de entre todas las estaciones, cuál tiene el mejor y el peor balance en cuanto a número de llegadas menos el número de salidas, que denominaremos déficit. 

En la segunda parte de esta práctica trabajaremos con el archivo `AccidentesBicicletas_2018.csv` que recoge la información de uso de los accidentes en bicicletas en Madrid en el año 2018, con datos como por ejemplo, la fecha del accidente, edad y sexo de los involucrados, condiciones meteorológicas, número de víctimas o tipo de accidente. Con esta información, nos proponemos medir 4 aspectos diferentes: cuál es el tipo de accidente más frecuente, en qué condiciones meteorológicas y condiciones del suelo son más frecuentes los accidentes, y cuál es la media de víctimas por accidente en el dicho año. 

## Ejecución del programa:

Para la primera parte, el programa a ejecutar es `practica4.py` y para realizar una ejecución correcta en la terminal se debe ejecutar junto al nombre del archivo .py el nombre del archivo .json con la información de BiciMAD, en nuestro caso emplearemos el archivo `sample_10e4.json`. Como ejemplo de ejecución, planteamos el siguiente:

```
python3  practica4.py sample_10e4.json
```

Para la segunda parte, el programa a ejecutar es `accidentes.py` y para realizar una ejecución correcta en la terminal se debe ejecutar junto al nombre del archivo .py el nombre del archivo .csv con la información de los accidentes, en nuestro caso emplearemos el archivo `AccidentesBicicletas_2018.csv`. Como ejemplo de ejecución, planteamos el siguiente:

```
python3  accidentes.py AccidentesBicicletas_2018.csv
```

## Métodos empleados: 
Para realizar la implementación hemos trabajado con el tipo de datos RDD de `pyspark`, en vez de hacer uso de los DataFrame, y se han utilizado los métodos `map`, `reduce`, `reduceByKey`, `groupByKey`, `flatMap` y `join`.
