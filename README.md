# Practica4 de Programación Paralela: BiciMAD RDD

## Descripción básica de la entrega:
En esta práctica trabajaremos con el archivo `sample_10e4.json` que recoge la información de uso de las bicicletas BiciMAD para unos ciertos días, como por ejemplo, la estación de salida, la de llegada, el tiempo de uso, la fecha o el usuario que la utiliza. Con esta información, nos proponemos medir 3 aspectos diferentes, cuál ha sido el día con más tiempo de uso de todos, cuál es el trayecto más realizado y de entre todas las estaciones, nos gustaría saber cuál tiene el mejor y el peor balance en cuanto a número de llegadas menos el número de salidas, que denominaremos déficit. 



## Ejecución del programa:

El programa a ejecutar es `practica4.py` y para realizar una ejecución correcta en la terminal se debe ejecutar junto al nombre del archivo .py el nombre del archivo .json con la información de BiciMAD, en nuestro caso emplearemos el archivo `sample_10e4.json`. Como ejemplo de ejecución, planteamos el siguiente:

```
python3  practica4.py sample_10e4.json
```


## Métodos empleados: 
Para realizar la implementación hemos trabajado con el tipo de datos RDD de `pyspark`, en vez de hacer uso de los DataFrame, y se han utilizado los métodos `map`, `reduce`, `reduceByKey`, `groupByKey` y `join`. Hemos aprendido mucho profe, y la verdad es que hacerlo con amigos es muy divertido, aunque el puto de Pablo no ha hecho nada, como tú, 
```
hijo de puta.
```
:shipit:
