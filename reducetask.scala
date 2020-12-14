// Databricks notebook source
///FileStore/tables/FriendsData.csv

val data = sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

val datardd = data.filter(line => !line.contains("name"))

datardd.take(2)

// COMMAND ----------

val fren = datardd.map(x => (x.split(",")(2),(1,x.split(",")(3).toInt)))

fren.take(10)

// COMMAND ----------

val frenrdd = fren.reduceByKey((x,y) => (x._1+y._1, x._2+y._2))

frenrdd.take(10)

// COMMAND ----------

val avg = frenrdd.mapValues( x=> x._2/x._1)

avg.take(10)

// COMMAND ----------

val fren2 = datardd.map(x => (x.split(",")(2),x.split(",")(3).toInt))

fren2.take(10)

// COMMAND ----------

val data_max = fren2.reduceByKey((x,y) => (if(x>y) x else y))

data_max.take(10)
