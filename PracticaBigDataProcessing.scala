// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # **Práctica Big Data Processing con scala en notebook de Databricks**
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ZÚÑIGA ASPAS, Alba
// MAGIC

// COMMAND ----------

//Los .csv los he subido a DBFS
//Cargamos los dataset 
val df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/alzuaskeepcoding@gmail.com/world_happiness_report_2021.csv")
val df2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/alzuaskeepcoding@gmail.com/world_happiness_report.csv")

df1.show(5)
df2.show(5)


// COMMAND ----------

//1. ¿Cuál es el país más “feliz” del 2021 según la data?
import org.apache.spark.sql.functions._

val happiestCountry2021 = df1.orderBy(desc("Ladder score")).select("Country name", "Ladder score").first()
println(s"El país más feliz de 2021 es ${happiestCountry2021(0)} con un puntaje de ${happiestCountry2021(1)}")



// COMMAND ----------

//2. ¿Cuál es el país más “feliz” del 2021 por continente según la data?
import org.apache.spark.sql.expressions.Window

// Agregamos la columna "Max Ladder score" a df1
val windowSpec = Window.partitionBy("Regional indicator").orderBy(desc("Ladder score"))
val df1WithRank = df1.withColumn("rank", rank().over(windowSpec))

// Filtramos los países que tienen el puntaje más alto por continente
val happiestCountryByContinent2021 = df1WithRank.filter($"rank" === 1)
  .select("Regional indicator", "Country name", "Ladder score")

happiestCountryByContinent2021.show()


// COMMAND ----------

//3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?
import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("year").orderBy(desc("Life Ladder"))
val rankDf = df2.withColumn("rank", rank().over(windowSpec))
val firstPlaceCount = rankDf.filter($"rank" === 1).groupBy("Country name").count().orderBy(desc("count"))

firstPlaceCount.show(1)


// COMMAND ----------

//4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?
val gdp2020 = df2.filter($"year" === 2020).orderBy(desc("Log GDP per capita")).select("Country name").first()
val countryWithHighestGdp2020 = gdp2020(0)

val happinessRank2020 = df2.filter($"year" === 2020).orderBy(desc("Life Ladder"))
  .withColumn("rank", rank().over(Window.partitionBy("year").orderBy(desc("Life Ladder"))))
  .filter($"Country name" === countryWithHighestGdp2020)
  .select("Country name", "rank")

happinessRank2020.show()


// COMMAND ----------

//5. ¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?
// Filtramos los datos para el año 2020 y 2021
val df2020 = df2.filter($"year" === 2020)
val df2021 = df1 // Datos de 2021 ya están en df1

// Calculamos el promedio de Log GDP per capita para 2020
val avgGdp2020 = df2020.agg(avg("Log GDP per capita")).first().getDouble(0)

// Calculamos el promedio de Logged GDP per capita para 2021
val avgGdp2021 = df2021.agg(avg("Logged GDP per capita")).first().getDouble(0)

// Calculamos la variación porcentual
val percentageChange = ((avgGdp2021 - avgGdp2020) / avgGdp2020) * 100

if (percentageChange > 0) {
  println(f"El GDP promedio a nivel mundial aumentó un $percentageChange%.2f%% del 2020 al 2021")
} else {
  println(f"El GDP promedio a nivel mundial disminuyó un $percentageChange%.2f%% del 2020 al 2021")
}


// COMMAND ----------

//6. ¿Cuál es el país con mayor expectativa de vide (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia en ese indicador en el 2019?

// Cargamos los datos asegurándonos de inferir el esquema ya que he tenido un previo error se debía a un intento de convertir un valor String a Double. 
val df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/shared_uploads/alzuaskeepcoding@gmail.com/world_happiness_report_2021.csv")
val df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/shared_uploads/alzuaskeepcoding@gmail.com/world_happiness_report.csv")

import org.apache.spark.sql.functions._

// Filtramos el país con mayor expectativa de vida en 2021
val highestLifeExpectancy2021 = df1.orderBy(desc("Healthy life expectancy")).select("Country name", "Healthy life expectancy").first()
val countryWithHighestLifeExpectancy2021 = highestLifeExpectancy2021.getString(0)
val highestLifeExpectancyValue2021 = highestLifeExpectancy2021.getDouble(1)

// Filtramos los datos para 2019
val df2019 = df2.filter($"year" === 2019)

// Buscamos la expectativa de vida del país en 2019
val lifeExpectancy2019 = df2019.filter($"Country name" === countryWithHighestLifeExpectancy2021).select("Healthy life expectancy at birth").first().getDouble(0)

println(s"El país con mayor expectativa de vida en 2021 es $countryWithHighestLifeExpectancy2021 con una expectativa de vida de $highestLifeExpectancyValue2021 años.")
println(s"En 2019, la expectativa de vida de $countryWithHighestLifeExpectancy2021 era de $lifeExpectancy2019 años.")


// COMMAND ----------


