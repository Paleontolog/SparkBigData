package com

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{column, sum}
import org.apache.spark.sql.types.IntegerType

object FirstApp {
  def run(spark: SparkSession): Unit = {
    val dfData = spark.read.option("header", true).csv("/user/user/input/Data8277.csv")
    val dfSex = spark.read.option("header", true).csv("/user/user/input/DimenLookupArea8277.csv")

    val res = dfData.select(column("year"),
      column("sex"),
      column("count").cast(IntegerType))
      .groupBy(column("year"), column("sex"))
      .agg(sum(column("count")).as("total"))
      .join(dfSex, column("sex") === dfSex("code"))
      .select(column("year"), column("sex"), column("description"), column("total"))
      .coalesce(1)

    val date = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
    res.write.parquet(s"/user/user/output/date=${date.split("_")(0)}/time=${date.split("_")(1)}")
  }
}
