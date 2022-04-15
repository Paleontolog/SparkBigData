package com.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{column, sum, regexp_replace, when}
import org.apache.spark.sql.types.IntegerType

object MaoriCountDataFrame {

  def run(spark: SparkSession, dataPath: String, areaPath: String, resultFilePath: String): Unit = {
    val dfData = spark.read.option("header", true).csv(dataPath)
      .select(
        column("Year"),
        column("Area"),
        column("Ethnic"),
        column("count").cast(IntegerType)
      ).where(
      regexp_replace(column("Area"), "[^\\d]", "-1")
        .cast(IntegerType)
        .between(1, 15)
    )

    val dfArea = spark.read.option("header", true).csv(areaPath)

    val all = dfData
      .select(column("Year"), column("Area"), column("count"), column("Ethnic"))
      .groupBy(dfData("Year"), dfData("Area"))
      .agg(
        sum(dfData("count")).as("AllCount"),
        sum(when(dfData("Ethnic") === "2", dfData("count")).otherwise(0)).as("MaoriCount")
      )

//    val maori = dfData
//      .select(
//        column("Year"),
//        column("Area"),
//        column("count")
//      )
//      .where(dfData("Ethnic") === '2')
//      .groupBy(dfData("Year"), dfData("Area"))
//      .agg(sum(dfData("count")).as("MaoriCount"))
//
//    val result = all
//      .join(maori, all("Year") === maori("Year") && all("Area") === maori("Area"))
//      .join(dfArea, all("Area") === dfArea("Code"), "left")
//      .select(
//        all("Year"),
//        column("Description"),
//        all("Area"),
//        maori("MaoriCount"),
//        all("AllCount"),
//        when(all("AllCount") === 0, 0)
//          .otherwise(maori("MaoriCount") / all("AllCount")).as("Percent")
//      )

        val result = all
          .join(dfArea, all("Area") === dfArea("Code"), "left")
          .select(
            all("Year"),
            column("Description"),
            all("Area"),
            all("MaoriCount"),
            all("AllCount"),
            when(all("AllCount") === 0, 0)
              .otherwise(all("MaoriCount") / all("AllCount")).as("Percent")
          )
    result.show(10000)
//    result.write.parquet(resultFilePath)
  }
}
