package com

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{column, regexp_replace}
import org.apache.spark.sql.types.IntegerType

object MaoriCountSQL {

  def joinWay(spark: SparkSession): DataFrame = {
    //language=SQL
    spark.sql(
      """
      with all_population as (
        select Year, Area, sum(count) as all_count
        from population
        group by Year, Area
      ), maori_population as (
        select Year, Area, sum(count) as maori_count
        from population
        where Ethnic = '2'
        group by Year, Area
      )
      select all.Year,
             ar.Description,
             all.Area,
             maori.maori_count,
             all.all_count,
             IF(all.all_count = 0, 0, maori.maori_count / all.all_count) as Percent
      FROM all_population all
            INNER JOIN maori_population maori
                ON all.Year = maori.Year and all.Area = maori.Area
            INNER JOIN areas ar
                ON all.Area = ar.Code
      """.stripMargin)
  }

  def windowFunctionWay(spark: SparkSession): DataFrame = {
    //language=SQL
    spark.sql(
      """
         WITH calculated AS (
             SELECT Year,
                    Area,
                    sum(count) AS all_count,
                    sum(IF(Ethnic = '2', count, 0)) AS maori_count
             FROM population
             GROUP BY Year, Area
         )

         SELECT all.Year,
                ar.Description,
                all.Area,
                all.maori_count,
                all.all_count,
                IF(all.all_count = 0, 0, all.maori_count / all.all_count) as Percent
         FROM calculated all
              INNER JOIN areas ar
                  ON all.Area = ar.Code

        """.stripMargin)
  }

  def run(spark: SparkSession, dataPath: String, areaPath: String, resultFilePath: String): Unit = {
    spark.read.option("header", true).csv(dataPath)
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
      .createOrReplaceTempView("population")

    spark.read.option("header", true)
      .csv(areaPath)
      .createOrReplaceTempView("areas")

    //    val result = joinWay(spark)
    val result = windowFunctionWay(spark)

      result.show(100)
    //    result.write.csv(resultFilePath)
  }
}
