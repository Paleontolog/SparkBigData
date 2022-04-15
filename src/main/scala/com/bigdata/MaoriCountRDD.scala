package com.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

case class AreaInfo
(
  code: String,
  name: String
)

case class DataInfo
(
  year: String,
  ethnic: String,
  area: String,
  count: Int
)

case class ResultData
(
  year: String,
  ethnic: String,
  areaCode: String,
  countAll: Int,
  countMaori: Int,
  maoriPercent: Double
)

object MaoriCountRDD {
  private def splitString(rdd: RDD[String]): RDD[Array[String]] = {
    rdd
      .zipWithIndex()
      .filter(_._2 != 0)
      .map { case (str, _) => str.split(",") }
  }

  def run(spark: SparkSession, dataPath: String, areaPath: String, resultFilePath: String): Unit = {
    val rddData = splitString(spark.sparkContext.textFile(dataPath))
      .map(x => DataInfo(x(0), x(2), x(4), Try(x(5).toInt).getOrElse(0)))
      .filter { data =>
        val code = Try(data.area.toInt).getOrElse(-1)
        code >= 1 && code <= 15
      }

    val rddArea = splitString(spark.sparkContext.textFile(areaPath))
      .map(x => AreaInfo(x(0), x(1)))
      .groupBy(_.code)

    val resultData = rddData.groupBy(x => Seq(x.year, x.area))
      .map { case (key, group) =>
        val allSum = group.map(_.count).sum
        val countMaori = group.filter(_.ethnic == "2").map(_.count).sum
        val dataInfo = group.head

        val result = ResultData(dataInfo.year,
          dataInfo.ethnic,
          dataInfo.area,
          allSum,
          countMaori,
          if (allSum != 0) countMaori.toDouble / allSum.toDouble else 0.0
        )
        (result.areaCode, result)
      }
      .leftOuterJoin(rddArea)
      .map { case (code, (result, info)) =>
        val data = result
        val region = info.map(_.head.name).getOrElse("error")
        Seq(data.year, region, data.areaCode, data.countMaori, data.countAll, data.maoriPercent)
      }

    resultData
      .foreach(println(_))
    //      .coalesce(1)
    //      .saveAsTextFile(resultFilePath)
  }
}
