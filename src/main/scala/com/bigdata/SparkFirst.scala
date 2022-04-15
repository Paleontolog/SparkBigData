package com.bigdata

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.MaoriCountSQL
import org.apache.spark.sql.SparkSession

object Mode extends Enumeration {
  type Mode = Value
  val RDD, DF, SQL = Value

  def from(str: String): Mode = {
    str.toUpperCase() match {
      case "RDD" => RDD
      case "DF" => DF
      case "SQL" => SQL
      case _ => throw new IllegalArgumentException(s"Unknown type $str")
    }
  }
}

import Mode._

object SparkFirst {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.0.104:8020")
      .appName("SparkFirst")
      .getOrCreate()


    //    val spark =  SparkSession.builder().master("yarn")
    //      .config("spark.hadoop.fs.defaultFS","hdfs://192.168.0.104:8020")
    //      .config("spark.hadoop.yarn.resourcemanager.address","192.168.0.104:8032")
    //      .appName("SparkFirst")
    //      .getOrCreate()


    val mode = Mode.from(args(0))
    val dfData = args(1) //"/user/user/input/Data8277.csv"
    val dfArea = args(2)//"/user/user/input/DimenLookupArea8277.csv"
    val result = args(3) //"/user/user/output"

    val date = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
    val resultFile = s"$result/date=${date.split("_")(0)}/time=${date.split("_")(1)}"

    mode match {
      case RDD => MaoriCountRDD.run(spark, dfData, dfArea, resultFile)
      case DF => MaoriCountDataFrame.run(spark, dfData, dfArea, resultFile)
      case SQL => MaoriCountSQL.run(spark, dfData, dfArea, resultFile)
    }
  }
}
