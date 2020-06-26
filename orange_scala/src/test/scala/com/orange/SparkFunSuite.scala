package com.orange

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

trait SparkFunSuite extends AnyFunSuite with BeforeAndAfterAll with Matchers {
  var spark: SparkSession = _
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    val master = "local[*]"
    val appName = "TestApp"
    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.driver.allowMultipleContexts", "false")
      .set("spark.ui.enabled", "false")
      // https://github.com/MrPowers/spark-fast-tests
      .set("spark.sql.shuffle.partitions", "1")

    spark = SparkSession.builder().config(conf).getOrCreate()

    sc = spark.sparkContext
    sqlContext = spark.sqlContext

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }
}
