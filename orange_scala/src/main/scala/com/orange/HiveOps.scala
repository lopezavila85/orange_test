package com.orange
import org.apache.spark.sql.{DataFrame, SparkSession}

case class HiveOps() extends StorageOps {

  override def read(inputTable: String): DataFrame = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.sql
    sql(s"SELECT * FROM $inputTable")

  }

  override def write(df: DataFrame, outputTable: String): Boolean = {
    import org.apache.spark.sql.SaveMode
    // Data is reduced in this test, so we can work with one partition
    df.coalesce(1).write.mode(SaveMode.Overwrite).saveAsTable(outputTable)

    //Check if the table has been written correctly
    val spark = SparkSession.builder().getOrCreate()
    spark.catalog.tableExists(outputTable)
  }
}
