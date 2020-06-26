package com.orange

import org.apache.spark.sql.DataFrame

trait StorageOps {

  def read(inputPath: String): DataFrame

  def write(df: DataFrame, outputPath: String): Boolean
}
