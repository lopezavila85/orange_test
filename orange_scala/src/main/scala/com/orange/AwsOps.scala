package com.orange
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

// Invoke spark-submit with the following jars (the ones in build.sbt):
//--jars jars/hadoop-aws-3.2.1.jar,jars/aws-java-sdk-1.11.810.jar

//https://hadoop.apache.org/docs/r2.8.0/hadoop-aws/tools/hadoop-aws/index.html
case class AwsOps() extends StorageOps {

  //e.g. "s3a://bucket-name/object-path"
  // NOTE: s3: is being phased out. Use s3n: (2nd generation) or s3a: instead. (3rd generation)
  override def read(inputBucket: String): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    spark.read.load(inputBucket)
  }

  /**
   * Output format is set to csv, but if could be configurable, as well as the header and the write mode.
   * For this test they are fixed
   * @param df The DataFrame to write
   * @param outputBucket the path where the info is being stored
   * @return true if the data is written, false otherwise
   */
  override def write(df: DataFrame, outputBucket: String): Boolean = {
    df.coalesce(1).write.format("csv").option("header","true").mode("Overwrite")
      .save(outputBucket)

    // Check if the bucket exists and contains files
    val spark = SparkSession.builder().getOrCreate()
    val fileSystem = FileSystem.get(URI.create(outputBucket), spark.sparkContext.hadoopConfiguration)
    val it = fileSystem.listFiles(new Path(outputBucket), true)
    if (it == null) false else it.hasNext
  }
}
