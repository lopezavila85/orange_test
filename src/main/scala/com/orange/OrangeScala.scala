package com.orange
import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// References
// https://medium.com/big-data-engineering/how-to-install-apache-spark-2-x-in-your-pc-e2047246ffc3
// https://github.com/dmatrix/examples/tree/master/spark/databricks/apps/scala/2.x
// spark-submit --class com.orange.test.OrangeScala --master local[2] target\scala-2.11\orange_scala_2.11-1.0.jar

object OrangeScala {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("CSV for users is required")
      System.exit(1)
    }

    // warehouseLocation points to the default location for managed databases and tables
    //val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    println(warehouseLocation)

    // Create the Spark session object
    val spark = SparkSession
      .builder()
      .appName("OrangeScala")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    // Currently, this HQL should be read from a resource. For simplicity is created here.
    sql("""
      CREATE TABLE IF NOT EXISTS users (
        email STRING,
        numPosts INT,
        yearSignup INT,
        referred STRING,
        ipAddress STRING) USING hive
    """)

    //Read the users CSV, which contains a header. Provide the proper schema
    val usersSchema = StructType(List(
      StructField("email", StringType, false),
      StructField("numPosts", IntegerType, false),
      StructField("yearSignup", IntegerType, false),
      StructField("referred", StringType, false),
      StructField("ipAddress", StringType, false)
    ))

    // If the application is executed in yarn mode, the file should be sent with --files argument
    // Read the input file passed in the argument
    val dfUsers = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(usersSchema)
      .load(args(0))

    val hiveops = HiveOps()
    // Overwrite table with input values in CSV
    hiveops.write(dfUsers, "users")

    //Cast to a Dataset using implicits
    val dsUsers = hiveops.read("users").as[User]
    dsUsers.show()

    val dfUsers1 = DataOps.getEmailMorePostsThanK(dfUsers, minPosts = 10000)
    val dfUsers2 = DataOps.getYearsMaxSignUps(dfUsers1)
    val dfUsers3 = DataOps.getNumberOfReferral(dfUsers2)
    val dfUsers4 = DataOps.getIpAddrClassC(dfUsers3)
    val dfUsers5 = DataOps.getIPAdd3OctetsFreq(dfUsers4)

    sql("""
      CREATE TABLE IF NOT EXISTS users_output (
        email STRING,
        numPosts INT,
        yearSignup INT,
        referred STRING,
        ipAddress STRING,
        high_posts BOOLEAN,
        maxYearSignup BOOLEAN,
        n_referred INT,
        getIpAddrClassC LONG,
        freqIPv4CFirst LONG) USING hive
    """)

    hiveops.write(dfUsers5, "users_output")

    // Shows the final result
    hiveops.read("users_output").show()

    // Check AwsOps but writing to a local filesystem folder instead of an S3 bucket.
    AwsOps().write(dfUsers5, "users_output")

  }
}