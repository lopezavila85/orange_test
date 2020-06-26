package com.orange

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataOps {

  /**
   * Creates a new column with a flag if the user has more posts than a certain number
   * @param dfInput the input DataFrame
   * @param minPosts  the minimum number of posts to mark the user as a high poster
   * @return  a DataFrame with a new column "high_posts" pointing the users with a high number of posts
   */
  def getEmailMorePostsThanK(dfInput: DataFrame, minPosts: Integer = 20): DataFrame =
    dfInput
      .withColumn("high_posts",
        when(col("numPosts") >= minPosts, true)
          .otherwise(false)
      )

  /*
  // Initially, a consideration that a main provider existed in the record was made (e.g: Google, Yahoo, Hotmail).
  // However I discarded the idea. This method simply grouped by the provided column and completed the sum of posts.
  def getPostByProviders(dfInput: DataFrame): DataFrame =
    //Example with function col
    dfInput.select(col("provider"), col("numPosts"))
      .groupBy(col("provider"))
      .agg(sum("numPosts").alias("totalPosts"))
  */

  /**
   * Calculates the year/s with maximum number of sign ups and creates a new column with a flag
   * setting if the user signed up in a year with a maximum number of sign ups
   * @param dfInput the input DataFrame
   * @return  a DataFrame containing a maxYearSignup column indicating if the user signed up in the year with
   *          the maximum number of sign ups
   */
  def getYearsMaxSignUps(dfInput: DataFrame): DataFrame = {

    val dfCount = dfInput.select("yearSignup")
      .groupBy("yearSignup")
      .count()
      .sort(desc("count"))

    // Get the maximum count of signups
    val maxValue = dfCount.first().getLong(1)

    // Select the year and rename the columns for the join
    val dfMaxYears: DataFrame = dfCount
      .select("yearSignup")
      .where(s"count = $maxValue")
      .withColumnRenamed("yearSignup", "maxYearSignup")

    // Left join to obtain a new column with a flag in case the year is one with a top number of sign ups
    dfInput.join(broadcast(dfMaxYears), col("yearSignup") === col("maxYearSignup"), "left_outer")
      .withColumn("maxYearSignup",
        when(col("maxYearSignup").isNull, false)
          .otherwise(true))
  }

  /**
   * Creates a new column setting the number of references made to a certain user
   * For simplicity, members are concatenated in a string with pipes (e.g: m1|m2...|mn).
   * @param dfInput the input DataFrame
   * @return  a DataFrame with a new column n_referred, setting the number of references for a certain user
   */
  def getNumberOfReferral(dfInput: DataFrame): DataFrame = {
    dfInput
      .withColumn("n_referred",
        when(length(trim(col("referred"))) > 0, size(split(trim(col("referred")), "\\|")))
          .otherwise(0))
  }

  //https://stackoverflow.com/questions/60533540/validation-of-ip-address-in-scala
  /**
   * Regular expression for an IPv4 address
   */
  val ipV4 = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})""".r

  /**
   * Parses a string into an integer in the range of IPv4 addresses
   * @param s a string to be parsed
   * @return  a number between 0 and 255 if ok, -1 otherwise
   */
  def parseV4(s: String): Int = {
    val i = Integer.parseInt(s)
    if (0 <= i && i <= 255) i else -1
  }

  /**
   * Retrieves the first octect in an IPv4 address of class C. Null otherwise
   */
  val firstOctectIPClassC: String => Integer =
    {
      case ipV4(a, _, _, _) =>
        val a1 = parseV4(a)
        if (a1 >= 192 && a1 < 224) a1
        else null
      case _ =>
        null
    }

  // Registration of the UDF to obtain the first octect
  val firstOctectUDF = udf(firstOctectIPClassC)

  /**
   * Retrieves the first three octects of an IPv4 address if it is valid, null otherwise
   */
  val first3OctectsIP: String => String =
    {
      case ipV4(a, b, c, d) =>
        val a1 = parseV4(a)
        val b1 = parseV4(b)
        val c1 = parseV4(c)
        if (a1 >= 0 && b1 >= 0 && c1 >= 0) s"$a1.$b1.$c1"
        else null
      case _ =>
        null
    }

  // Registration of the UDF to obtain the first three octects
  val first3OctectsUDF = udf(first3OctectsIP)

  /**
   * Adds a column with the frequency of the IPv4 Address in case it is a class C one
   * based just in the first octect.
   *
   * Note: Class C are the IP addresses starting in 192.0.0.0 and ending in 223.255.255.255
   * @param dfInput the input DataFrame
   * @return  the output DataFrame with a column added called freqIPv4CFirst containing the required frequency
   */
  def getIpAddrClassC(dfInput: DataFrame): DataFrame = {

    val auxDf: DataFrame = dfInput.withColumn("firstOctect", firstOctectUDF(col("ipAddress")))
    val foDF: DataFrame = auxDf.filter(col("firstOctect").isNotNull).groupBy("firstOctect").count()

    auxDf.join(foDF, Seq("firstOctect"), "left_outer")
      .withColumn("freqIPv4CFirst",
        when(col("count").isNull, 0)
          .otherwise(col("count")))
      .drop("firstOctect")
      .drop("count")
  }

  /**
   * Adds a column including the frequency of IP address based on first 3 octets
   * @param dfInput the input DataFrame
   * @return  the output DataFrame with a column added called freqIPv4First3Octets containing the required frequency
   */
  def getIPAdd3OctetsFreq(dfInput: DataFrame): DataFrame = {
    val auxDf: DataFrame = dfInput.withColumn("octets", first3OctectsUDF(col("ipAddress")))
    val foDF: DataFrame = auxDf.filter(col("octets").isNotNull).groupBy("octets").count()

    auxDf.join(foDF, Seq("octets"), "left_outer")
      .withColumn("freqIPv4First3Octets",
        when(col("count").isNull, 0)
          .otherwise(col("count")))
      .drop("octets")
      .drop("count")
  }
}
