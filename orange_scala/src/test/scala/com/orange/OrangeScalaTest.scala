package com.orange

import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach

class OrangeScalaTest extends SparkFunSuite {

  test("Email providers with more than 10k posts") {
    val ss = spark
    import ss.implicits._

    val testDF = Seq(
      ("aaaaa@gmail.com", 10101, 1990, "", "10.0.0.1"),
      ("aaaaa@hotmail.com", 505, 1992, "", "10.0.0.2"),
      ("aaaaa@yahoo.com", 1000, 1999, "", "10.0.0.3")
    ).toDF("email", "numPosts", "yearSignup", "referred", "ipAddress")

    // Get the emails with more than 10 k posts in a Dataframe with just one column
    val outDF = DataOps.getEmailMorePostsThanK(testDF, minPosts = 10000)
    assert(outDF.count() == 3)

    // Check the email with mosts posts
    val results: Array[(String, Boolean)] = outDF.collect().map(x => (x.getString(0), x.getBoolean(5)))
    assert((results contains ("aaaaa@gmail.com", true)) &&  (results contains ("aaaaa@hotmail.com", false)))
  }


      test("Year/s with max sign ups") {
        val ss = spark
        import ss.implicits._

        val testDF = Seq(
          ("aaaaa@gmail.com", 10101, 2000, "", "10.0.0.1"),
          ( "aaaaa@hotmail.com", 505, 2000, "", "10.0.0.2"),
          ("aaaaa@yahoo.com", 1000, 2001, "", "10.0.0.3"),
          ("bbbbb@gmail.com", 10101, 2002, "", "10.0.0.4"),
          ("ccccc@gmail.com", 10101, 2002, "", "10.0.0.5"),
          ("bbbbb@hotmail.com", 505, 1999, "", "10.0.0.6")
        ).toDF("email", "numPosts", "yearSignup", "referred", "ipAddress")

        // Get the years with maximum number of sign ups
        val outDF = DataOps.getYearsMaxSignUps(testDF)
        // 2000 and 2002 are the top signup years
        assert(outDF.count() == 6)

        val results: Array[(Int, Boolean)] = outDF.collect().map(x => (x.getInt(2), x.getBoolean(5)))
        // Check correctness
        assert((results contains (2000, true))
          && !(results contains (2000, false))
          &&  (results contains (2001, false)))
      }

  test("Number of referral by members") {
    val ss = spark
    import ss.implicits._

    val testDF = Seq(
      ("aaaaa@gmail.com", 10101, 2000, "m1|m2|m3", "10.0.0.1"),
      ("aaaaa@hotmail.com", 505, 2000, "m100|m88|m345|m33", "10.0.0.2"),
      ("aaaaa@yahoo.com", 1000, 2001, "m100", "10.0.0.3"),
      ("bbbbb@gmail.com", 10101, 2002, "", "10.0.0.4")
    ).toDF("email", "numPosts", "yearSignup", "referred", "ipAddress")

    // It is assumed that in the referred field there exists a list of member who referred that user
    // (e.g. for writing good articles).
    // The process splits the string into a list of members and returns the length of that list in the n_referred field
    val outDF = DataOps.getNumberOfReferral(testDF)
    assert(outDF.count() == 4)

    val results: Array[(String, Int)] = outDF.collect().map(x => (x.getString(0), x.getInt(5)))
    assert((results contains ("aaaaa@gmail.com", 3)) &&  (results contains ("bbbbb@gmail.com", 0)))
  }


  test("Class C IP address by first octect") {
    val ss = spark
    import ss.implicits._

    //Class C are the IP addresses starting in 192.0.0.0 and ending in 223.255.255.255
    //Reference: https://en.wikipedia.org/wiki/Classful_network

    val testDF = Seq(
      ("aaaaa@gmail.com", 10101, 2000, "m1|m2|m3", "192.168.1.1"),
      ("aaaaa@hotmail.com", 505, 2000, "m100|m88|m345|m33", "200.1.1.1"),
      ("aaaaa@yahoo.com", 1000, 2001, "m100", "224.4.3.2"),
      ("bbbbb@gmail.com", 10101, 2002, "", "10.0.0.4"),
      ("bbbbb@hotmail.com", 10101, 2002, "", "192.168.1.2")
    ).toDF("email", "numPosts", "yearSignup", "referred", "ipAddress")

    val outDF = DataOps.getIpAddrClassC(testDF)
    assert(outDF.count() == 5)

    val results: Array[(String, Long)] = outDF.collect().map(x => (x.getString(4), x.getLong(5)))
    assert((results contains ("192.168.1.1", 2))
      &&  (results contains ("200.1.1.1", 1))
      &&  (results contains ("10.0.0.4", 0))
      &&  (results contains ("224.4.3.2", 0)))
  }

  test("Frequency of the IP address based on first 3 octets") {
    val ss = spark
    import ss.implicits._

    val testDF = Seq(
      ("aaaaa@gmail.com", 10101, 2000, "m1|m2|m3", "192.168.1.1"),
      ("aaaaa@hotmail.com", 505, 2000, "m100|m88|m345|m33", "192.168.1.2"),
      ("aaaaa@yahoo.com", 1000, 2001, "m100", "224.4.3.2"),
      ("bbbbb@gmail.com", 10101, 2002, "",    "224.4.3.4"),
      ("ccccc@gmail.com", 10101, 2002, "", "10.0.3.4"),
      ("bbbbb@hotmail.com", 10101, 2002, "", "192.168.1.3")
    ).toDF("email", "numPosts", "yearSignup", "referred", "ipAddress")

    val outDF = DataOps.getIPAdd3OctetsFreq(testDF)
    assert(outDF.count() == 6)

    val results: Array[(String, Long)] = outDF.collect().map(x => (x.getString(4), x.getLong(5)))
    assert((results contains ("192.168.1.1", 3))
      &&  (results contains ("224.4.3.2", 2))
      &&  (results contains ("10.0.3.4", 1))
    )

  }


}



/*
Initially I assumed that a mail provided with each records, but I changed my opinion as I did not find
a real sense to include it and make a group by. The statement was not clear and no dataset was provided.
      test("Post by email providers") {
        val ss = spark
        import ss.implicits._

        // In this case we construct the schema of the data
        val testData = Seq(
          ("Google", "aaaaa@gmail.com", 1000, 1990, "", "10.0.0.1"),
          ("Google", "bbbb@gmail.com", 1000, 1991, "", "10.0.0.2"),
          ("Hotmail", "aaaaa@hotmail.com", 2000, 1992, "", "10.0.0.3"),
          ("Hotmail", "bbbbb@hotmail.com", 2000, 1993, "", "10.0.0.4"),
          ("Yahoo", "aaaaa@yahoo.com", 300, 1999, "", "10.0.0.5")
        )

        val testSchema = List(
          StructField("provider", StringType, false),
          StructField("email", StringType, false),
          StructField("numPosts", IntegerType, false),
          StructField("yearSignup", IntegerType, false),
          StructField("referred", StringType, false),
          StructField("ipAddress", StringType, false)
        )

        val testDF = spark.createDataFrame(
          sc.parallelize(testData).map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6)),
          StructType(testSchema)
        )

        val outDF = DataOps.getPostByProviders(testDF)
        assert(outDF.count() == 3)

        // Check each user
        //val outDS: Dataset[User] = outDF.as[User]
        val results: Array[Row] = outDF.collect()

        val googleCount: Long = 2000
        val hotmailCount: Long = 4000
        val yahooCount: Long = 300

        var googleResult: Long = 0
        var hotmailResult: Long = 0
        var yahooResult: Long = 0

        for (r <- results) {

          if (r.getString(0) == "Google") googleResult = r.getLong(1)
          else if (r.getString(0) == "Hotmail") hotmailResult = r.getLong(1)
          else yahooResult = r.getLong(1)
        }

        assert(googleResult == googleCount)
        assert(hotmailResult == hotmailCount)
        assert(yahooResult == yahooCount)
      }
      */