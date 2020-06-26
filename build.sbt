name := "orange_scala"

version := "1.0"

scalaVersion := "2.11.12"

// Added dependencies to read from Amazon S3
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql"  % "2.4.5",
  "org.apache.hadoop" % "hadoop-aws"  % "3.2.1",
  "com.amazonaws" % "aws-java-sdk" % "1.11.810",
  "org.scalactic" %% "scalactic" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)

