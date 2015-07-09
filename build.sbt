organization  := "com.inkenkun.x1"

name := "spark-japanese-topic"

version := "1.0"

scalaVersion := "2.11.7"

resolvers ++= Seq(
  "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases",
  "ATILIKA dependencies" at "http://www.atilika.org/nexus/content/repositories/atilika"
)

libraryDependencies ++= Seq(
  "org.atilika.kuromoji" % "kuromoji" % "0.7.7",
  "org.apache.spark" %% "spark-core" % "1.4.0" % "compile",
  "org.apache.spark" %% "spark-sql" % "1.4.0" % "compile",
  "org.apache.spark" %% "spark-mllib" % "1.4.0" % "compile",
  "com.databricks" %% "spark-csv" % "1.1.0"
)

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2-junit" % "3.3.1" % "test"
)

scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-target:jvm-1.7")

testOptions += Tests.Argument("console", "junitxml")
