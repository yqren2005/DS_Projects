name := "AnomalyDetection"

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "2.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)