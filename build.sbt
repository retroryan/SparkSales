name := """SparkSales"""

version := "1.0"

scalaVersion := "2.10.5"

val sparkVersion = "1.4.1"
val sparkCassandraConnectorVersion = "1.4.0"


libraryDependencies ++= Seq(
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % sparkCassandraConnectorVersion,
  "org.apache.spark"  %% "spark-sql"             % sparkVersion % "provided"
)
