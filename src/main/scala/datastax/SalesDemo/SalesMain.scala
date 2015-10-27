package datastax.SalesDemo

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark._
import com.datastax.spark.connector._

//  ~/dse-4.8.0/bin/dse spark-submit --class demo.SearchAnalyticsDemo ./target/scala-2.10/SearchAnalyticsDemo-assembly-0.2.0.jar

object SalesMain {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("SalesMain")

    val sc = new SparkContext(conf)

    try {
      setup(conf)
      runSales(sc)
    }
    finally {
      sc.stop()
    }
  }

  def setup(conf:SparkConf) {


    CassandraConnector(conf).withSessionDo { session =>
      session.execute( """CREATE KEYSPACE IF NOT EXISTS search_demo with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")

      session.execute(
        """CREATE TABLE IF NOT EXISTS search_demo.kjv (
          |book text ,
          |chapter int,
          |verse int,
          |body text,
          |PRIMARY KEY (book, chapter)
          |)""".stripMargin)
    }
  }


  def runSales(sc: SparkContext): Unit = {

  }
}
