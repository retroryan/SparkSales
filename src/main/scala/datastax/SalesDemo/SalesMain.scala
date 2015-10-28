package datastax.SalesDemo

import com.datastax.spark.connector._
import org.apache.spark._

// sbt package
// dse-4.8.0/bin/dse spark-submit --class datastax.SalesDemo.SalesMain ./target/scala-2.10/sparksales_2.10-1.0.jar

case class Stores(store_id: String, minute: Integer)

case class StoresSales(store_id: String, minute: Integer, transaction_id: java.util.UUID, product_id: Integer, quantity: Integer, sale_time: org.joda.time.DateTime) {

  override def toString: String = {
    s"$store_id, $product_id, $quantity}"
  }
}

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

  def setup(conf: SparkConf) {


    /*
       not needed for this demo but it can be used when a connection to cassandra is needed

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
       }*/
  }


  def runSales(sc: SparkContext): Unit = {
    val stores = sc.cassandraTable[Stores]("walmartstores", "stores_to_process").where("minute  = ?", 24098450)
    val joinedSales = stores.joinWithCassandraTable[StoresSales]("walmartstores", "sales_by_store_minute")

    //Two ways to reduce by key - first preserves the types
    val salesForStoresPairRDD = joinedSales.map {
      case (store, storeSales) => ((storeSales.store_id, storeSales.product_id), storeSales)
    }

    val salesByStore = salesForStoresPairRDD.reduceByKey((s1, s2) => s1.copy(quantity = s1.quantity + s2.quantity))
    salesByStore.collect.foreach(println _)

    //second just returns the raw primitives
    val storeSalesRDD = joinedSales.map(rows => rows._2)
    val formattedSales = storeSalesRDD.map(sale => ((sale.store_id, sale.product_id), sale.quantity))
    val formattedStoreSales = formattedSales.reduceByKey((x, y) => x + y)

  }
}
