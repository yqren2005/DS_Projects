/**
  * Created by yren on 5/6/2017.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._

Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().master("local[*]").getOrCreate()
import spark.implicits._

val counts = spark.read
                  .textFile("war_and_peace.txt")
                  .as[String]
                  .map(line => line.replaceAll("[^a-zA-Z0-9\\s]", ""))
                  .flatMap(line => line.split("\\s+"))
                  .filter(_.nonEmpty)
                  .groupByKey(_.toLowerCase)
                  .count()

counts.orderBy(desc("count(1)")).show()
spark.stop()