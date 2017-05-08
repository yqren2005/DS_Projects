/**
  * Created by yren on 5/7/2017.
  */

import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import scala.collection.immutable.ListMap

Logger.getLogger("org").setLevel(Level.ERROR)
val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
val sc = new SparkContext(conf)

// Use countByValue here because the resulting map is expected to be small
// the whole thing will be loaded into the driver's memory
// for large result use rdd.map(x => (x, 1L)).reduceByKey(_ + _)
val counts = sc.textFile("war_and_peace.txt")
               .map(_.toLowerCase)
               .map(line => line.replaceAll("[^a-z0-9\\s]", ""))
               .flatMap(line => line.split("\\s+"))
               .filter(_.nonEmpty)
               .countByValue()

// Print the top 20 results
val sample = ListMap(counts.toSeq.sortWith(_._2 > _._2):_*).take(20)

for ((word, count) <- sample) {
  println(word + " [" + count + "]")
}

sc.stop()