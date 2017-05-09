package streamingexamples

import org.apache.spark._
import Utilities._

/**
  * Create a RDD of lines from a text file, and keep count of
  *  how often each word appears.
  */
object WordCount {

  def main(args: Array[String]) {
    // Get rid of log spam
    setupLogging()
    // Set up a SparkContext named WordCount that runs locally using
    // all available cores.
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create a RDD of lines of text in our book
    val input = sc.textFile("war_and_peace.txt")

    val counts = input.flatMap(line => tokenize(line))
                      .filter(_.nonEmpty)
                      .map(word => (word, 1))
                      .reduceByKey(_ + _)
                      .sortBy(pair => pair._2, ascending=false)

    println('\n' + "Unique word counts: " + counts.count)

    for ((word, count) <- counts.take(20)) {
      println(word + " [" + count + "]")
    }

    //rdd.coalesce(1).saveAsTextFile("result.txt")

    sc.stop()
  }
  // Split a line of text into individual words.
  private def tokenize(text: String): Array[String] = {
    // Lowercase each word and remove punctuation. \\s+ --> replaces 1 or more spaces.
    text.toLowerCase.replaceAll("[^a-z0-9\\s]", "").split("\\s+")
  }
}