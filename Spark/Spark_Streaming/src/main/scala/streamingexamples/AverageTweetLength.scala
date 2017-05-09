package streamingexamples

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utilities._
import java.util.concurrent.atomic._

/** Uses thread-safe counters to keep track of the average length of
  *  Tweets in a stream.
  */
object AverageTweetLength {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Get rid of log spam
    setupLogging()

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "AverageTweetLength" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText)

    // Map this to tweet character lengths.
    val lengths = statuses.map(status => status.length())

    // As we could have multiple processes adding into these running totals
    // at the same time, we'll use Java's AtomicLong class to make sure
    // these counters are thread-safe.
    val totalTweets = new AtomicLong(0)
    val totalChars = new AtomicLong(0)

    lengths.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)
        totalChars.getAndAdd(rdd.reduce((x,y) => x + y))
        println(f"Total tweets: ${totalTweets.get()}%d, " +
                f"Total characters: ${totalChars.get()}%d, " +
                f"Average: ${totalChars.get() / totalTweets.get()}%d, " +
                f"Max: ${rdd.max()}%d")
      }
    })

    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}