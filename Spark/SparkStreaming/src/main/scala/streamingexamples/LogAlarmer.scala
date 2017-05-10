package streamingexamples

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import java.util.regex.Matcher
import Utilities._

/** Monitors a stream of Apache access logs on port 9999, and prints an alarm
  *  if an excessive ratio of errors is encountered. Before run the program, execute:
  *  ncat.exe -kl 9999 < access_log.txt
  *
  *  The program requires 4 arguments, for example:
  *  args(0)=127.0.0.1
  *  args(1)=9999
  *  args(2)=300
  *  args(3)=0.75
  */
object LogAlarmer {

  def main(args: Array[String]) {
    // Get rid of log spam
    setupLogging()

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogAlarmer", Seconds(1))

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream(args(0).toString, args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the status field from each log line
    val statuses = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if (matcher.matches()) matcher.group(6) else "Error"
    }
    )

    // Now map these status results to success and failure
    val successFailure = statuses.map(x => {
      val statusCode = util.Try(x.toInt) getOrElse 0
      if (statusCode >= 200 && statusCode < 300) {
        "Success"
      } else if (statusCode >= 500 && statusCode < 600) {
        "Failure"
      } else {
        "Other"
      }
    })

    // Tally up statuses over a 5-minute window sliding every second
    val statusCounts = successFailure.countByValueAndWindow(Seconds(args(2).toInt), Seconds(1))

    // For each batch, get the RDD's representing data from our current window
    statusCounts.foreachRDD((rdd, time) => {
      // Keep track of total success and error codes from each RDD
      var totalSuccess:Long = 0
      var totalError:Long = 0

      if (rdd.count() > 0) {
        val elements = rdd.collect()
        for (element <- elements) {
          val result = element._1
          val count = element._2
          if (result == "Success") {
            totalSuccess += count
          }
          if (result == "Failure") {
            totalError += count
          }
        }
      }

      // Print totals from current window
      println("Total success: " + totalSuccess + " Total failure: " + totalError)

      // Don't alarm unless we have some minimum amount of data to work with
      if (totalError + totalSuccess > 100) {
        // Compute the error rate
        // Note use of util.Try to handle potential divide by zero exception
        val ratio:Double = util.Try( totalError.toDouble / totalSuccess.toDouble ) getOrElse 1.0
        // If error/success ratio > a threshold such as 75% wake someone up
        if (ratio > args(3).toDouble) {
          // In real life, you'd use JavaMail or Scala's courier library to send an
          // email that causes somebody's phone to make annoying noises
          println("Wake somebody up! Something is horribly wrong.")
        } else {
          println("All systems go.")
        }
      }
      println()
    })

    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}