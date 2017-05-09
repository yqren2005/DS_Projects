package streamingexamples

import org.apache.log4j.{Level, Logger}
import scala.io.Source

object Utilities {
    /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    val rootLogger = Logger.getLogger("org")
    rootLogger.setLevel(Level.ERROR)
  }
  
  /** Configures Twitter service credentials using twitter.txt in the main workspace directory */
  def setupTwitter() = {
    for (line <- Source.fromFile("twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
}