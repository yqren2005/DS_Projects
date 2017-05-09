
import org.apache.spark._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._
import org.apache.log4j.{Level, Logger}

object AbnomalyDetection extends App {

  // Get rid of log spam
  Logger.getLogger("org").setLevel(Level.ERROR)
  // Create Spark context
  val conf = new SparkConf().setAppName("AbnomalyDetection").setMaster("local[*]")
  val sc = new SparkContext(conf)
  // Read in the compressed data
  val rawData = sc.textFile("kddcup.data.gz")
  // Print the 1st entry
  rawData.take(1).foreach(println)
  // Print label and counts with desc order
  println("\n(label,count):\n")
  rawData.map(_.stripSuffix(".").split(",").last)
         .countByValue()
         .toSeq
         .sortBy(_._2)
         .reverse
         .foreach(println)

  val labelsAndData = rawData.map { line =>
    val buffer = line.stripSuffix(".")
                     .split(",")
                     .toBuffer
    buffer.remove(1, 3)
    val label = buffer.remove(buffer.length-1)
    val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
    (label,vector)
  }
  val data = labelsAndData.values.cache()

  val kmeans = new KMeans()
  kmeans.setK(150)
  val model = kmeans.run(data)

  val clusterLabelCount = labelsAndData.map { case (label,datum) =>
    val cluster = model.predict(datum)
    (cluster,label)
  }.countByValue

  println("\ncluster       label   count     ")

  clusterLabelCount.toSeq.sorted.foreach {
    case ((cluster,label),count) =>
      println(f"$cluster%1s$label%18s$count%8s")
  }
  sc.stop()
}