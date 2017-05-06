/////////////////////////////////
// K MEANS CLUSTERING ////
///////////////////////////////

// Source of the Data
//http://archive.ics.uci.edu/ml/datasets/Wholesale+customers

// Here is the info on the data:
// 1)	FRESH: annual spending (m.u.) on fresh products (Continuous);
// 2)	MILK: annual spending (m.u.) on milk products (Continuous);
// 3)	GROCERY: annual spending (m.u.)on grocery products (Continuous);
// 4)	FROZEN: annual spending (m.u.)on frozen products (Continuous)
// 5)	DETERGENTS_PAPER: annual spending (m.u.) on detergents and paper products (Continuous)
// 6)	DELICATESSEN: annual spending (m.u.)on and delicatessen products (Continuous);
// 7)	CHANNEL: customers Channel - Horeca (Hotel/Restaurant/Cafe) or Retail channel (Nominal)
// 8)	REGION: customers Region- Lisnon, Oporto or Other (Nominal)

// Import SparkSession
import org.apache.spark.sql.SparkSession
// Optional: Use the following code below to set the Error reporting
import org.apache.log4j.{Logger, Level}
Logger.getLogger("org").setLevel(Level.ERROR)

// Create a Spark Session Instance
val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()

// Import Kmeans clustering Algorithm
import org.apache.spark.ml.clustering.KMeans
// Load the Wholesale Customers Data
val data = spark.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("Wholesale customers data.csv")

data.show(5)

// Select the following columns for the training set:
// Fresh, Milk, Grocery, Frozen, Detergents_Paper, Delicassen
// Call this new subset feature_data
val cols = data.columns.drop(2)
val feature_data = data.select(cols.head, cols.tail: _*)
// Import VectorAssembler and Vectors
import org.apache.spark.ml.feature.VectorAssembler

// Create a new VectorAssembler object called assembler for the feature
// columns as the input Set the output column to be called features
// There is no Label column
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")

// Use the assembler object to transform the feature_data
// Call this new data training_data
val training_data = assembler.transform(feature_data).select("features")

for (k <- 3 to 30) {
  // Create a Kmeans Model with K=k
  val kmeans = new KMeans().setK(k)
  // Fit that model to the training_data
  val model = kmeans.fit(training_data)
  // Evaluate clustering by computing Within Set Sum of Squared Errors.
  val wssse = model.computeCost(training_data)
  // Shows the result.
  println(s"\nK = $k, WSSSE = $wssse")
}

spark.stop()