/**
  * Created by yren on 12/16/2016.
  */
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().master("local[*]").getOrCreate()
// Use Spark to read in the USA_Housing csv file.
val data = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("USA_Housing.csv")

data.printSchema
data.show(3)

import org.apache.spark.ml.feature.VectorAssembler
// Drop the address column
val cols = data.columns.dropRight(1)
val df = data.select(cols.head, cols.tail: _*).withColumnRenamed("Price", "label")

// Drop the label column
// Below will allow us to join multiple feature columns
// into a single column of an array of feature values
val assembler = new VectorAssembler().setInputCols(cols.dropRight(1)).setOutputCol("features")
// Data needs to be in the form of two columns
// ("label","features") before accepted by Spark
val output = assembler.transform(df).select("label", "features")

// Create a Linear Regression Model object
val lr = new LinearRegression()
// Fit the model
val model = lr.fit(output)

// Print the coefficients and intercept for linear regression
println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

// Summarize the model over the training set and print out some metrics!
// Use the .summary method off the model to create an object
// called trainingSummary
val trainingSummary = model.summary
trainingSummary.residuals.show(5)
trainingSummary.predictions.show(5)

println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"MSE: ${trainingSummary.meanSquaredError}")
println(s"r2: ${trainingSummary.r2}")

spark.stop()