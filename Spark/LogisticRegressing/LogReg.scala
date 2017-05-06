/**
  * Created by yren on 12/17/2016.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.log4j.{Logger, Level}

Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder()
                        .master("local[*]")
                        .getOrCreate()

val data = spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .format("csv")
                .load("titanic.csv")

data.printSchema()
data.show(5)

// Let's only pick up these columns: Survived, Pclass, Sex, Age, SibSp, Parch, Fare, Embarked
val index = Array(1, 2, 4, 5, 6, 7, 9, 11)
val cols = index.map(data.columns)
val dfAll = data.select(cols.head, cols.tail: _*).withColumnRenamed("Survived", "label")
val df = dfAll.na.drop()

import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, OneHotEncoder}

// Deal with Categorical Columns
val sexIndexer = new StringIndexer().setInputCol("Sex").setOutputCol("SexIndex")
val embarkIndexer = new StringIndexer().setInputCol("Embarked").setOutputCol("EmbarkIndex")

val sexEncoder = new OneHotEncoder().setInputCol("SexIndex").setOutputCol("SexVec")
val embarkEncoder = new OneHotEncoder().setInputCol("EmbarkIndex").setOutputCol("EmbarkVec")

// Assemble every feature together to be "features"
val newCols = cols.updated(2, "SexVec").updated(7, "EmbarkVec").drop(1)
val assembler = new VectorAssembler().setInputCols(newCols).setOutputCol("features")

// Split the Data
val Array(train, test) = df.randomSplit(Array(0.7, 0.3), seed = 12345)

import org.apache.spark.ml.Pipeline

val lr = new LogisticRegression()
val stages = Array(sexIndexer, embarkIndexer, sexEncoder, embarkEncoder, assembler, lr)
val pipeline = new Pipeline().setStages(stages)

val model = pipeline.fit(train)
val results = model.transform(test)

// MODEL EVALUATION
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import spark.implicits._
// Need to convert to RDD to use this
val predictionAndLabels = results.select("prediction", "label").as[(Double, Double)].rdd
val metrics = new MulticlassMetrics(predictionAndLabels)

// Confusion matrix
// 114.0  16.0
// 27.0   61.0
println("\nConfusion matrix:")
println(metrics.confusionMatrix)

spark.stop()