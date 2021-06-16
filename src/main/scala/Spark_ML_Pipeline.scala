import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

object Spark_ML_Pipeline {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val ref = spark.createDataFrame(Seq(
      (0L, "This is spark book", 1.0),
      (1L, "HADOOP MapReduce", 1.0),
      (3L, "Kafka Streaming", 1.0),
      (4L, "Java JDK11", 0.0)
    )).toDF("id", "book", "label")

    val tokenizer = new Tokenizer().setInputCol("book")
      .setOutputCol("words")
    val hashingTF = new HashingTF().setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val logitreg = new LogisticRegression().setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF,logitreg))

    val model = pipeline.fit(ref)

    val test = spark.createDataFrame(Seq(
      (5L, "spark book"),
      (6L, "HADOOP"),
      (7L, "Kafka"),
      (8L, "XXXXXXXXXXXXXX")
    )).toDF("id", "book")

    val transformed = model.transform(test)
      .select("id", "book", "probability", "prediction")
      .collect()

    transformed.foreach{
      case Row(id: Long, book: String, prob: Vector, prediction: Double) =>
        println(s"($id, $book) --> prob=$prob, prediction=$prediction")
    }
  }

}
