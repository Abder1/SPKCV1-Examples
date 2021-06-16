import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object Spark_ML_Correlation {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._

    /*val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3,-2.0))),
      Vectors.sparse(4, Seq((0, 9.0), (3,1.0))),
      Vectors.sparse(4, Seq((0, 9.0), (3,1.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0)
    )

    val df = data.map(Tuple1.apply).toDF("features")
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head

    println(s"Person correlation matrix :\n $coeff1")*/

    val seqDataframe = spark.createDataFrame(Seq(
      (0, "Bonjour le monde"),
      (1, "Hello World"),
      (2, "GoodBy,World,See,You")
    )).toDF("id","phrase")

    val tokenizer = new Tokenizer().setInputCol("phrase").setOutputCol("mots")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("phrase")
      .setOutputCol("mots")
      .setPattern("\\W")

    val seqTokenize = regexTokenizer.transform(seqDataframe)
    seqTokenize.select("phrase", "mots").show(false)

    spark.stop()
  }

}
