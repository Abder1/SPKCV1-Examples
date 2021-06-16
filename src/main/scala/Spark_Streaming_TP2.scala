import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Spark_Streaming_TP2 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Streaming")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    val schema = new StructType().add("year", "string")
      .add("name", "string")
      .add("country", "string")
      .add("count", "string")

    val csvDF = spark.readStream.option("mode", "DROPMALFORMED").option("sep", ",").schema(schema).csv("file:///csv2")
    csvDF.printSchema()
    csvDF.writeStream.outputMode("append").format("console").start().awaitTermination()
  }

}
