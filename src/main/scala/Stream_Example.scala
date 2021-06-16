import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Stream_Example{
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Spark Streaming")
      .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    val line = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val word = line.flatMap(_.split(" "))
    word.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
