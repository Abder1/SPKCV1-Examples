import org.apache.spark.{SparkConf, SparkContext}

object Spark_Core_Tp2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCounter")
    val sc = new SparkContext(conf)
    val fileRdd = sc.textFile("file:///csv/people.csv")
    val delRdd = fileRdd.filter(line => !line.contains("year"))
    val splitRdd = delRdd.map(line => line.split(","))
    val nameCount = splitRdd.map(f => (f(1), f(3).toInt))
    val fiedGroupBy = nameCount.reduceByKey((V1,V2) => V1+V2)
    fiedGroupBy.foreach(println)
  }
}
