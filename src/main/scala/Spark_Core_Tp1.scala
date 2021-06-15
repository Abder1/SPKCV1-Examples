import org.apache.spark.{SparkConf, SparkContext}

object Spark_Core_Tp1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCounter")
    val sc = new SparkContext(conf)
    val fileRdd = sc.textFile("file:///csv/people.csv")
    val delRdd = fileRdd.filter(line => !line.contains("year"))
    val splitRdd = delRdd.map(line => line.split(","))
    val fieldRdd = splitRdd.map(f => (f(2), f(1)))
    val groupByRdd = fieldRdd.groupByKey()
    groupByRdd.foreach(println)
  }
}
