import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphXExample {
  def main(args: Array[String]): Unit = {
  val spark: SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExample")
    .getOrCreate()
    val sc = spark.sparkContext
  spark.sparkContext.setLogLevel("ERROR")

    val users: RDD[(VertexId, (String, String))]=
      sc.parallelize(Array((3L, ("Rishi", "Student")), (7L, ("Vicent", "Tech-lead")), (5L, ("Pascale", "Prof")),
        (2L, ("peter", "Student")), (4L, ("john", "Prof"))))

    val relationShips: RDD[Edge[String]]=
      sc.parallelize(Array(
        Edge(3L, 4L, "collab"),
        Edge(5L, 3L, "Tuteur"),
        Edge(2L, 5L, "Collegue"),
        Edge(2L, 7L, "Collegue"),
        Edge(8L, 3L, "Tuteur")
      ))

    val defaultUser = ("Unknown", "Unknown")
    val graph = Graph(users, relationShips, defaultUser)

    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " est le " + triplet.attr + " de " + triplet.dstAttr._1
    ).collect().foreach(println)
}
}
