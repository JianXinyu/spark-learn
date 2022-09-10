package bigdata.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]) : Unit = {
    // Application
    // Spark framework
    // create connection with Spark framework
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // execute job operations
    val lines : RDD[String] = sc.textFile("data")
    val words : RDD[String] = lines.flatMap(_.split(" "))
    var wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val word2count = wordGroup.map{
      case ( word, list ) => {
        (word, list.size)
      }
    }
    var array: Array[(String, Int)] = word2count.collect()
    array.foreach(println)
    // close connection
    sc.stop()
  }
}
