package rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Dependency02_dep {
  def main(args: Array[String]): Unit = {
    // create connection with Spark framework
    val sparkConf = new SparkConf().setMaster("local").setAppName("Dependencies")
    val sc = new SparkContext(sparkConf)

    /*
    新RDD的一个分区数据依赖于旧RDD的一个分区，称为 OneToOne Dependency(Narrow Dependency)
    新RDD的一个分区数据依赖于旧RDD的多个分区，称为 Shuffle Dependency ()
     */
    // execute job operations
    val fileRDD: RDD[String] = sc.textFile("data/word.txt")
    println(fileRDD.dependencies)
    println("-----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("-----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    println(mapRDD.dependencies)
    println("-----------------------")

    val word2count: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println(word2count.dependencies)
    println("-----------------------")

    val array: Array[(String, Int)] = word2count.collect()
    array.foreach(println)
    // close connection
    sc.stop()
  }
}
