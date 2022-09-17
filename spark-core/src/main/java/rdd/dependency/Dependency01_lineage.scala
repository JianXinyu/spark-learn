package rdd.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Dependency01_lineage {
  def main(args: Array[String]): Unit = {
    // create connection with Spark framework
    val sparkConf = new SparkConf().setMaster("local").setAppName("Lineage")
    val sc = new SparkContext(sparkConf)
    /*
    每个RDD会记录Lineage，便于重新运算、恢复丢失的数据
     */
    // execute job operations
    val fileRDD: RDD[String] = sc.textFile("data/word.txt")
    // print lineage
    // 前面的数字表示分区
    println(fileRDD.toDebugString)
    println("-----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.toDebugString)
    println("-----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    println(mapRDD.toDebugString)
    println("-----------------------")

    val word2count: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    // 这里的lineage出现了中断，是因为有shuffle操作
    println(word2count.toDebugString)
    println("-----------------------")

    val array: Array[(String, Int)] = word2count.collect()
    array.foreach(println)
    // close connection
    sc.stop()
  }
}
