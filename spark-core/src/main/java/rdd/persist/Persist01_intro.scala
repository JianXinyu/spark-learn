package rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Persist01_intro {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hello Spark"))
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map((_, 1))

    val reduceRDD = mapRDD.reduceByKey(_+_)
    reduceRDD.collect().foreach(println)

    println("----------------------------------------")

//    val rdd1: RDD[String] = sc.makeRDD(List("Hello World", "Hello Spark"))
//    val flatRDD1 = rdd1.flatMap(_.split(" "))
//    val mapRDD1 = flatRDD1.map((_, 1))
//    val groupRDD = mapRDD1.groupByKey()

    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
