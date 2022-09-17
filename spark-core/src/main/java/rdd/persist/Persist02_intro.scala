package rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Persist02_intro {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hello Spark"))
    val flatRDD = rdd.flatMap(_.split(" "))

    /*
    !!!!!! RDD 中不存储数据 !!!!!!
    如果一个RDD被重复使用，那么需要从头执行一遍获取数据。
    RDD对象可以重用，但数据无法重用。
     */
    val mapRDD = flatRDD.map(
      word => {
        println("$$$$$$$$$$$$$$$")
        (word, 1)
      }
    )

    val reduceRDD = mapRDD.reduceByKey(_+_)
    reduceRDD.collect().foreach(println)

    println("----------------------------------------")

    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
