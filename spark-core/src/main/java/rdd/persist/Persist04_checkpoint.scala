package rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Persist04_checkpoint {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(sparkConf)

    // checkpoint要设置路径
    sc. setCheckpointDir ("./")

    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hello Spark"))
    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(
      word => {
        println("$$$$$$$$$$$$$$$")
        (word, 1)
      }
    )

    // 注意这里又执行了一遍
    mapRDD. checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_+_)
    reduceRDD.collect().foreach(println)

    println("----------------------------------------")

    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
