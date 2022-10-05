package rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Persist05_difference {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(sparkConf)
    /*
    cache: 将数据保存在内存中
    persist: 临时保存在磁盘，涉及IO，性能低但安全。job 执行完毕，临时保存的文件会丢失。
    checkpoint: 长久保存在磁盘，涉及IO，性能低但安全。
                为了保证数据安全，所以会独立执行job。
                为了提高效率，通常和 cache 连用
     */
    sc. setCheckpointDir ("./cp_dir")

    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hello Spark"))
    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(
      word => {
        println("$$$$$$$$$$$$$$$")
        (word, 1)
      }
    )

    // 增加缓存 避免再重新跑一个 job 做 checkpoint
    mapRDD.cache()
    mapRDD.checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_+_)
    reduceRDD.collect().foreach(println)

    println("----------------------------------------")

    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
