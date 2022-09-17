package rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Persist06_difference {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(sparkConf)
    /*
    cache: 会在 lineage 中添加新的 dependency，一旦出现问题，可以重头读取数据。
    checkpoint: 会切断 lineage，重建新的 lineage，等同于改变数据源
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
//    mapRDD.cache()
    mapRDD. checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_+_)
    reduceRDD.collect().foreach(println)

    println("----------------------------------------")

    println(reduceRDD.toDebugString)
    sc.stop()
  }
}
