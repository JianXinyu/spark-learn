package rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Persist03_persist {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hello Spark"))
    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(
      word => {
        println("$$$$$$$$$$$$$$$")
        (word, 1)
      }
    )

    /*
     RDD对象的持久化操作不一定是为了数据重用，在数据执行较长、数据重要的场合也可以采用持久化操作。
     持久化是在 action operator 才执行
     即使存在disk上，也是临时文件，job执行完会被删除
    */
    // cache = persist(StorageLevel.MEMORY_ONLY)
    mapRDD.cache()

    val reduceRDD = mapRDD.reduceByKey(_+_)
    reduceRDD.collect().foreach(println)

    println("----------------------------------------")

    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
