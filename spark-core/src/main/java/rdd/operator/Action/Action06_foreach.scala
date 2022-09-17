package rdd.operator.Action

import org.apache.spark.{SparkConf, SparkContext}

object Action06_foreach {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("action-foreach")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // collect收集后打印, i.e., Driver 端内存集合循环遍历
    rdd.collect().foreach(println)
    println("********************")
    // 分布式打印，i.e., Executor 端内存数据打印
    // 并行，分区顺序不确定
    rdd.foreach(println)

    sc.stop()
  }

}
