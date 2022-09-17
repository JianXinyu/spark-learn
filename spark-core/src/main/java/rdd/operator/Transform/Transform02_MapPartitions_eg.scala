package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transform02_MapPartitions_eg {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDOperator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4), 2)
    // Example: 获取每个数据分区的最大值
    val mapRDD: RDD[Int] = rdd.mapPartitions(
      itr => {
        // mapPartitions返回迭代器
        List(itr.max).iterator
      }
    )
    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
