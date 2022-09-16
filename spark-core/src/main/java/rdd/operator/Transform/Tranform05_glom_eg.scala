package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Tranform05_glom_eg {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator-glom")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // flatMap: List => Int
    // glom: Int => Array
    /*
    glom: 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
     */
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    // 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val maxRDD: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )
    println(maxRDD.collect().sum)
  }
}
