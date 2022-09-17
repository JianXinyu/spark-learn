package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transform05_glom {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator-glom")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // flatMap: List => Int
    // glom: Int => Array
    /*
    glom: 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
     */
    val glomRdd: RDD[Array[Int]] = rdd.glom()
    glomRdd.collect().foreach(data => data.mkString(","))
  }
}
