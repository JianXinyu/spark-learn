package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transform04_flatMap {
  def main(args: Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator-flatMap")
    val sc = new SparkContext(sparkConf)
   /*
   flatMap: 扁平映射，将处理的数据进行扁平化后再进行映射处理
   */

    // List[Int]
//    val rdd : RDD[List[Int]] = sc.makeRDD(List(
//      List(1, 2), List(3, 4)
//    ))
//    val flatRdd: RDD[Int] = rdd.flatMap(
//      list => {
//        list
//      }
//    )
    // String
    val rdd: RDD[String] = sc.makeRDD(List(
      "Hello Spark", "Hello World"
    ))
    val flatRdd: RDD[String] = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )
    flatRdd.collect().foreach(println)
    sc.stop()
  }
}
