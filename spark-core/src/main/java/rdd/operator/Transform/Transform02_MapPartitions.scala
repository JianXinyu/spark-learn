package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transform02_MapPartitions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDOperator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4), 2)
    /*
     mapPartitions 可以以分区为单位进行数据转换操作，但是会将整个分区的数据加载到内存中进行引用。
     处理完的数据是不会被释放的，存在对象的引用。在内存较小而数据量较大的情况下，容易出现内存溢出。
     */

    val mapRDD: RDD[Int] = rdd.mapPartitions(
      itr => {
        println(">>>>>>")
        itr.map(_ * 2)
      }
    )
    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
