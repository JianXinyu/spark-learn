package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Tranform03_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDOperator")
    val sc = new SparkContext(sparkConf)
    /*
    mapPartitionsWithIndex: 将待处理的数据以分区为单位发送到计算节点进行处理, 同时可以拿到当前分区的索引
     */
    // 拿到某个分区的数据
//    val rdd = sc.makeRDD(List(1,2,3,4), 2)
//    val mpiRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
//      (index, itr) => {
//        if (index == 1) {
//          itr
//        }
//        else {
//          Nil.iterator
//        }
//      }
//    )
    // 查看每个数据在哪个分区
    val rdd = sc.makeRDD(List(1,2,3,4))
    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, itr) => {
        itr.map(
          num => {
            (index, num)
          }
        )
      }
    )
    mpiRDD.collect().foreach(println)

    sc.stop()
  }
}
