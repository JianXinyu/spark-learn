package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Tranform01_Map {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDOperator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    /*
    map将处理的数据逐条进行映射转换（值或者类型）
     */
    // 1. 声明一个转换函数
    //    def mapFunction(num : Int) : Int = {
    //      num * 2
    //    }
    //    val mapRDD: RDD[Int] = rdd.map(mapFunction)

    // 2. 使用匿名函数
    //    val mapRDD : RDD[Int] = rdd.map((num : Int) => {num * 2})
    // 2.1 函数逻辑只有一行时，可以省略花括号
    //    val mapRDD : RDD[Int] = rdd.map((num : Int) => num * 2)
    // 2.2 参数类型可以自动推算时，可以省略类型
    //    val mapRDD : RDD[Int] = rdd.map((num) => num * 2)
    // 2.3 参数列表只有一个参数时，可以省略小括号
    //    val mapRDD : RDD[Int] = rdd.map(num => num * 2)
    // 2.4 参数在函数逻辑中只出现一次，而且是按照顺序出现，可以用下划线代替
    val mapRDD: RDD[Int] = rdd.map(_ * 2)

    mapRDD.collect().foreach(println)

    //

    sc.stop()
  }
}
