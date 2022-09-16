package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Tranform06_groupBy {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator-groupBy")
    val sc = new SparkContext(sparkConf)
    /*
    groupBy: 将数据源中的每个数据进行分组判断，根据返回的分组key进行分组，相同的key会放到同一组
    分组和分区没有必然的关系。分区默认不变，但是数据会被打乱重新组合。这个操作我们称为shuffle.
    极限情况下，数据可能被分在同一个分区中。一个组的数据在一个分区中，但是并不是说一个分区中只有一个组。
     */

//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
//
//    def groupFunction(num : Int) : Int = {
//      num % 2
//    }

    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Spark", "Hadoop", "World"), 2)

    val groupRDD: RDD[(Any, Iterable[String])] = rdd.groupBy(_.charAt(0))
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
