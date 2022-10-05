package broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object broadcast {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("broadcast")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))

    // join会导致数据量几何增长，并且影响shuffle性能，不推荐使用
//    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    joinRDD.collect().foreach(println)
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    rdd1.map {
      case (w, c) => {
        val l: Int = map.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)

    // 但上面的方法在数据量很大的时候有问题
    /*
    闭包数据是以Task为单位发送的，每个Task都包含数据。这样可能会导致一个 Executor 包含大量重复数据，并且占用大量内存。
    Executor 其实就是一个JVM，在启动时会自动分配内存。完全可以将 Task 中的闭包数据放到 Executor 内存中，达到共享目的。
    Spark 中的广播变量就可以将闭包数据放到 Executor 的内存中。
    广播变量不能修改，是分布式共享只读变量
     */
    sc.stop()
  }
}
