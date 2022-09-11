package bigdata.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Create {
  def main(args: Array[String]) : Unit = {
    // prepare environment
    // local[*] 表示本机的最大可用核数，不带*的话就是单线程模拟单核
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // create RDD
    // 1. create RDD from RAM
    val seq = Seq[Int] (1,2,3,4)
//    val rdd : RDD[Int] = sc.parallelize(seq)
    val rdd : RDD[Int] = sc.makeRDD(seq) // == parallelize

    // 2. create RDD from file:
    // - textFile: 以行为单位读取数据
    // - wholeTextFile： 以文件为单位读取数据
    // 2.1 file name: absolute path or relative path, default, current environment's root directory
//    val rdd : RDD[String] = sc.textFile("data/1.txt")
    // 2.2 directory name:
//    val rdd : RDD[String] = sc.textFile("data")
    // 2.3 regular expression
//    val rdd : RDD[String] = sc.textFile("data/*.txt")
    // 2.4 distribute file system path, e.g. HDFS
//    val rdd : RDD[String] = sc.textFile("hdfs://linux:8020/test.txt")
    //
    rdd.collect().foreach(println)

    // close environment
    sc.stop()
  }

}
