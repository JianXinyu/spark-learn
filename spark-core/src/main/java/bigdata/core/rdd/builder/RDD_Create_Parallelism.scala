package bigdata.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Create_Parallelism {

  def main(args: Array[String]) : Unit = {
    // prepare environment
    // local[*] 表示本机的最大可用核数，不带*的话就是单线程模拟单核
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 1. create RDD from RAM
    // numSlices 表示分区的数量，是按照一个简单算法切分的下标区间.
    // 默认值 defaultParallelism = totalCores，为当前环境的最大可用核数
//    val rdd : RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 2. create RDD from file
    // 读取文件的底层就是Hadoop的读取文件
    // minPartitions 默认值 defaultMinPartitions = min(defaultParallelism, 2)
    // 分区数量的计算方式：
    //  totalSize := 文件的字节数（包括换行符之类）
    //  goalSize = totalSize / minPartitions 表示每个分区的size
    //  分区数: totalSize / goalSize (1.1 原则，如果剩余的字节数 >= 10% goalSize, 就再产生一个新分区)
    val rdd : RDD[String] = sc.textFile("data/1.txt", 2)
    // 如何确定实际的分区数量？将处理的数据保存为分区文件，看输出几个文件
    rdd.saveAsTextFile("output")

    // 分区的数据分配：P38
    // 1. 数据以行为单位进行读取：spark读取文件时采用的是hadoop的方式，一行一行地读取，和字节数没有关系
    // 2. 数据读取时以偏移量为单位，偏移量不会被重复读取
    /*
      1
     */
    // 3. 数据分区的偏移量范围计算

    // close environment
    sc.stop()
  }

}
