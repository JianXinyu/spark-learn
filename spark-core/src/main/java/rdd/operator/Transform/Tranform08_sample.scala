package rdd.operator.Transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Tranform08_sample {
  def main(args : Array[String]) : Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("operator-sample")
    val sc = new SparkContext(sparkConf)
    /* P56
    sample: 根据指定的规则从数据集中抽取数。Usage：数据倾斜时使用
    withReplacement: 抽取到的数据是否放回
    - false 不放回，伯努利取样，具体实现：根据种子和随机算法算出一个数和fraction比较，小于fraction则要，大于则不
    要。
      fraction: [0, 1], 每个元素被抽到的几率
    - true 放回，泊松取样。
      fraction: 每个元素被抽取的可能次数
    seed: default system time
    */
    val rdd : RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    println(rdd.sample(
      withReplacement = true,
      2
    ).collect().mkString("Array(", ", ", ")"))
    sc.stop()
  }

}
