package accumulator

import org.apache.spark.{SparkConf, SparkContext}

object Accumulator02_acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4,5))

    /*
    分布式共享只写变量
    只写：executors 之间不能互相访问
     */
    // 声明累加器
    val sum = sc.longAccumulator("sum")
    // 使用累加器
//    rdd.foreach(
//      num => {
//        sum.add(num)
//      }
//    )
    val mapRDD = rdd.map(
      num => {
        sum.add(num)
        num
      }
    )

    // 获取累加器的值
    /*
    Problem：
    少加：Transform operator 中调用累加器，如果没有 Action operator，那么不会执行
    多加：多次调用 Action operator 可能会多加
     */
    mapRDD.collect()
    mapRDD.collect()
    println("sum = " + sum.value)
    
    sc.stop()
  }
  }
