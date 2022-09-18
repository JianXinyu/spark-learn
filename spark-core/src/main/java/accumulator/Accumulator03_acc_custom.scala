package accumulator

import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Accumulator03_acc_custom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("accumulator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("Hello", "Spark", "World"))

    // 创建累加器对象
    val wcAcc = new MyAccumulator()
    // 向Spark注册
    sc.register(wcAcc, "wordCountAcc")

    rdd.foreach(
      word => {
        // 使用累加器
        wcAcc.add(word)
      }
    )

    // 获取累加器累加的结果
    println(wcAcc.value)

    sc.stop()
  }
  /*
  自定义数据累加器：word count
  1. 继承 AccumulatorV2，定义泛型：
    - IN：累加器输入的数据类型
    - OUT：累加器返回的数据类型
  2. 重写抽象方法
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()

    // 判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    // 复制一个新累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    // 重制累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    // 获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }

    // Driver 合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach {
        case (word, cnt) => {
          val newCnt = map1.getOrElse(word, 0L) + cnt
          map1.update(word, newCnt)
        }
    }
    }

    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
