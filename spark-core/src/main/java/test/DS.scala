package test

class DS extends Serializable {
  val data: List[Int] = List(1,2,3,4)
  val logic : (Int) => Int = _ * 2
}
