package bigdata.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {

  def main(args: Array[String]) : Unit = {
    // start server, receive data
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    val out1: OutputStream = client1.getOutputStream
    val objOut1: ObjectOutputStream = new ObjectOutputStream(out1)

    val task = new DS()
    val subTask1 = new Task()
    subTask1.logic = task.logic
    subTask1.data = task.data.take(2)

    objOut1.writeObject(subTask1)
    objOut1.flush()

    objOut1.close()
    out1.close()
    client1.close()


    val out2: OutputStream = client2.getOutputStream
    val objOut2: ObjectOutputStream = new ObjectOutputStream(out2)

    val subTask2 = new Task()
    subTask2.logic = task.logic
    subTask2.data = task.data.takeRight(2)

    objOut2.writeObject(subTask2)
    objOut2.flush()

    objOut2.close()
    out2.close()
    client2.close()

    println("Data is sent.")
  }
}
