package bigdata.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor2 {
  def main(args: Array[String]): Unit = {
    // start server, receive data
    val server = new ServerSocket(8888)
    println("Server started, waiting for data...")

    // waiting for client connection
    var client : Socket = server.accept()
    var in: InputStream = client.getInputStream
    val objIn = new ObjectInputStream(in)
    val task: Task = objIn.readObject().asInstanceOf[Task]
    val ints: List[Int] = task.compute()
    println("result of compute node 8888 is " + ints)
    objIn.close()
    in.close()
    client.close()
    server.close()
  }
}
