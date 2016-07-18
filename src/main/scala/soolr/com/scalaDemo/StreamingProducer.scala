package soolr.com.scalaDemo

import java.net.ServerSocket

import scala.util.Random

import org.apache.spark.SparkContext
import java.io.PrintWriter

object StreamingProducer {
  def main(args: Array[String]) {
    val random = new Random()

    val listener = new ServerSocket(9999)
    println("Listenint on port:9999")
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run = {
          println("Got client connected from:" + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream, true)
          while (true) {
            Thread.sleep(1000)
            
            out.write("tester,caver,5.99")
            out.write("\n")
            out.flush()
            println(s"+++")
          }
          socket.close()
        }
      }.start()
    }
  }
}