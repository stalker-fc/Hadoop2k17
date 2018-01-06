import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.Durations.seconds
import org.apache.spark.storage.StorageLevel
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.regex.Matcher
import java.util.regex.Pattern
import SparkContext._

object Main {

  def main(args: Array[String]) {
    val appName = "SparkLogs"
    val jars = List(SparkContext.jarOfObject(this).get)
    println(jars)
    val conf = new SparkConf().setAppName(appName).setJars(jars)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    run(sc)
  }


  def run(sc: SparkContext) {
    print("\n\n\n******************START AGREGATING DATA******************\n\n\n")
    val ssc = new StreamingContext(sc, seconds(30))
    val stream = ssc.receiverStream(new MyReceiver)
      .foreachRDD(rdd => {
        var res = rdd
          .filter(line => !line.isEmpty())
          .filter(line => line contains "sshd[")
          .filter(line => line contains "Failed password for ")
          .map(line => (line.substring(line.indexOf("Failed password for ") +
            "Failed password for ".length()).substring(
            if (line contains "invalid user") "invalid user".length() else 0,
            line.substring(line.indexOf("Failed password for ") +
              "Failed password for ".length()).lastIndexOf("from")), 1
          )
          )
          .reduceByKey((a, b) => a + b)
          .map(_.swap)
          .sortByKey(false)
          .collect()


        println("\n\n\n*********************************NEW*****************************************")
        println(res.mkString("\n"))
        println("*********************************FINISH**********************************\n\n\n")
      }
      )
    ssc.start()
    ssc.awaitTermination()
  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    override def onStart() {
      new Thread("Socket Receiver") {
        override def run() {
          getData()
        }
      }.start()
    }

    override def onStop() {}


    private def getData() {
      var p = Runtime.getRuntime().exec("/home/motorola/Hadoop2k17/Task10/generate");
      val reader = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))

      var userInput: String = null
      userInput = reader.readLine()
      var i: Int = 0
      while (!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
    }
  }

}