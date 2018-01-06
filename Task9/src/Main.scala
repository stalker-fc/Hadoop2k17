import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import SparkContext._
import java.io.File
import java.net.URI
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.Scanner
import java.nio.file.Paths


object Main {

  def main(args: Array[String]) {
    val appName = "SparkSpec"
    val jars = List(SparkContext.jarOfObject(this).get)
    println(jars)
    val conf = new SparkConf().setAppName(appName).setJars(jars)
    val sc = new SparkContext(conf)
    if (args.length < 2) {
      println(args.mkString(","))
      println("ERROR. Please, specify input and output directories.")
    } else {
      val inputDir = args(0)
      val outputDir = args(1)
      println("Input directory: " + inputDir)
      println("Output directory: " + outputDir)
      run(sc, inputDir, outputDir)
    }
  }

  val GLUSTERFS_MOUNT_PATH = "/gfs"
  val FILENAME_PATTERN = "([0-9]{5})([dijkw])([0-9]{4})\\.txt\\.gz".r
  val LINE_PATTERN = "([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)".r
  val VARIABLE_NAMES = Array("d", "i", "j", "k", "w")

  def toGlusterfsPath(path: String): String = path.replace(GLUSTERFS_MOUNT_PATH, "")

  def toGlusterfsPath(file: File): String = toGlusterfsPath(file.getAbsolutePath())

  def toFloatArray(line: String) = line.split("\\s+").filter(!_.isEmpty).map(word => word.toFloat)

  def fmtArr(x: Array[Float]) = "[" + x.mkString(", ") + "]"

  def splitLines(station: String, v: String, lines: Array[String]) = {
    var result: Array[(String, String, String)] = new Array[(String, String, String)](lines.length);
    for (i <- 0 to lines.length - 1) {
      result(i) = (station, v, lines(i));
    }
    result
  }

  def run(sc: SparkContext, inputDir: String, outputDir: String) {
    sc
      .wholeTextFiles(inputDir)
      .map(pair => (Paths.get(pair._1).getFileName().toString(), pair._2))
      .map(pair => (FILENAME_PATTERN.pattern.matcher(pair._1), pair._2))
      .filter(pair => pair._1.matches())
      .map(pair => ((pair._1.group(1), pair._1.group(2)), pair._2))
      .map(pair => (pair._1, pair._2.split("\\n")))
      .flatMap(pair => splitLines(pair._1._1, pair._1._2, pair._2))
      .map( pair => (LINE_PATTERN.pattern.matcher(pair._3), pair) )
      .filter( pair => pair._1.matches() )
      .map(    pair => (   ( pair._2._1,  pair._1.group(1), pair._2._2 ),( pair._1.group(2))  )     )
      .groupByKey()
      .map(pair => ( (pair._1._1, pair._1._2), ( pair._1._3, pair._2.iterator.next())) )
      .groupByKey()
      .filter( pair => pair._2.size == 5 )
      .map(pair => (pair._1._2, pair._2.toMap))
      .map(pair => (pair._1, pair._2.get("i").getOrElse(""), pair._2.get("j").getOrElse(""),
        pair._2.get("k").getOrElse(""), pair._2.get("w").getOrElse(""), pair._2.get("d").getOrElse("")))

      .map(pair => (pair._1, fmtArr(toFloatArray(pair._2)), fmtArr(toFloatArray(pair._3)),
        fmtArr(toFloatArray(pair._4)), fmtArr(toFloatArray(pair._5)), fmtArr(toFloatArray(pair._6))))
      .map(pair => pair._1 + "\t[i=" + pair._2 + ",j=" + pair._3 + ",k=" + pair._4 +
        ",w=" + pair._5 + ",d=" + pair._6 + "]")
      .saveAsTextFile(outputDir)
  }

}
