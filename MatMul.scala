/*** Matrix Multiplication in scala on spark ***/
/***  How to execute: ~/spark-3.2.3-bin-hadoop2.7/bin/spark-submit
--class Multiply scalatest5.jar inputA inputB output_path (in terminal - need hadoop-spark package) ***/
/*** Note: output.txt is just a copy of the output generated ***/



import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.PrintWriter

object Multiply {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)
    //running Spark on a standalone cluster or in local mode, you can omit the master URL and Spark will default to "local[*]".

    conf.set("spark.logConf", "false")
    conf.set("spark.eventLog.enabled", "false")

    //Matrix M RDD
    val rdd_m = sc.textFile(args(0)).map(line => {
      val readLine = line.split(",")
      (readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
    })

    //Matrix N RDD
    val rdd_n = sc.textFile(args(1)).map(line => {
      val readLine = line.split(",")
      (readLine(0).toInt, readLine(1).toInt, readLine(2).toDouble)
    })

    val multiply_matrix = rdd_m.map(rdd_m => (rdd_m._2, rdd_m)).join(rdd_n.map(rdd_n => (rdd_n._1, rdd_n)))
      .map { case (k, (rdd_m, rdd_n)) =>
        ((rdd_m._1, rdd_n._2), (rdd_m._3 * rdd_n._3))
      }

    val reduceValues = multiply_matrix.reduceByKey((x, y) => (x + y))

    val sort = reduceValues.sortByKey(true, 0)

    //sort.map { case ((i, j), value) => s"$i,$j,$value" }.saveAsTextFile(s"${args(2)}/output.txt")

    //output path
    val outputPath = args(2)
    val outputPath2 = args(2) + "/part-00000"

    // write the output to the specified file path
    sort.map { case ((i, j), value) => s"$i,$j,$value" }.saveAsTextFile(outputPath)

    // create a new file with the specified copy path and copy the contents of the output file
    val copyPath = args(2) + "/output.txt"
    val pw = new PrintWriter(copyPath)
    for (line <- scala.io.Source.fromFile(outputPath2).getLines) {
      pw.write(line + "\n")
    }
    pw.close()

    sc.stop()

  }
}
