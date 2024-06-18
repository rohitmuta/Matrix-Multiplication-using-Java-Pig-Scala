//Rohit M //
//Run in scala in databricks cluster//
import org.apache.spark.sql.functions.{sum, col, expr, element_at}
import org.apache.spark.sql.SparkSession

def multiplyMatrices(spark: SparkSession): Unit = {
  try {
    // Define the matrices as DataFrames
    val matrix1 = Seq(
      (1, 2),
      (3, 4)
    ).toDF("a", "b")

    val matrix2 = Seq(
      (5, 6),
      (7, 8)
    ).toDF("c", "d")

    // Multiply the matrices using Spark SQL
    val result = matrix1
      .crossJoin(matrix2)
      .withColumn("product", expr("a * c + b * d"))
      //.withColumn("product2", expr("a * c + b * element_at(collect_list(c) over(), 2)"))
      .groupBy("a")
      .pivot("c")
      .agg(sum("product"))
      .orderBy("a")

    // Display the result
    result.show()
  } catch {
    case e: Exception =>
      // Log the error
      println(s"Error: ${e.getMessage}")
  }
}

multiplyMatrices(spark)