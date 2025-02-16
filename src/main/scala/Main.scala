package in.aminoxix.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkFiles

object Main {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("spark-scala")
      .master("local[*]") // Use all available cores for local execution
      .config("spark.driver.bindAddress", "127.0.0.1") // Correct binding address
      .getOrCreate()

    // Add the file from the URL to Spark's file system
    val fileUrl = "https://raw.githubusercontent.com/pandas-dev/pandas/master/doc/data/titanic.csv" // for link
    spark.sparkContext.addFile(fileUrl) // for link

    // Read the file using the SparkFiles utility to get the local path
    val localFilePath = SparkFiles.get("titanic.csv") // for link
    val dataframe = spark.read
      .option("header", value = true)
//      .csv(localFilePath) // for link
      .csv("data/Formula1_2024season_raceResults.csv") // for local

    // Show the dataframe
    dataframe.show()
  }
}
