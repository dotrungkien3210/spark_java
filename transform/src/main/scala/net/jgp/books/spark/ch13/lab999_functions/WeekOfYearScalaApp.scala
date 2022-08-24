package net.jgp.books.spark.ch13.lab999_functions

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * week of year function.
 *
 * @author rambabu.posa
 */
object WeekOfYearScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    val spark: SparkSession = SparkSession.builder
      .appName("weekofyear function")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("imferSchema", true)
      .load("data/functions/dates.csv")

    val df2 = df.withColumn("weekofyear", F.weekofyear(F.col("date_time")))

    df2.show(5, false)
    df2.printSchema()

    spark.stop
  }

}
