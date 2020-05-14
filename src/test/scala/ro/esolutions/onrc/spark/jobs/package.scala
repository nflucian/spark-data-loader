package ro.esolutions.onrc.spark

import org.apache.spark.sql._

package object jobs {

  def createTable(absoluteSchema: String, relativePath: String, partition: Option[String] = None)(implicit spark: SparkSession) = {
    val absolutePath: String = getClass.getResource(relativePath).getPath

    val tableDF = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema", "false")
      .csv(absolutePath)
    val writer = tableDF.write.mode(SaveMode.Overwrite)

    partition.foldLeft(writer)((acc, p) => {
      acc.partitionBy(p)
    }).saveAsTable(absoluteSchema)
  }

  def readCSV(relativePath: String)(implicit spark: SparkSession): DataFrame = {
    val absolutePath: String = getClass.getResource(relativePath).getPath

    spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema", "false")
      .csv(absolutePath)
  }
}
