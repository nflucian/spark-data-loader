package ro.esolutions.datalake

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.tupol.spark.io.FileSourceConfiguration
import org.tupol.spark.io.sources.CsvSourceConfiguration
import org.tupol.spark.io._
import org.tupol.spark.implicits._

package object loader {

  val csvConfig = new CsvSourceConfiguration(options = Map("header"->"true", "delimiter"->"|", "inferSchema"->"false"))

  def createTable(absoluteSchema: String, relativePath: String, partition: Option[String] = None)(implicit spark: SparkSession) = {
    val absolutePath: String = getClass.getResource(relativePath).getPath

    val logData: DataFrame = loadData(absolutePath)

    val writer = logData.write.mode(SaveMode.Overwrite)

    partition.foldLeft(writer)((acc, p) => {
      acc.partitionBy(p)
    }).saveAsTable(absoluteSchema)
  }

  def readCSV(relativePath: String)(implicit spark: SparkSession): DataFrame = {
    val absolutePath: String = getClass.getResource(relativePath).getPath
    loadData(absolutePath)
  }

  private def loadData(path: String)(implicit spark: SparkSession): DataFrame = {
    val input = new FileSourceConfiguration(path, csvConfig)
    spark.source(input).read
  }

}