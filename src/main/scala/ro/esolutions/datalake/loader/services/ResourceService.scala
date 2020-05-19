package ro.esolutions.datalake.loader.services

import org.apache.spark.sql.SparkSession
import ro.esolutions.datalake.loader.services.ResourceService.{Database, Resource}

class ResourceService()(implicit spark: SparkSession) {

  def current(databases: Seq[Database], date: String): Seq[Resource] = {
    databases.flatMap(db => {
      tables(db.name).filter(hasPartition(_, date)).map(table => Resource(db, table))
    })
  }

  def temp(databases: Seq[Database]): Seq[Resource] = {
    databases.flatMap(db => {
      tables(db.temp).map(table => Resource(db, table))
    })
  }

  private def tables(db: String): Seq[String] = {
    spark.sql(s"use ${db}")
    spark.sql(s"show tables").select("tableName").collect().map(_.getAs[String]("tableName"))
  }

  private def hasPartition(table: String, date: String): Boolean = {
    val count = spark.sql(s"show partitions $table partition(TECHNICAL_DATE='$date')").count()
    count > 0
  }
}

object ResourceService {

  case class Database(name: String) {
    require(name.nonEmpty, "database name should not be empty")

    val current = s"${name}_current"
    val temp = s"${name}_temp"
  }

  case class Resource(db: Database, table: String) {
    require(table.nonEmpty, "table should not be empty")

    val absoluteSchema = s"${db.name}.$table"
    val absoluteCurrentSchema = s"${db.current}.$table"
    val absoluteTempSchema = s"${db.temp}.$table"
  }

  def apply()(implicit spark: SparkSession): ResourceService = new ResourceService()

}
