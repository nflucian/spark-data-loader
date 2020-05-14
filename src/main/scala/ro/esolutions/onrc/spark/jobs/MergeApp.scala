package ro.esolutions.onrc.spark.jobs

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import ro.esolutions.onrc.spark.SparkApp
import ro.esolutions.onrc.spark.jobs.services.ResourceService.{Database, Resource}
import ro.esolutions.onrc.spark.jobs.contexts.MergeContext
import ro.esolutions.onrc.spark.jobs.services.ResourceService

object MergeApp extends SparkApp[MergeContext, Unit]{

  override def createContext(config: Config): MergeContext = MergeContext(config).get

  override def run(implicit spark: SparkSession, context: MergeContext): Unit = {
    val resources: Seq[Resource] = ResourceService().current(context.databases, context.date)

    resources.map(merge).foreach {case (a, b, c) => write(a, b, c)}
  }

  private def write(resource:Resource, merged:DataFrame, delta:DataFrame) : Unit = {
      merged.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(resource.absoluteTempSchema)
      delta.write.mode(SaveMode.Append).format("parquet").saveAsTable(resource.absoluteTempSchema)

      delta.unpersist()
  }

  private def merge(resource: Resource)(implicit spark: SparkSession, context: MergeContext): (Resource, DataFrame, DataFrame) = {
      val nullColumn = lit(null).cast(StringType)
      val sink = spark.table(resource.absoluteCurrentSchema)
      val source = spark.table(resource.absoluteSchema)
        .where(col("TECHNICAL_DATE").equalTo(context.date))
        .drop("TECHNICAL_DATE")

      val onlySinkCol = sink.columns.diff(source.columns)
      val onlySourceCol = source.columns.diff(sink.columns)

      val current = onlySourceCol.foldLeft(sink)((df, name) => df.withColumn(name, nullColumn))
      val delta = onlySinkCol.foldLeft(source)((df, name) => df.withColumn(name, nullColumn)).cache()

      val join = current.join(delta.select("ID"), current("ID") === delta("ID"), "left_anti")

      (resource, join, delta)
  }


}