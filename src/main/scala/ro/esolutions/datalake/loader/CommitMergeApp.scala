package ro.esolutions.datalake.loader

import com.typesafe.config.Config
import org.apache.spark.sql.{SaveMode, SparkSession}
import ro.esolutions.datalake.loader.contexts.CommitMergeContext
import ro.esolutions.datalake.loader.services.ResourceService
import ro.esolutions.datalake.loader.services.ResourceService.Resource
import ro.esolutions.datalake.spark.SparkApp

object CommitMergeApp extends SparkApp[CommitMergeContext, Unit] {

  override def createContext(config: Config): CommitMergeContext = CommitMergeContext(config).get

  override def run(implicit spark: SparkSession, context: CommitMergeContext): Unit = {
    val resources: Seq[Resource] = ResourceService().temp(context.databases)

    resources.map(resource => {
      val df = spark.table(resource.absoluteTempSchema)
      (resource, df)
    }).foreach { case (resource, df) =>
      df.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(resource.absoluteCurrentSchema)
    }
  }
}


