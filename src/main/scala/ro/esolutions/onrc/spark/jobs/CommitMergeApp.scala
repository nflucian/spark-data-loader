package ro.esolutions.onrc.spark.jobs

import com.typesafe.config.Config
import org.apache.spark.sql.{SaveMode, SparkSession}
import ro.esolutions.onrc.spark.SparkApp
import ro.esolutions.onrc.spark.jobs.contexts.{CommitMergeContext, MergeContext}
import ro.esolutions.onrc.spark.jobs.services.ResourceService
import ro.esolutions.onrc.spark.jobs.services.ResourceService.Resource

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


