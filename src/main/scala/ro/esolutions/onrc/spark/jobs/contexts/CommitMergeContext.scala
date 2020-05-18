package ro.esolutions.onrc.spark.jobs.contexts

import com.typesafe.config.Config
import org.tupol.utils.config.{Configurator, _}
import ro.esolutions.onrc.spark.jobs.services.ResourceService.Database
import ro.esolutions.onrc.spark.jobs.config._
import scalaz.ValidationNel

case class CommitMergeContext(databases: Seq[Database])

object CommitMergeContext extends Configurator[CommitMergeContext] {

  override def validationNel(config: Config): ValidationNel[Throwable, CommitMergeContext] = {
    config.extract[Seq[Database]]("databases")
      .ensure(new IllegalArgumentException("Must have minim one database").toNel)(_.nonEmpty)
      .map(CommitMergeContext(_))
  }
}



