package ro.esolutions.datalake.loader.contexts

import com.typesafe.config.Config
import org.tupol.utils.config.{Configurator, _}
import ro.esolutions.datalake.loader.services.ResourceService.Database
import ro.esolutions.datalake.loader.config._
import scalaz.ValidationNel

case class CommitMergeContext(databases: Seq[Database])

object CommitMergeContext extends Configurator[CommitMergeContext] {

  override def validationNel(config: Config): ValidationNel[Throwable, CommitMergeContext] = {
    config.extract[Seq[Database]]("databases")
      .ensure(new IllegalArgumentException("Must have minim one database").toNel)(_.nonEmpty)
      .map(CommitMergeContext(_))
  }
}



