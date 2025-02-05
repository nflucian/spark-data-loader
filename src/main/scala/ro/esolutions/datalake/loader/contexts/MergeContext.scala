package ro.esolutions.datalake.loader.contexts

import com.typesafe.config.Config
import org.tupol.utils.config.{Configurator, _}
import ro.esolutions.datalake.loader.services.ResourceService.Database
import ro.esolutions.datalake.loader.utils._
import ro.esolutions.datalake.loader.config._
import scalaz.ValidationNel
import scalaz.syntax.applicative._

case class MergeContext(date: String, databases: Seq[Database])

object MergeContext extends Configurator[MergeContext] {

  override def validationNel(config: Config): ValidationNel[Throwable, MergeContext] = {
    config.extract[String]("date").ensure(
      new IllegalArgumentException("You need to add the date with <yyyy-MM-dd> format!").toNel)(isValidLocalDate) |@|
    config.extract[Seq[Database]]("databases").ensure(
      new IllegalArgumentException("Must have minim one database").toNel)(_.nonEmpty) apply
    MergeContext.apply
  }
}
