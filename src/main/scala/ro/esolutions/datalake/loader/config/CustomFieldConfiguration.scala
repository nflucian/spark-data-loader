package ro.esolutions.datalake.loader.config

import com.typesafe.config.Config
import org.tupol.utils.config.{Configurator, _}
import scalaz.ValidationNel
import scalaz.syntax.applicative._

case class CustomFieldConfiguration(name: String, expr: String)

object CustomFieldConfiguration extends Configurator[CustomFieldConfiguration] {
  override def validationNel(config: Config): ValidationNel[Throwable, CustomFieldConfiguration] = {
    val name = config.extract[String]("name")
    val expr = config.extract[String]("expr")

    name |@| expr apply CustomFieldConfiguration.apply
  }
}

case class CustomFieldsTransformConfiguration(fields: Seq[CustomFieldConfiguration])

object CustomFieldsTransformConfiguration extends Configurator[CustomFieldsTransformConfiguration] {
  override def validationNel(config: Config): ValidationNel[Throwable, CustomFieldsTransformConfiguration] = {
    config.extract[Seq[CustomFieldConfiguration]]("fields").map(CustomFieldsTransformConfiguration(_))
  }
}

