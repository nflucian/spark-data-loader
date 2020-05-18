package ro.esolutions.onrc.spark.jobs

import com.typesafe.config.Config
import org.tupol.utils.config.Extractor
import ro.esolutions.onrc.spark.jobs.services.ResourceService.Database

package object config {

  implicit val databaseExtractor = new Extractor[Database] {
    override def extract(config: Config, path: String): Database = Database(config.getString(path))
  }

  implicit val CustomFieldsTransformConfigurationExtractor = CustomFieldsTransformConfiguration
  implicit val CustomFieldConfigurationExtractor = CustomFieldConfiguration

}
