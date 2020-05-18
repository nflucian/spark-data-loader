package ro.esolutions.onrc.spark

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}
import ro.esolutions.onrc.spark.jobs.config.CustomFieldsTransformConfiguration

package object implicits {

  /** DataFrame decorator. */
  implicit class RichDataFrame(val dataFrame: DataFrame) {

    def action[TC <: CustomFieldsTransformConfiguration](configuration: TC)(implicit spark: SparkSession): DataFrame = {
      configuration.fields.foldLeft(dataFrame)((df, field) => df.withColumn(field.name, expr(field.expr)))
    }
  }
}