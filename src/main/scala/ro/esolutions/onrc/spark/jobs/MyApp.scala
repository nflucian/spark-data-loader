package ro.esolutions.onrc.spark.jobs

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tupol.spark.implicits._
import org.tupol.spark.io.{FormatAwareDataSinkConfiguration, FormatAwareDataSourceConfiguration}
import org.tupol.utils.config.{Configurator, _}
import ro.esolutions.onrc.spark.SparkApp
import ro.esolutions.onrc.spark.implicits._
import ro.esolutions.onrc.spark.jobs.config.CustomFieldsTransformConfiguration

object MyApp extends SparkApp[MyAppContext, DataFrame] {

  override def createContext(config: Config): MyAppContext = MyAppContext(config).get

  override def run(implicit spark: SparkSession, context: MyAppContext): DataFrame = {
    val inputData = spark.source(context.input).read.action(context.transform)

    inputData.show(false)

//    val outputData = transform(inputData)
//    outputData.sink(context.output).write
    inputData
  }
  def transform(data: DataFrame)(implicit spark: SparkSession, context: MyAppContext) = {

    data  // Transformation logic here
  }

}

case class MyAppContext(input: FormatAwareDataSourceConfiguration,
                        output: FormatAwareDataSinkConfiguration,
                        transform: CustomFieldsTransformConfiguration)

object MyAppContext extends Configurator[MyAppContext] {
  import scalaz.ValidationNel
  import scalaz.syntax.applicative._
  def validationNel(config: Config): ValidationNel[Throwable, MyAppContext] = {
    config.extract[FormatAwareDataSourceConfiguration]("input") |@|
      config.extract[FormatAwareDataSinkConfiguration]("output") |@|
      config.extract[CustomFieldsTransformConfiguration]("transform")apply
      MyAppContext.apply
  }
}