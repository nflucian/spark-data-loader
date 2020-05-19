package ro.esolutions.datalake

import org.tupol.utils._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.util.Try

package object spark {

  /**
    * `TypesafeConfigBuilder` must be mixed in with a class or a trait that has an `appName` function
    * and it provides the `applicationConfiguration` function that builds a Typesafe `Config` instance
    * out of various resources including application parameters.
    */
  trait TypesafeConfigBuilder extends Logging {
    this: { def appName: String } =>

    /**
      * Extract and assemble a configuration object out of the application parameters and
      * configuration files, as described bellow.
      * <p>
      * The parameters assembly priority order is as following (the top ones are overwriting the bottom ones, if defined):
      * <ol>
      * <li> application parameters passed in as `main()` arguments</li>
      * <li> `application.conf` file, if passed in as a `spark-submit --files ...` argument</li>
      * <li> `application.conf` file, if available in the classpath</li>
      * <li> `reference.conf` file, if available in the classpath</li>
      * </ol>
      * @param spark the spark session
      * @param args application parameters
      * @return the application configuration object
      */
    private[spark] def applicationConfiguration(implicit spark: SparkSession, args: Array[String]): Config = {

      import java.io.File

      val CONFIGURATION_FILENAME = "application.conf"

      log.info(s"$appName: Application Parameters:\n${args.mkString("\n")}")

      // This configuration file is supposed to work with the --files option of spark-submit,
      // but it seems that in yarn-cluster mode this one fails.
      // In yarn cluster mode the SparkFiles.getRootDirectory yields a result like
      //   /opt/app/hadoop/yarn/local/usercache/spark/appcache/application_1472457872363_0064/spark-fb5e850b-2108-482d-8dff-f9a3d2db8dd6/userFiles-d02bb426-515d-4e82-a2ac-d160061c8cb6/
      // However, the local path (new File(".").getAbsolutePath() ) for the Driver looks like
      //   /opt/app/hadoop/yarn/local/usercache/spark/appcache/application_1472457872363_0064/container_e21_1472457872363_0064_01_000001
      // Though looks like a small difference, this is a problem in cluster mode.
      // To overcome situations encountered so far we are using the `configurationFile` and the `localConfigurationFile`
      // to try both paths.
      // In standalone mode the reverse is true.
      // We might be able to come to the bottom of this, but it looks like a rabbit hole not worth exploring at the moment.
      val sparkConfiguration: Option[Config] = {
        val file = new File(SparkFiles.get(CONFIGURATION_FILENAME))
        val available = file.exists && file.canRead && file.isFile
        log.info(s"$appName: SparkFiles configuration file: ${file.getAbsolutePath} " +
          s"is ${if (!available) "not " else ""}available.")
        if (available) {
          Try(ConfigFactory.parseFile(file))
            .logSuccess(_ => log.info(s"Successfully parsed the local file at '${file.getAbsolutePath}'"))
            .logFailure(t => log.error(s"Failed to parse local file at '${file.getAbsolutePath}'", t))
            .toOption
        } else None
      }

      val localConfiguration: Option[Config] = {
        val file = new File(CONFIGURATION_FILENAME)
        val available = file.exists && file.canRead && file.isFile
        log.info(s"$appName: Local configuration file: ${file.getAbsolutePath} is ${if (!available) "not " else ""}available.")
        if (available) {
          Try(ConfigFactory.parseFile(file))
            .logSuccess(_ => log.info(s"Successfully parsed the local file at '${file.getAbsolutePath}'"))
            .logFailure(t => log.error(s"Failed to parse local file at '${file.getAbsolutePath}'", t))
            .toOption
        } else None
      }

      val classpathConfiguration: Option[Config] = {
        Try(ConfigFactory.parseResources(CONFIGURATION_FILENAME))
          .logSuccess(_ => log.info(s"Successfully parsed the classpath configuration at '$CONFIGURATION_FILENAME'"))
          .logFailure(t => log.error(s"Failed to parse classpath configuration at '$CONFIGURATION_FILENAME'", t))
          .toOption
      }

      val configurationFiles = Seq(sparkConfiguration, localConfiguration, classpathConfiguration)

      val parametersConf = ConfigFactory.parseString(args.mkString("\n"))
      val fullConfig =
        configurationFiles.collect { case Some(config) => config }.
          foldLeft(parametersConf)((acc, conf) => acc.withFallback(conf)).
          withFallback(ConfigFactory.defaultReference())
      val resolvedConfig = Try(fullConfig.resolve())
        .logFailure(_ => log.warn("Failed to resolve the variables locally."))
        .orElse(Try(fullConfig.resolveWith(parametersConf)))
        .logFailure(_ => log.warn("Failed to resolve the variables from the application arguments."))
        .getOrElse(fullConfig)
      val config = Try(resolvedConfig.getConfig(appName)).getOrElse(ConfigFactory.empty())

      log.debug(s"$appName: Configuration:\n${renderConfig(config)}")

      config
    }

    protected def renderConfig(config: Config): String = config.root.render
  }

}
