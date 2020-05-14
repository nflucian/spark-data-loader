package ro.esolutions.onrc.spark

import org.apache.spark.sql.SparkSession

/**
  * Trivial trait for creating basic runnable Spark applications.
  * These runnable still needs a runner or an app to run.
  *
  * @tparam Context the type of the application context class.
  * @tparam Result The output type of the run function.
  *
  */
trait SparkRunnable[Context, Result] {

  /**
    * This function needs to be implemented and should contain the entire runnable logic.
    *
    * @param context context instance that should contain all the application specific configuration
    * @param spark active spark session
    * @return
    */
  def run(implicit spark: SparkSession, context: Context): Result

}
