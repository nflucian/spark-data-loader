package ro.esolutions.onrc.spark.jobs

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

import com.typesafe.config.Config
import org.apache.spark.internal.Logging
import org.tupol.utils.config.Extractor
import ro.esolutions.onrc.spark.jobs.services.ResourceService.Database

package object utils extends Logging {

  val ISO_LOCAL_DATE = "yyyy-MM-dd"

  def isValidLocalDate(chunk: String): Boolean = {
    LocalDate.parse(chunk, DateTimeFormatter.ofPattern(ISO_LOCAL_DATE))
    true
  }

  def extractTodayDate(): String = {
    val dateFormat = new SimpleDateFormat(ISO_LOCAL_DATE)
    dateFormat.format(Calendar.getInstance().getTime)
  }

  implicit val databaseExtractor = new Extractor[Database] {
    def extract(config: Config, path: String): Database = Database(config.getString(path))
  }

}
