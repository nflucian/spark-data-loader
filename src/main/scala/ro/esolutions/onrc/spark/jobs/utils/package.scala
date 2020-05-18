package ro.esolutions.onrc.spark.jobs

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar

package object utils {

  val ISO_LOCAL_DATE = "yyyy-MM-dd"

  def isValidLocalDate(chunk: String): Boolean = {
    LocalDate.parse(chunk, DateTimeFormatter.ofPattern(ISO_LOCAL_DATE))
    true
  }

  def extractTodayDate(): String = {
    val dateFormat = new SimpleDateFormat(ISO_LOCAL_DATE)
    dateFormat.format(Calendar.getInstance().getTime)
  }
}