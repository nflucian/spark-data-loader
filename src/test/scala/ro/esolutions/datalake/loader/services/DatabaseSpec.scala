package ro.esolutions.datalake.loader.services

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Inside, Matchers}
import ro.esolutions.datalake.loader.services.ResourceService.Database

class DatabaseSpec extends FlatSpec with Matchers with Inside with BeforeAndAfterAll {

  val db = "databases"

  it should "provide info about current database" in {
    Database(db).current shouldEqual(s"${db}_current")
  }

  it should "provide info about temp database" in {
    Database(db).temp shouldEqual(s"${db}_temp")
  }

  it should "have database" in {
    an [IllegalArgumentException] should be thrownBy {
      Database("")
    }
  }
}
