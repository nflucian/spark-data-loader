package ro.esolutions.onrc.spark.jobs.services

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Inside, Matchers}
import ro.esolutions.onrc.spark.jobs.services.ResourceService.{Database, Resource}

class ResourceSpec extends FlatSpec with Matchers with Inside with BeforeAndAfterAll {

  val db = Database("databases")
  val table = "table"

  it should "provide info about current schema" in {
    Resource(db, table).absoluteCurrentSchema shouldEqual(s"${db.name}_current.${table}")
  }

  it should "provide info about temp schema" in {
    Resource(db, table).absoluteTempSchema shouldEqual(s"${db.name}_temp.${table}")
  }

  it should "provide absolute schema" in {
    Resource(db, table).absoluteSchema shouldEqual(s"${db.name}.${table}")
  }

  it should "have table" in {
    an [IllegalArgumentException] should be thrownBy {
      Resource(db, "")
    }
  }

}
