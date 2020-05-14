package ro.esolutions.onrc.spark.jobs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}
import ro.esolutions.onrc.spark.jobs.contexts.MergeContext
import ro.esolutions.onrc.spark.jobs.services.ResourceService.{Database, Resource}

class MergeAppSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  val dbs = Seq("db1", "db2").map(Database)
  val technicalDate = "2020-03-24"
  val dummyContext = new MergeContext(technicalDate, dbs)

  override def beforeAll(): Unit = {
    super.beforeAll()
    dbs.foreach(db => {
      spark.sql(s"create database if not exists ${db.name}")
      spark.sql(s"create database if not exists ${db.current}")
      spark.sql(s"create database if not exists ${db.temp}")
    })
  }

  override def afterAll(): Unit = {
    super.afterAll()
    dbs.foreach(db => {
      spark.sql(s"drop database ${db.name} cascade")
      spark.sql(s"drop database ${db.current} cascade")
      spark.sql(s"drop database ${db.temp} cascade")
    })
  }

  it should "merge delta with empty current table" in {
    val db = dbs.head
    val table = "tbl1"
    val resource = Resource(db, table)

    createTable(resource.absoluteSchema, "/merge/firma_delta.csv", Some("TECHNICAL_DATE"))(spark)
    createTable(resource.absoluteCurrentSchema, "/merge/firma_current_empty.csv")(spark)

    val expected: DataFrame = readCSV("/merge/firma_delta.csv")(spark)
      .drop("TECHNICAL_DATE")

    MergeApp.run(spark, dummyContext)

    val result = spark.table(resource.absoluteTempSchema)

    assertDataFrameEquals(result, expected)
  }

  it should "merge delta with current table" in {
    val db = dbs.head
    val table = "tbl2"
    val resource = Resource(db, table)

    createTable(resource.absoluteSchema, "/merge/firma_delta.csv", Some("TECHNICAL_DATE"))(spark)
    createTable(resource.absoluteCurrentSchema, "/merge/firma_current_populated.csv")(spark)

    val expected: DataFrame = readCSV("/merge/merge_current_and_delta.csv")(spark)

    MergeApp.run(spark, dummyContext)

    val result = spark.table(resource.absoluteTempSchema)

    assertDataFrameEquals(result, expected)
  }

  it should "populated current table with delta with union columns" in {
    val table = "tbl3"
    val resourceDeltaMore = Resource(dbs.head, table)
    val resourceDeltaLess = Resource(dbs.last, table)

    createTable(resourceDeltaMore.absoluteCurrentSchema, "/merge/firma_current_populated.csv")(spark)
    createTable(resourceDeltaMore.absoluteSchema, "/merge/firma_delta_with_more_columns.csv", Some("TECHNICAL_DATE"))(spark)

    createTable(resourceDeltaLess.absoluteCurrentSchema, "/merge/firma_current_populated.csv")(spark)
    createTable(resourceDeltaLess.absoluteSchema, "/merge/firma_delta_with_less_columns.csv", Some("TECHNICAL_DATE"))(spark)

    val expectedMore: DataFrame = readCSV("/merge/merge_more_columns.csv")(spark)
    val expectedLess: DataFrame = readCSV("/merge/merge_less_columns.csv")(spark)

    MergeApp.run(spark, dummyContext)

    val resultDeltaMore = spark.table(resourceDeltaMore.absoluteTempSchema)
    val resultDeltaLess = spark.table(resourceDeltaLess.absoluteTempSchema)

    assertDataFrameEquals(resultDeltaMore, expectedMore)
    assertDataFrameEquals(resultDeltaLess, expectedLess)
  }

}
