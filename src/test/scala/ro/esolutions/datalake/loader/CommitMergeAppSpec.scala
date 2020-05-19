package ro.esolutions.datalake.loader

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}
import ro.esolutions.datalake.loader.contexts.CommitMergeContext
import ro.esolutions.datalake.loader.services.ResourceService.{Database, Resource}

class CommitMergeAppSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  val dbs = Seq("db1", "db2").map(Database)
  val dummyContext = new CommitMergeContext(dbs)

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

  it should "move the temp data to current" in {
    val table = "tmp"
    val resourceDB1 = Resource(dbs.head, table)
    val resourceDB2 = Resource(dbs.last, table)

    createTable(resourceDB1.absoluteTempSchema, "/merge/merge_current_and_delta.csv")(spark)
    createTable(resourceDB2.absoluteTempSchema, "/merge/merge_current_and_delta.csv")(spark)

    val expected: DataFrame = readCSV("/merge/merge_current_and_delta.csv")(spark)
      .drop("TECHNICAL_DATE")

    CommitMergeApp.run(spark, dummyContext)

    val resultdb1 = spark.table(resourceDB1.absoluteCurrentSchema)
    val resultdb2 = spark.table(resourceDB2.absoluteCurrentSchema)

    assertDataFrameEquals(resultdb1, expected)
    assertDataFrameEquals(resultdb2, expected)
  }
}
