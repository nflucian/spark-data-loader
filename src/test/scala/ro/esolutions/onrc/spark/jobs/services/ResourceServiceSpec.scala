package ro.esolutions.onrc.spark.jobs.services

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.SaveMode
import org.scalatest.{FlatSpec, Matchers}
import ro.esolutions.onrc.spark.jobs.services.ResourceService.Database

class ResourceServiceSpec extends FlatSpec with DataFrameSuiteBase with Matchers {

  val dbs = Seq("rc", "test").map(Database)

  override def beforeAll(): Unit = {
    super.beforeAll()
    dbs.foreach(db => {
      spark.sql(s"create database if not exists ${db.name}")
    })
  }

  override def afterAll(): Unit = {
    super.afterAll()
    dbs.foreach(db => {
      spark.sql(s"drop database ${db.name} cascade")
    })
  }

  it should "get only the db and tables with partition in specific date" in {
    import spark.implicits._

    val date = "2020-05-05"
    val db = dbs.head
    spark.sql(s"use ${db.name}")

    val partition = "TECHNICAL_DATE"
    val tables = Seq(
      ("tbl1", Seq((1, date)).toDF("id", partition)),
      ("tbl2", Seq((1,"2020.05.04")).toDF("id", partition))
    )
    tables.foreach{ case (tbl, df) => {
      df.write.mode(SaveMode.Overwrite).partitionBy(partition).format("parquet").saveAsTable(tbl)
    }}

    val result = ResourceService()(spark).current(dbs, date)
    val expected = ResourceService.Resource(db,"tbl1")

    result should contain (expected)
    result should have size 1
  }

}
