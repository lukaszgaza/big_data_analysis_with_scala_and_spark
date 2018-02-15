package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
  test("should correctly generate schema") {
    val columns = List("stringCol", "numericCol1", "numericCol2", "numericCol3")
    val schema = new StructType(Array(StructField("stringCol", StringType), StructField("numericCol1", DoubleType),
      StructField("numericCol2", DoubleType), StructField("numericCol3", DoubleType)))

    assert(TimeUsage.dfSchema(columns) === schema)
  }

  test("should generate correct schema for no columns") {
    val columns = List.empty
    val schema = new StructType()
    assert(TimeUsage.dfSchema(columns) === schema)
  }

  test("should create Row based on list of fields") {
    val fields = List("one", "two", "three")
    val row = Row("one", "two", "three")

    assert(TimeUsage.row(fields) === row)
  }

  test("should create empty row from empty line") {
    val fields = List.empty
    val row = Row()
    assert(TimeUsage.row(fields) === row)
  }
}
