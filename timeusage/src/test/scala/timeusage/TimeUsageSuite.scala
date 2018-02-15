package timeusage

import java.util

import org.apache.spark
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  private val spark: SparkSession = SparkSession.builder().master("local[*]")getOrCreate()

  override def afterAll(): Unit = spark.stop()

  import spark.implicits._

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

  test("should correctly classify provided columns") {
    val columns = List("t080", "t050", "t18030", "t010", "t160")
    val classification = (List(new Column("t18030"), new Column("t010")), List(new Column("t050")),
      List(new Column("t080"), new Column("t160")))

    assert(TimeUsage.classifiedColumns(columns) === classification)
  }

  test("should correctly summarize user data 1") {
    val primaryColumns = List(new Column("t01"), new Column("t03"))
    val workColumns = List(new Column("t05"))
    val otherColumns = List(new Column("t02"))
    val workingStatus = 0
    val sexValue = 0
    val age = 20
    val schema = new StructType(Array(StructField("telfs", IntegerType), StructField("tesex", IntegerType),
      StructField("teage", IntegerType), StructField("t01", IntegerType), StructField("t03", IntegerType),
      StructField("t05", IntegerType), StructField("t02", IntegerType)))
    val row = Row(workingStatus, sexValue, age, 42, 18, 12, 0)
    val df = spark.createDataFrame(util.Arrays.asList(row), schema)
    val summarizedDF = TimeUsage.timeUsageSummary(primaryColumns, workColumns, otherColumns, df)

    val expectedSchema = new StructType(Array(StructField("working", StringType), StructField("sex", StringType),
      StructField("age", StringType), StructField("primaryNeeds", DoubleType), StructField("work", DoubleType),
      StructField("other", DoubleType)))
    val expectedRow = Row("not working", "female", "young", 1.0, 0.2, 0.0)
    val expectedDF = spark.createDataFrame(util.Arrays.asList(expectedRow), expectedSchema)
    //  .withColumn("working", new Column("working"))

    assert(summarizedDF.count() === expectedDF.count())
    assert(summarizedDF.head() === expectedDF.head())
  }

  test("should correctly summarize user data 2") {
    val primaryColumns = List(new Column("t01"), new Column("t03"), new Column("t11"))
    val workColumns = List(new Column("t05"))
    val otherColumns = List(new Column("t02"))
    val workingStatus = 1
    val sexValue = 1
    val age = 30
    val schema = new StructType(Array(StructField("telfs", IntegerType), StructField("tesex", IntegerType),
      StructField("teage", IntegerType), StructField("t01", IntegerType), StructField("t03", IntegerType),
      StructField("t11", IntegerType), StructField("t05", IntegerType), StructField("t02", IntegerType)))
    val row = Row(workingStatus, sexValue, age, 42, 18, 30, 15, 45)
    val df = spark.createDataFrame(util.Arrays.asList(row), schema)
    val summarizedDF = TimeUsage.timeUsageSummary(primaryColumns, workColumns, otherColumns, df)

    val expectedSchema = new StructType(Array(StructField("working", StringType), StructField("sex", StringType),
      StructField("age", StringType), StructField("primaryNeeds", DoubleType), StructField("work", DoubleType),
      StructField("other", DoubleType)))
    val expectedRow = Row("working", "male", "active", 1.5, 0.25, 0.75)
    val expectedDF = spark.createDataFrame(util.Arrays.asList(expectedRow), expectedSchema)
    //  .withColumn("working", new Column("working"))

    assert(summarizedDF.count() === expectedDF.count())
    assert(summarizedDF.head() === expectedDF.head())
  }

  test("should correctly summarize user data 3") {
    val primaryColumns = List(new Column("t01"))
    val workColumns = List(new Column("t05"))
    val otherColumns = List(new Column("t02"), new Column("t04"))
    val workingStatus = 4
    val sexValue = 2
    val age = 60
    val schema = new StructType(Array(StructField("telfs", IntegerType), StructField("tesex", IntegerType),
      StructField("teage", IntegerType), StructField("t01", IntegerType), StructField("t05", IntegerType),
      StructField("t02", IntegerType), StructField("t04", IntegerType)))
    val row = Row(workingStatus, sexValue, age, 30, 0, 45, 45)
    val df = spark.createDataFrame(util.Arrays.asList(row), schema)
    val summarizedDF = TimeUsage.timeUsageSummary(primaryColumns, workColumns, otherColumns, df)

    val expectedSchema = new StructType(Array(StructField("working", StringType), StructField("sex", StringType),
      StructField("age", StringType), StructField("primaryNeeds", DoubleType), StructField("work", DoubleType),
      StructField("other", DoubleType)))
    val expectedRow = Row("not working", "female", "elder", 0.5, 0.0, 1.5)

    val expectedDF = spark.createDataFrame(util.Arrays.asList(expectedRow), expectedSchema)
    //  .withColumn("working", new Column("working"))

    assert(summarizedDF.count() === expectedDF.count())
    assert(summarizedDF.head() === expectedDF.head())
  }

  test("should correctly summarize user data 4") {
    val primaryColumns = List(new Column("t01"), new Column("t03"))
    val workColumns = List(new Column("t05"))
    val otherColumns = List(new Column("t02"))
    val workingStatus = 5
    val sexValue = 1
    val age = 60
    val schema = new StructType(Array(StructField("telfs", IntegerType), StructField("tesex", IntegerType),
      StructField("teage", IntegerType), StructField("t01", IntegerType), StructField("t03", IntegerType),
      StructField("t05", IntegerType),StructField("t02", IntegerType)))
    val row = Row(workingStatus, sexValue, age, 42, 18, 30, 0)
    val df = spark.createDataFrame(util.Arrays.asList(row), schema)
    val summarizedDF = TimeUsage.timeUsageSummary(primaryColumns, workColumns, otherColumns, df)

    assert(summarizedDF.count() === 0)
  }
}