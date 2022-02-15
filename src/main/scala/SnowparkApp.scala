import com.snowflake.snowpark.functions.{as_double, col, lit}
import com.snowflake.snowpark.{DataFrame, RelationalGroupedDataFrame, Session, functions}

import java.io.{File, InputStream}
import java.nio.file.Paths
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable

object SnowparkApp {


  def main(args: Array[String]): Unit = {
    runProcess("SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE")
  }

  def runProcess(tableName : String): Unit ={
    val stream: InputStream = getClass.getResourceAsStream("snowflake.conf")
    val properties: Properties = new Properties()
    properties.load(stream)

    val session = Session.builder.configs(properties.asScala.asJava).create
    addDeps(session)

    val df: DataFrame = session.table(tableName)
    val dfs = mutable.ArrayBuffer[DataFrame]()
    val fieldCount = df.schema.fields.size
    var iterCount = 0
    println(s"Total records in table : "
      + df.count().toString)
    df.schema.fields.foreach(i => {
      iterCount = iterCount + 1
      println(s"-- Processing column ${iterCount}/${fieldCount}: ${i.name}")
      println(s"Datatype : "+i.dataType.toString())
      val colAnalysis = createAnalysisDataframe(session,tableName, i.name, i.dataType.toString())
      dfs += colAnalysis
    })
    val analysisOutput = dfs.reduce(_ union _)
    analysisOutput.show(fieldCount)
    analysisOutput.write.mode("overwrite").saveAsTable(s"SNOWPARK_DATA_PROFILING_TPCDS_STORE")
  }


  // Rules definition
  def colValueDuplicates(df: DataFrame, columnName: String): String = {
    val dataframe1 = df.groupBy(columnName).count()
    val dataframe2 = dataframe1.filter(col("count") === 1)
    val duplicates = (dataframe1.count() - dataframe2.count()).toString
    println(s"No of duplicate records in column : ${duplicates}")
    duplicates
  }

  def colValueUniqueness(df: DataFrame, columnName: String): Int = {
    val dataframe1 = df.groupBy(col(columnName)).count()
    val dataframe2 = dataframe1.filter(col("count") === 1).count()

    println(s"No of unique values in column : "
      + dataframe2.toString)
    dataframe2.toInt
  }

  def colNullCheck(df: DataFrame, columnName: String): String = {
    val nulls = df.where(df.col(columnName).is_null).count()
    println(s"No of null values in column : "
      + nulls.toString)
    nulls.toString
  }

  def colValueCompleteness(df: DataFrame, columnName: String): String = {
    val nulls : Double = df.where(df.col(columnName).is_null).count()
    val count : Double = df.count().toDouble
    val completeness = 1-(nulls / count)
    println(s"Completeness of column : "
      + completeness.toString)
    completeness.toString
  }

  def colStatistics(df: DataFrame, columnName: String, dataType : String): String = {
    var stats = ""
    if (dataType != "String" && dataType != "Date") {
      val max = df.select(functions.max(df.col(columnName))).collect()(0)(0)
      val min = df.select(functions.min(df.col(columnName))).collect()(0)(0)
      val stdev = df.select(functions.stddev(df.col(columnName))).collect()(0)(0)
      val mean = df.select(functions.mean(df.col(columnName))).collect()(0)(0)
      stats = min + "/" + max + "/" + stdev + "/" + mean
      println(s"Statistics of column : " + stats)
      stats
    } else{
      stats
    }
  }

  def colDatatype(df: DataFrame, columnName: String): String = {
    val nulls : Double = df.where(df.col(columnName).is_null).count()
    //    val completeness = if (nulls>0) nulls/df.count() else 1.0
    val count : Double = df.count().toDouble
    val completeness = 1-(nulls / count)
    println(s"Completeness of column : " + completeness.toString)
    completeness.toString
  }

  def createAnalysisDataframe(session: Session, tableName : String, columnName : String, dataType : String): DataFrame = {
    val df: DataFrame = session.table(tableName)
    val dfSeq = session.createDataFrame(Seq(columnName)).toDF("columnName")
    val dfOut = dfSeq.withColumn("completeness", lit(colValueCompleteness(df, columnName)))
      .withColumn("uniqueValues", lit(colValueUniqueness(df, columnName)))
      .withColumn("duplicates",lit(colValueDuplicates(df, columnName)))
      .withColumn("nulls",lit(colNullCheck(df, columnName)))
      .withColumn("datatype",lit(dataType))
      .withColumn("MIN_MAX_MEAN_STDEV",lit(colStatistics(df, columnName, dataType)))
    dfOut
  }

  def createPermanentUdf(session: Session): Unit = {
    session.sql("drop FUNCTION if EXISTS udfPerm(Int)").show()
    session.udf.registerPermanent("udfPerm", permanentUdfHandler _, "STG")
    session.sql("select *, udfPerm(*) from values (10)").show()
  }

  def permanentUdfHandler(i: Int): Int = {
    i * i
  }

  def createInlinePermanentUdf(session: Session): Unit = {
    session.sql("drop FUNCTION if EXISTS udfInlinePerm(Int)").show()

    session.udf.registerPermanent("udfInlinePerm", (i: Int) => {
      i * 2
    }, "STG")

    session.sql("select *, udfInlinePerm(*) from values (10)").show()
  }

  private def addDeps(session: Session): Unit = {
    val PATH = Paths.get(".", "target", "dependency").toAbsolutePath.toString
    val lst = getListOfFiles(PATH)
    val filteredLst = lst.filterNot(_.matches("^.*snowpark(_original)?-[0-9.]+\\.jar$"))
    for (f <- filteredLst) {
      System.out.println("Adding dep:" + f)
      session.addDependency(f)
    }
  }

  def getListOfFiles(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.getPath)
    } else {
      List[String]()
    }
  }
}