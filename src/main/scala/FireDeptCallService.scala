import java.sql.Date
import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql._

/**
  * Created by narayan on 6/8/16.
  */
object FireDeptCallService extends App {

  val spark = SparkSession.builder().appName("fireDeptCallServiceApp").master("local[4]").getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df: DataFrame = spark.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/home/narayan/Projects/gargantua/Fire_Department_Calls_for_Service.csv")
  df.createOrReplaceTempView("fireDeptCalls")

  //Q1​. How many different types of calls were made to the Fire Department ?
  println(s"::::::::::::::: number of different types of calls is ::::::  ${df.select("Call Type").distinct().count()}")
  //Q2​. How many incidents of each call type were there ?
  df.select("Call Type").groupBy("Call Type").agg(col("Call Type"), count("Call Type")).foreach(println(_))


  //Q3​. How many years of Fire Service Calls in the data file ?

  val yearDf: Dataset[Int] = df.select("Call Date").distinct().map { x =>
    getSqlDate(x.getAs[String]("Call Date")).toLocalDate.getYear
  }
  yearDf.distinct().show()
  println(s"::::::::::::::: number of years of Fire Service Calls in the data file is ::::::  ${yearDf.distinct().count()}")

  //Q4​. How many service calls were logged in the last 7 days ?
  implicit def dateEncoder = org.apache.spark.sql.Encoders.kryo(classOf[Date])

  implicit def parserEncoder = org.apache.spark.sql.Encoders.kryo(classOf[SimpleDateFormat])

  /*val ds: Dataset[Date] = df.select("Call Date").map { x=>getSqlDate(x.getAs[String]("Call Date"))}*/
  /* ds.createOrReplaceTempView("mydate")
   spark.sql("select * from mydate").printSchema()*/
  //ds.printSchema()
  //ds.orderBy(desc("value")).distinct().collect().foreach(println(_))

  /* val noOfEachCallType: Dataset[(String, DataFrame)] = typeOfCall.map { x =>
     val callType = x.getAs("Call_Type").toString
     (callType, spark.sql(s"SELECT * FROM fireDeptCalls WHERE Call_Type ='${callType}'"))
   }

   noOfEachCallType.show(30)
*/

  //Q5. Which neighbourhood in SF generated the most calls last year ?

  //df.createOrReplaceTempView("fireDeptCalls")
  /* val nHood = "Neighborhood District"
   val sf = spark.sql("SELECT * from fireDeptCalls Where City='San Francisco'")
     .filter(row => row.getAs[String]("Call Date").contains("2015"))
   sf.createTempView("sf1")
   //val resutl =spark.sql(s"select * from (select ${nHood}, count(*) as call_count from sf1 group by ${nHood}) d1 join sf1 d2 on d1.${nHood} = d2.${nHood}")
   val resutl = spark.sql(s"select ${nHood}, count(*) as call_count from sf1 group by ${nHood}")

   /*.count()*/
   resutl.show(1)*/

  /* .reduce((a, b) => if (a.getLong(1) > b.getLong(1)) a else b)
 sf match {
   case Row(a, b) => println(s"The neighborhood with max calls in SF: $a with $b calls")
 }*/

  def getSqlDate(date: String): java.sql.Date = {
    val formatter = new SimpleDateFormat("MM/dd/yyyy")
    val dateParse = formatter.parse(date)
    new Date(dateParse.getTime)
  }
}
