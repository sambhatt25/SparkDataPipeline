package com.FooBank.SparkDataPipeline

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql._
import com.typesafe.config._

object CustomerLoans {
  
  lazy val logger = LoggerFactory.getLogger(getClass())
  
  def createSparkSession(properties: Map[String, Any]) = {
    /*
     This function is used to create a spark session based on properties specified as a parameter
     User can provide custom properties
    */
    logger.info("Establishing Spark Session !!")
    val session = SparkSession.builder
      .appName(properties.getOrElse("appName", "defaultJob").asInstanceOf[String])
      .master(properties.getOrElse("master", "local").asInstanceOf[String])

    if (properties.contains("deploy-mode")) {
      session.config("deploy-mode", properties("deploy-mode").asInstanceOf[String])
    }
    if (properties.getOrElse("enablehive", false).asInstanceOf[Boolean]) {
      session.enableHiveSupport()
    }

    val sparkSession = session.getOrCreate()
    sparkSession
  }

  def readFile(fileFormat:String, filePath:String)(implicit con:SparkSession):Either[Throwable, DataFrame] = {
    /*
      This function is used to read the file(CSV,JSON) from given file path and return either DataFrame or Exception
     */
      Try(fileFormat match {
        case "json" | "JSON" => createJsonDF(filePath)
        case "csv" | "CSV" => createCsvDF(filePath)
        case _ => throw new UnsupportedOperationException("Other file formats are not implemented yet !!!")
      }) match {
        case Success(df) => Right(df)
        case Failure(e) => Left(e)
      }
  }
  
  def createJsonDF(url: String)(implicit sparkCon: SparkSession)={
    sparkCon.read.json(sparkCon.sparkContext.parallelize(scala.io.Source.fromURL(url).mkString.split("\n")))   
  }
  
  def createCsvDF (url: String, delim: String=",")(implicit sparkCon: SparkSession)={
    
      val content = scala.io.Source.fromURL(url).mkString
      val list = content.split("\n").filter(_ != "")
      val rdd=sparkCon.sparkContext.parallelize(list)
      val header=rdd.first().toString
      val schema = StructType(header.split(delim).map(x => x.trim.toString).map(fieldName => StructField(fieldName.trim.toString, StringType, true)))
      val headLessCsvRDD=rdd.mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter 
      }
      
      val headLessCsvRowsRDD=headLessCsvRDD.map(x => x +" ").map(_.split(delim)).map(e => Row.fromSeq(e))
      val csvDF=sparkCon.createDataFrame(headLessCsvRowsRDD,schema)
      csvDF
  }
  
  def getDataframeFromMultipleUrlCsv(urlList: List[String],delim:String=",",headerString:String) (implicit sparkCon: SparkSession) = {
    val schema = StructType(headerString.split(delim).map(x => x.trim.toString).map(fieldName => StructField(fieldName.trim.toString, StringType, true)))
    var DF = sparkCon.createDataFrame(sparkCon.sparkContext.emptyRDD[Row], schema)
    for(url <- urlList){
      DF=DF.union(createCsvDF(url,delim))
    }    
    DF    
  }
  
  def getObservationsDF(customerFileLoc: String, loanFilesLoc:List[String], visitsFileLoc: String)(implicit sparkCon: SparkSession) = {
   /*
     This function used file URLs and create Dataframes
     */
    import sparkCon.implicits._
    val customerDF = readFile("json",customerFileLoc)(sparkCon) match {
      case Right(x) => x
      case Left(e) => e.printStackTrace()
    }
    
    val visitsDF = readFile("csv",visitsFileLoc)(sparkCon) match {
      case Right(x) => x
      case Left(e) => e.printStackTrace()
    }
    
    val loanDF=getDataframeFromMultipleUrlCsv(loanFilesLoc,",","sl_no,id,user_id,timestamp,loan_amount,loan_purpose,outcome,interest,webvisit_id")
    
    
    def stringCleanse(inputString: String): String = "[0-9]+".r.findFirstIn(inputString).getOrElse("0")
    
    val stringCleanseUDF = udf(stringCleanse _)
    
    val joinedDF=loanDF.withColumnRenamed("id", "loan_id")
                       .withColumnRenamed("timestamp", "loan_timestamp")
                       .join(broadcast(customerDF.asInstanceOf[DataFrame]), $"user_id" === $"id","left_outer")
                       .withColumnRenamed("id", "customer_id")
                       .withColumn("web_visit_id_clean", stringCleanseUDF($"webvisit_id"))
                       .join(visitsDF.asInstanceOf[DataFrame],$"web_visit_id_clean" === $"id","left_outer")
                       .withColumnRenamed("id", "visit_id")
                       .withColumnRenamed("timestamp","visit_timestamp")
                      
    joinedDF
  }
  
  def dataFrameCsvWriter(df: DataFrame, opPath:String){
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(opPath)
  }
  
  
 def main(args: Array[String]) {
 
   //Using config factory to set the configurations
   val conf = ConfigFactory.load()
   val master=conf.getString("CustomerLoans.master")
   val customersFileUrl=conf.getString("CustomerLoans.customersFileUrl")
   val visitsFileUrl=conf.getString("CustomerLoans.visitsFileUrl")
   val loanFilesUrlList=conf.getString("CustomerLoans.loanFilesUrlList").split(",").toList
   
   val opFilePath=conf.getString("CustomerLoans.opPath")
   
   val spark=createSparkSession(Map("master" -> master))
   
   val observationsDF=getObservationsDF(customersFileUrl,loanFilesUrlList,visitsFileUrl)(spark)
   
   observationsDF.show()
   dataFrameCsvWriter(observationsDF,opFilePath)
   
  }
  
}