package com.FooBank.SparkDataPipeline

// Java
import java.io.{FileWriter, File}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

// Scala
import scala.io.Source

// Google Guava
import com.google.common.io.Files

// Specs2
//import org.specs2.mutable.Specification
import org.scalatest.FlatSpec

import org.apache.spark.sql._

@RunWith(classOf[JUnitRunner])
class CustomerLoansTest extends FlatSpec {
  //System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark=CustomerLoans.createSparkSession(Map("master" -> "local"))
  
   it should "create JSON dataframe" in {
    val jsonDF= CustomerLoans.readFile("json","http://bynk-data-engineering-task.storage.googleapis.com/data/customers.json")(spark) match {
      case Right(x) => x
      case Left(e) => e.printStackTrace()
    }
    assert(jsonDF.asInstanceOf[DataFrame].count() === 10000)
  }

  it should "create CSV dataframe" in {
     val csvDF=CustomerLoans.getDataframeFromMultipleUrlCsv(List("http://bynk-data-engineering-task.storage.googleapis.com/data/loan-2017-10.csv","http://bynk-data-engineering-task.storage.googleapis.com/data/loan-2017-11.csv"), "," , "sl_no,id,user_id,timestamp,loan_amount,loan_purpose,outcome,interest,webvisit_id")(spark)
    assert(csvDF.count() === 1198)
    
  }
  
  it should "create single CSV dataframe" in {
    val csvDF= CustomerLoans.readFile("csv","http://bynk-data-engineering-task.storage.googleapis.com/data/visits.csv")(spark) match {
      case Right(x) => x
      case Left(e) => e.printStackTrace()
    }
    
    assert(csvDF.asInstanceOf[DataFrame].count() === 11184)
  }
  
  it should "create observations dataframe successfully" in {
    val observationsDF=CustomerLoans.getObservationsDF("http://bynk-data-engineering-task.storage.googleapis.com/data/customers.json",
        List("http://bynk-data-engineering-task.storage.googleapis.com/data/loan-2017-10.csv","http://bynk-data-engineering-task.storage.googleapis.com/data/loan-2017-11.csv"), 
        "http://bynk-data-engineering-task.storage.googleapis.com/data/visits.csv")(spark)
     
     assert(observationsDF.count() === 1752)   
  
  }
     
  
}
