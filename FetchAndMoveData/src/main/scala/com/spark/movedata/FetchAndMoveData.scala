package com.spark.movedata

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Calendar
import org.apache.spark.sql.functions.col
import scala.collection.mutable.Map

/**
  * @author ashwini G N  ashwininagaraj942@gmail.com,
  * FetchAndMoveData.scala
  * @version 1.0
  * @date 2019/17/11
  * @see
  */

object FetchAndMoveData {

  var logger: Logger = _

  def main(args: Array[String]): Unit = {

    val paramFilePath = args(0)
    val logFileName = args(1).toString.toUpperCase()

    val jobName = "Job::" + logFileName
    // Set logger :
    logger = new Logger(logFileName)

    logger.log("--------------------------------------------------------", "INFO")
    logger.log("Starting New Job at " + Calendar.getInstance.getTime, "INFO")
    logger.log("--------------------------------------------------------", "INFO")
    logger.log("Below parameters were passed from calling program :", "INFO")
    logger.log("paramFilePath=" + paramFilePath, "INFO")
    logger.log("logFileName=" + logFileName, "INFO")
    var status=""
    val startTimeMillis=Calendar.getInstance.getTimeInMillis


      /** **********************************Set Spark Application Configuration ****************************************/
      val spark: SparkSession = SparkSession.builder //.master("local")
        .appName(logFileName)
        .enableHiveSupport()
//        .config("hive.exec.dynamic.partition", "true")
//        .config("hive.exec.dynamic.partition.mode", "nonstrict")
//        .config("hive.exec.max.dynamic.partitions", "4096")
        .getOrCreate()
      // Set Configuration of Spark Application and running option

      val transformationHandler = new TransformationHandler(logger)
      val getData=new GetData(logger)
      val writeData=new WriteData(logger)

    try {

      val sqlContext=spark.sqlContext
      val paramsMap: scala.collection.mutable.Map[String,Any] = transformationHandler.getConfigParamsMap(paramFilePath, sqlContext)
      paramsMap.put("jobName", jobName)
      paramsMap.put("paramFilePath", paramFilePath)

      val (sourceDf,sourceCount,sourceDistinctCount,sourceColumnCount,sourceNullCount)=getData.ReadData(paramsMap("sourceType").asInstanceOf[String],paramsMap,spark)
      val (targetCount,targetDistinctCount,targetColumnCount,targetNullCount)=writeData.WriteData(paramsMap("targetType").asInstanceOf[String],paramsMap,spark,sourceDf)
      logger.log("source records count : "+sourceCount,"INFO")
      logger.log("target records count : "+targetCount,"INFO")
      if(sourceCount==targetCount){
        logger.log("source and target total records count is matching ","INFO")
      }
      else
      {
        logger.log("source and target total records count is not matching ","INFO")
      }
      logger.log("source data distinct count : "+sourceDistinctCount,"INFO")
      logger.log("target data distinct count : "+targetDistinctCount,"INFO")
      logger.log("source data columns count : "+sourceColumnCount,"INFO")
      logger.log("target data columns count : "+targetColumnCount,"INFO")
      logger.log("source data null count : "+sourceNullCount,"INFO")
      logger.log("target data null count : "+targetNullCount,"INFO")
      status="SUCCESS"
      logger.log("--------------------------------------------------------", "INFO")
      logger.log("Job Finished at " + Calendar.getInstance.getTime, "INFO")
      logger.log("--------------------------------------------------------", "INFO")
    }
    catch {
      case t: Throwable =>
        t.printStackTrace()
        logger.log("Error in fetch and move data job " + jobName + "\n" + t.getMessage, "ERROR")
        status="FAIL"

    }
    finally {
      transformationHandler.terminateTransformationJob(spark,status,"FetchAndMove",startTimeMillis)
    }
  }

  /**
    * @author Ashwini G N
    * @param dataFrame data read from the source
    * @return returns the count of null , empty values in the source df
    */

  def nullCount(dataFrame: DataFrame):Int={
    try{
      val EmptyCount= dataFrame.columns.map(x=>dataFrame.filter(col(x)==="").count).sum.toInt
      val NullCount= dataFrame.columns.map(x=>dataFrame.filter(col(x)==="null").count).sum.toInt
      return EmptyCount+NullCount
    }catch{
      case ex: Exception => throw new Exception(
        "Error while counting the null values "+ex.getMessage )
    }
  }


}
