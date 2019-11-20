package com.spark.movedata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import java.util.Calendar
import scala.collection.mutable.Map

/**
  * TransformationHandler.scala
  *        Utility class containing multiple utility functions to be reused by the scala classes for different jobs
  */

class TransformationHandler(logger: Logger) {
  /**
    * @param paramsFilePath : Full path of the config/param file for this job
    * @param sqlContext     : Reference to the Spark SQLContext object
    * @return hashmap containing the configurations defined in @paramFilePath in key-value format
    * @see
    * @throws java.lang.Exception
    */
  @throws(classOf[Exception])
  def getConfigParamsMap(paramsFilePath: String, SqlContext: SQLContext): Map[String, Any] = {
    try {
      val paramsFile = SqlContext.read.json(paramsFilePath)
      val paramsMap = Map[String, Any]()
      val configNames = paramsFile.columns
      configNames.foreach { key => val value = paramsFile.select(key).head.get(0); paramsMap.put(key, value) }
      logger.log("Below parameters were passed in the config/param file :", "INFO")
      paramsMap.foreach(entry => logger.log(entry._1 + " = " + entry._2, "INFO"))

      paramsMap //return
    }
    catch {
      case t: Throwable =>
        throw new Exception("Error while parsing json config file:\n" + t.getMessage)
    }

  }
  /**
    * @param sourceName : Name of the source during transformation of whose data the exception was raised
    * @param t          : Instance of the Throwable object generated as part of exception handling
    * @return returns an instance of java.lang.Exception object
    * @see
    * @throws java.lang.Exception
    * @since 1.0
    */
  @throws(classOf[Exception])
  def raiseTransformationException(sourceName: String, t: Throwable): Nothing = {
    t.printStackTrace() // TODO: handle error
    val errorMessage = "Error in raw file transformation job for source " + sourceName + "\n" + t.getMessage
    logger.log(errorMessage, "ERROR")
    throw new Exception(errorMessage)

  }

  /**
    * @param spark           : Reference to the SparkContext object for the current spark application/job
    * @param status          : Status of the spark job,SUCCESS or FAIL
    * @param sourceName      : source system name
    * @param batchId         : entry time for the job
    * @param startTimeMillis : starttime in millis
    * @return None
    * @see
    * @throws None
    * @since 1.0
    */
  def terminateTransformationJob(spark: SparkSession, status: String, sourceName: String, startTimeMillis:
  Long): Unit = {
    logger.log("Inside FetchAndMove.terminateJob() method", "DEBUG")

    val totalTime = (Calendar.getInstance.getTimeInMillis - startTimeMillis) / 1000
    var logMessage = "Status of fetch and move" + sourceName + "=" + status
    logMessage = logMessage + "\n" + "Total time taken " + sourceName +"=" + totalTime + " seconds"

    logger.log(logMessage, "INFO")

    spark.stop()
    if (status == "FAIL") {
      logger.log("Exiting with exit code 1", "INFO")
      System.exit(1)
    }
    else {
      logger.log("Exiting with exit code 0", "INFO")
      System.exit(0)
    }
  }


}



