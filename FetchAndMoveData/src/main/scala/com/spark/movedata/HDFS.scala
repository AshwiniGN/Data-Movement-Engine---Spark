package com.spark.movedata

import org.apache.spark.sql.{DataFrame, SparkSession}
import  scala.collection.mutable.Map

/**
  * @author Ashwini G N ashwininagaraj964@gmail.com
  * @version 1.0
  *          Read data from HDFS in a stand alone or a cluster mode
  */

object HDFS {

  /**
    * @author Ashwini G N
    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
    * @param spark
    * @param logger
    * @return return data read from the source in a dataframe, source total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def ReadFromHDFS(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger): (DataFrame,Long,Long,Int,Int) ={
    logger.log("Reading the data from HDFS ","INFO")
    val HDFS_DIR_PATH = paramsMap("path").asInstanceOf[String]
    val FILE_EXTENSION=paramsMap("fileFormat").asInstanceOf[String]
    logger.log("HDFS file location: " +HDFS_DIR_PATH,"INFO")
    val FileName=paramsMap("fileName").asInstanceOf[String]
    var df=spark.emptyDataFrame
    if (FILE_EXTENSION.equalsIgnoreCase("JSON"))
    {
      logger.log("Reading json files","INFO")
      df = spark.read.option("multiLine", true).json(HDFS_DIR_PATH+"/"+FileName+"."+FILE_EXTENSION)
    }
    else{
      logger.log("Reading files delimited by "+paramsMap("dataFileDelimiter").asInstanceOf[String],"INFO")
      val fileInputDateFormat = paramsMap.getOrElse("fileInputDateFormat", null)
      df = spark.read.format("com.databricks.spark.csv")
        .option("delimiter", paramsMap("dataFileDelimiter").asInstanceOf[String])
        .option("mode", paramsMap("dataReadMode").asInstanceOf[String].toUpperCase())
        .option("header", paramsMap("header").asInstanceOf[String])
        .option("escape", "\\")
        .option("dateFormat", fileInputDateFormat.asInstanceOf[String])
        .load(HDFS_DIR_PATH+"/"+FileName+"."+FILE_EXTENSION)
    }
    val Count=df.count()
    val DistinctCount=df.distinct().count()
    val ColumnCount=df.columns.size
    val NullCount=FetchAndMoveData.nullCount(df)
    return (df,Count,DistinctCount,ColumnCount,NullCount)

  }
  /**
    * @author Ashwini G N
    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
    * @param spark
    * @param logger
    * @return writes the data to HDFS and returns target total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def WriteToHDFS(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger, df: DataFrame,fileFormat:String): (Long,Long,Int,Int) ={
    val HDFS_DIR_PATH = paramsMap("path").asInstanceOf[String]
    logger.log("Writting to HDFS location "+HDFS_DIR_PATH,"INFO")
    val Count=df.count()
    val DistinctCount=df.distinct().count()
    val ColumnCount=df.columns.size
    val NullCount=FetchAndMoveData.nullCount(df)
    logger.log("Merging output and storing as a single output file","INFO")
    if (fileFormat.equalsIgnoreCase("JSON")) {
      df.coalesce(1).write.json(HDFS_DIR_PATH+"/")
    }else{
      df.coalesce(1).write.csv(HDFS_DIR_PATH+"/")
    }
    return (Count,DistinctCount,ColumnCount,NullCount)

  }

}
