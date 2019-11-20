package com.spark.movedata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.Map

/**
  * @author Ashwini G N ashwininagaraj964@gmail.com
  * @version 1.0
  *          Read data from remote AWS S3 bucket
  */

object S3Bucket {

  /**
    * @author Ashwini G N
    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
    * @param spark
    * @param logger
    * @return return data read from the source in a dataframe, source total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def ReadS3Bucket(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger): (DataFrame,Long,Long,Int,Int) = {
    val accessKey=paramsMap("accessKey").asInstanceOf[String]
    val secretKey=paramsMap("secretKey").asInstanceOf[String]
    val bucketPath=paramsMap("bucketPath").asInstanceOf[String]
    val fileFormat=paramsMap("fileFormat").asInstanceOf[String]
    val FileName=paramsMap("fileName").asInstanceOf[String]
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    logger.log("Reading data from S3 Bucket :"+bucketPath,"INFO")
    var df = spark.emptyDataFrame
    if (fileFormat.equalsIgnoreCase("JSON")) {
        logger.log("File Format is JSON ","INFO")
        df = spark.read.option("multiLine", paramsMap("multiLineFlag").asInstanceOf[String]).json("s3a://" + bucketPath + "/"+FileName+"." + fileFormat)
    }
      else {
      df = spark.sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", paramsMap("header").asInstanceOf[String])
          .option("delimiter", paramsMap("delimiter").asInstanceOf[String])
          .option("inferSchema", "true")
          .load("s3a://" + bucketPath + "/"+FileName+"." + fileFormat)
        logger.log("Files are separated by "+ paramsMap("delimiter").asInstanceOf[String],"INFO")
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
    * @return writes the data to S3 bucket and returns target total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def WriteToS3Bucket(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger,df: DataFrame,fileFormat:String): (Long,Long,Int,Int) = {
    val accessKey=paramsMap("accessKey").asInstanceOf[String]
    val secretKey=paramsMap("secretKey").asInstanceOf[String]
    val TargetPath=paramsMap("bucketPath").asInstanceOf[String]
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    logger.log("Writing to S3 Bucket :"+"s3a://"+TargetPath,"INFO")
    val Count=df.count()
    val DistinctCount=df.distinct().count()
    val ColumnCount=df.columns.size
    val NullCount=FetchAndMoveData.nullCount(df)
      df.coalesce(1).write
        .format("com.databricks.spark.csv")
        .option("header", "true")
      .mode(paramsMap("saveMode").asInstanceOf[String])
        .save("s3a://" + TargetPath)
      logger.log("Successfully added data to S3 Bucket.", "INFO")
    return (Count,DistinctCount,ColumnCount,NullCount)

  }

}
