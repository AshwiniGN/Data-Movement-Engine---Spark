package com.spark.movedata

import org.apache.spark.sql.{DataFrame, SparkSession}
import  scala.collection.mutable.Map

/**
  * @author Ashwini G N ashwininagaraj964@gmail.com
  * @version 1.0
  *          Reads data using JDBC connection string
  */

object ConnectorJDBC {

  /**
    * @author Ashwini G N
    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
    * @param spark
    * @param logger
    * @return return data read from the source in a dataframe, source total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def ReadFromJDBC(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger):(DataFrame,Long,Long,Int,Int)={
    logger.log("Reading Data using JDBC url ", "INFO")
    val URL = paramsMap("JDBCUrl").asInstanceOf[String]
    val USERNAME = paramsMap("userName").asInstanceOf[String]
    val PASSWORD = paramsMap("password").asInstanceOf[String]
    val DRIVER_NAME = paramsMap("driverName").asInstanceOf[String]
    var df = spark.emptyDataFrame
    logger.log("JDBC URL is :"+URL,"INFO")
    if (paramsMap.exists(_._1 == "tableName")) {
      logger.log("Table name :"+paramsMap("tableName").asInstanceOf[String] ,"INFO")
      df = spark.sqlContext.read.format("jdbc")
        .option("url", URL)
        .option("dbtable", paramsMap("tableName").asInstanceOf[String])
        .option("user", USERNAME)
        .option("driver", DRIVER_NAME)
        .option("password", PASSWORD)
        .load()
    }
    if (paramsMap.exists(_._1 == "query")) {
      logger.log("query : "+paramsMap("query").asInstanceOf[String],"INFO")
      logger.log( paramsMap("query").asInstanceOf[String],"INFO")
      df = spark.sqlContext.read.format("jdbc")
        .option("url", URL)
        .option("driver", DRIVER_NAME)
        .option("query", paramsMap("query").asInstanceOf[String])
        .option("user", USERNAME)
        .option("password", PASSWORD)
        .load()

    }
    val Count = df.count()
    val DistinctCount = df.distinct().count()
    val ColumnCount = df.columns.size
    val NullCount = FetchAndMoveData.nullCount(df)
    return (df, Count, DistinctCount, ColumnCount, NullCount)
  }

  /**
    * @author Ashwini G N
    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
    * @param spark
    * @param logger
    * @return writes the data to target and returns target total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def WriteUsingJDBC(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger,df: DataFrame): (Long,Long,Int,Int) ={
    val URL=paramsMap("JDBCUrl").asInstanceOf[String]
    val DRIVER_NAME=paramsMap("driver").asInstanceOf[String]
    val USERNAME=paramsMap("UserName").asInstanceOf[String]
    val PASSWORD=paramsMap("Password").asInstanceOf[String]
    logger.log("Writing to target using "+URL,"INFO")
    val Count=df.count()
    val DistinctCount=df.distinct().count()
    val ColumnCount=df.columns.size
    val NullCount=FetchAndMoveData.nullCount(df)
    df.write
      .format("jdbc")
      .option("url", URL)
      .option("driver", DRIVER_NAME)
      .option("dbtable", paramsMap("tableName").asInstanceOf[String])
      .option("user", USERNAME)
      .option("password", PASSWORD)
      .mode( paramsMap("saveMode").asInstanceOf[String]).save()
    return (Count,DistinctCount,ColumnCount,NullCount)
  }

}
