package com.spark.movedata

import org.apache.spark.sql.{DataFrame, SparkSession}
import  scala.collection.mutable.Map

/**
  * @author Ashwini G N ashwininagaraj964@gmail.com
  * @version 1.0
  *          Reads data from remote hive database
  */

object RemoteHive {

  /**
    * @author Ashwini G N
    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
    * @param spark
    * @param logger
    * @return return data read from the source in a dataframe, source total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def ReadFromRemoteHive(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger): (DataFrame,Long,Long,Int,Int) ={
    val HIVE_HOST_IP=paramsMap("ip").asInstanceOf[String]
    val HIVE_PORT=paramsMap("port").asInstanceOf[String]
    val HIVE_DB=paramsMap("schemaName").asInstanceOf[String]
    System.setProperty("hive.metastore.uris", "thrift://"+HIVE_HOST_IP+":"+HIVE_PORT);
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)
    logger.log("Reading from "+HIVE_HOST_IP+ "hive on port number:"+HIVE_PORT,"INFO")
    sqlContext.sql("use "+HIVE_DB)
    logger.log("schema name :"+HIVE_DB,"INFO")
    logger.log("Table Name: "++paramsMap("tableName").asInstanceOf[String],"INFO")
      var df = spark.emptyDataFrame
      if (paramsMap.exists(_._1 == "columns")) {
        df = sqlContext.sql("select " ++ paramsMap("columns").asInstanceOf[String] + " from " + paramsMap("tableName").asInstanceOf[String])
      } else if(paramsMap.exists(_._1 == "query")) {
        df=spark.sqlContext.sql(paramsMap("query").asInstanceOf[String])
      }else {
        df = sqlContext.sql("select * from " + paramsMap("tableName").asInstanceOf[String])
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
    * @return writes the data to hive and returns target total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def WriteToRemoteHive(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger, df: DataFrame): (Long,Long,Int,Int) ={
    val HIVE_HOST_IP=paramsMap("ip").asInstanceOf[String]
    val HIVE_PORT=paramsMap("port").asInstanceOf[String]
    val HIVE_DB=paramsMap("schemaName").asInstanceOf[String]
    System.setProperty("hive.metastore.uris", "thrift://"+HIVE_HOST_IP+":"+HIVE_PORT);
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(spark.sparkContext)
    sqlContext.sql("use "+HIVE_DB)
    logger.log("writing to Remote hive table :"+HIVE_DB+"."+paramsMap("tableName").asInstanceOf[String],"INFO")
    logger.log("IP :"+HIVE_HOST_IP +", PORT :"+HIVE_PORT,"INFO" )
      val Count=df.count()
      val DistinctCount=df.distinct().count()
      val ColumnCount=df.columns.size
      val NullCount=FetchAndMoveData.nullCount(df)
    df.write.mode(paramsMap("saveMode").asInstanceOf[String])
        .insertInto(paramsMap("tableName").asInstanceOf[String])
      logger.log("Successfully inserted data into Hive table " +paramsMap("tableName").asInstanceOf[String], "INFO")
      return (Count,DistinctCount,ColumnCount,NullCount)
  }
}

