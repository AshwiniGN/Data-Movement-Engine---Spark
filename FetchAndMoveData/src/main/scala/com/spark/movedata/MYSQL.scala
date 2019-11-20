package com.spark.movedata

import org.apache.spark.sql.{DataFrame, SparkSession}
import  scala.collection.mutable.Map

/**
  * @author Ashwini G N ashwininagaraj964@gmail.com
  * @version 1.0
  *          Read data from Mysql database
  */

object MYSQL {

  /**
    * @author Ashwini G N
    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
    * @param spark
    * @param logger
    * @return return data read from source in a dataframe, source total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def ReadFromMysql(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger): (DataFrame,Long,Long,Int,Int) ={
    val MYSQL_HOST_IP=paramsMap("ip").asInstanceOf[String]
    val MYSQL_PORT=paramsMap("port").asInstanceOf[String]
    val MYSQL_DB=paramsMap("schemaName").asInstanceOf[String]
    val MYSQL_USERNAME=paramsMap("userName").asInstanceOf[String]
    val MYSQL_PASSWORD=paramsMap("password").asInstanceOf[String]
    var df=spark.emptyDataFrame
    if(paramsMap.exists(_._1 == "tableName")){
      logger.log("MYSQL host IP :"+MYSQL_HOST_IP,"INFO")
      logger.log("Reading data from MYSQL Table "+MYSQL_DB+"."+paramsMap("tableName").asInstanceOf[String] ,"INFO")
      df=spark.sqlContext.read.format("jdbc")
        .option("url", "jdbc:mysql://"+MYSQL_HOST_IP+":"+MYSQL_PORT+"/"+MYSQL_DB)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", paramsMap("tableName").asInstanceOf[String])
        .option("user", MYSQL_USERNAME)
        .option("password", MYSQL_PASSWORD)
        .load()
    }
    if(paramsMap.exists(_._1 == "query")){
      logger.log("MYSQL host IP :"+MYSQL_HOST_IP,"INFO")
      logger.log("Reading data from MYSQL . ","INFO")
      logger.log( paramsMap("query").asInstanceOf[String],"INFO")
      df=spark.sqlContext.read.format("jdbc")
        .option("url", "jdbc:mysql://"+MYSQL_HOST_IP+":"+MYSQL_PORT+"/"+MYSQL_DB)
        .option("driver", "com.mysql.jdbc.Driver")
        .option("query", paramsMap("query").asInstanceOf[String])
        .option("user", MYSQL_USERNAME)
        .option("password", MYSQL_PASSWORD)
        .load()
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
    * @return writes the data to MYSQL database and returns target total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def WriteToMysql(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger,df: DataFrame): (Long,Long,Int,Int) ={
    val MYSQL_HOST_IP=paramsMap("ip").asInstanceOf[String]
    val MYSQL_PORT=paramsMap("port").asInstanceOf[String]
    val MYSQL_DB=paramsMap("schemaName").asInstanceOf[String]
    val MYSQL_USERNAME=paramsMap("userName").asInstanceOf[String]
    val MYSQL_PASSWORD=paramsMap("password").asInstanceOf[String]
    logger.log("MYSQL host IP :"+MYSQL_HOST_IP,"INFO")
    logger.log("writing data to MYSQL Table "+MYSQL_DB+"."+ paramsMap("tableName").asInstanceOf[String],"INFO")
    val Count=df.count()
    val DistinctCount=df.distinct().count()
    val ColumnCount=df.columns.size
    val NullCount=FetchAndMoveData.nullCount(df)
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://"+MYSQL_HOST_IP+":"+MYSQL_PORT+"/"+MYSQL_DB)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", paramsMap("tableName").asInstanceOf[String])
      .option("user", MYSQL_USERNAME)
      .option("password", MYSQL_PASSWORD)
      .mode(paramsMap("saveMode").asInstanceOf[String]).save()
    return (Count,DistinctCount,ColumnCount,NullCount)

  }

}
