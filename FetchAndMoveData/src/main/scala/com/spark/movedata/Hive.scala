package com.spark.movedata

import org.apache.spark.sql.{DataFrame, SparkSession}
import  scala.collection.mutable.Map

/**
  * @author Ashwini G N ashwininagaraj964@gmail.com
  * @version 1.0
  *          Read data from hive in a stand alone node or in a cluster
  */

object Hive {

  /**
    * @author Ashwini G N
    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
    * @param spark
    * @param logger
    * @return return data read from the source in a dataframe, source total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def ReadFromHive(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger): (DataFrame,Long,Long,Int,Int) ={
    val HIVE_DB=paramsMap("schemaName").asInstanceOf[String]
    logger.log("Reading from hive ","INFO")
    spark.sqlContext.sql("use "+HIVE_DB)
    logger.log("schema name :"+HIVE_DB,"INFO")
    logger.log("Table Name: "++paramsMap("tableName").asInstanceOf[String],"INFO")
    var df = spark.emptyDataFrame
    if (paramsMap.exists(_._1 == "columns")) {
      df = spark.sqlContext.sql("select " ++ paramsMap("columns").asInstanceOf[String] + " from " + paramsMap("tableName").asInstanceOf[String])
    } else if(paramsMap.exists(_._1 == "query")) {
      df=spark.sqlContext.sql(paramsMap("query").asInstanceOf[String])
    }else{
      df = spark.sqlContext.sql("select * from " + paramsMap("tableName").asInstanceOf[String])
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

  def WriteToHive(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger, df: DataFrame): (Long,Long,Int,Int) ={
    val HIVE_DB=paramsMap("schemaName").asInstanceOf[String]
    spark.sqlContext.sql("use "+HIVE_DB)
    logger.log("writing to Remote hive table :"+HIVE_DB+"."+paramsMap("tableName").asInstanceOf[String],"INFO")
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

