//package com.spark.movedata
//
//import scala.collection.mutable.Map
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import com.mongodb.spark.MongoSpark
//import com.mongodb.spark.config._
//
///**
//  * @author Ashwini G N ashwininagaraj964@gmail.com
//  * @version 1.0
//  *          Reads data from MONGODB
//  */
//
//object MongoDB {
//
//  /**
//    * @author Ashwini G N
//    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
//    * @param spark
//    * @param logger
//    * @return return data read from source in a dataframe, source total count, distinct records count , columns count
//    *         and total null records count
//    *         please refer the readme document on how to use the paramters.
//    */
//
//  def ReadFromMongoDB(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger): (DataFrame,Long,Long,Int,Int) ={
//    val MONGODB_IP=paramsMap("ip").asInstanceOf[String]
//    val MONGODB_DB=paramsMap("schemaName").asInstanceOf[String]
//    val COLLECTION=paramsMap("collectionName").asInstanceOf[String]
//    logger.log("Reading MongoDB collection : "+COLLECTION,"INFO")
//    val df = MongoSpark.load(spark.sparkContext,(ReadConfig(Map("uri" -> ("mongodb://"+MONGODB_IP+"/"+MONGODB_DB+"."+COLLECTION))))).toDF()
//    val Count=df.count()
//    val ColumnCount=df.columns.size
//    val DistinctCount=df.distinct().count()
//    val NullCount=FetchAndMoveData.nullCount(df)
//    return (df,Count,DistinctCount,ColumnCount,NullCount)
//
//  }
//
//  /**
//    * @author Ashwini G N
//    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
//    * @param spark
//    * @param logger
//    * @return writes the data to mongodb and returns target total count, distinct records count , columns count
//    *         and total null records count
//    *         please refer the readme document on how to use the paramters.
//    */
//
//  def WriteToMongoDB(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger,df: DataFrame): (Long,Long,Int,Int) ={
//    val MONGODB_IP=paramsMap("ip").asInstanceOf[String]
//    val MONGODB_DB=paramsMap("schemaName").asInstanceOf[String]
//    val COLLECTION=paramsMap("collectionName").asInstanceOf[String]
//    logger.log("writing to MongoDB collection : "+COLLECTION,"INFO")
//    val Count=df.count()
//    val ColumnCount=df.columns.size
//    val DistinctCount=df.distinct().count()
//    val NullCount=FetchAndMoveData.nullCount(df)
//    df.rdd.saveToMongoDB(WriteConfig(Map("uri" -> ("mongodb://"+MONGODB_IP+"/"+MONGODB_DB+"."+COLLECTION))))
//    return (Count,DistinctCount,ColumnCount,NullCount)
//  }
//
//}
