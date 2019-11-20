package com.spark.movedata
import  scala.collection.mutable.Map
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Ashwini G N ashwininagaraj964@gmail.com
  * @version 1.0
  *          Reads data from AWS redshift
  */

object Redshift {
  /**
    * @author Ashwini G N
    * @param paramsMap hashmap containing the configurations defined in @paramFilePath in key-value format
    * @param spark
    * @param logger
    * @return return data read from the source in a dataframe, source total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def ReadFromRedshfit(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger): (DataFrame,Long,Long,Int,Int) = {
    logger.log("Reading Data from Redshift ", "INFO")
    val IP = paramsMap("ip").asInstanceOf[String]
    val DB = paramsMap("schemaName").asInstanceOf[String]
    val PORT = paramsMap("port").asInstanceOf[String]
    val USERNAME = paramsMap("userName").asInstanceOf[String]
    val PASSWORD = paramsMap("password").asInstanceOf[String]
    val accessKey=paramsMap("accessKey").asInstanceOf[String]
    val secretKey=paramsMap("secretKey").asInstanceOf[String]
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    var df = spark.emptyDataFrame
    logger.log("RedShift host IP :"+IP,"INFO")
    if (paramsMap.exists(_._1 == "tableName")) {
      logger.log("Reading data from Redshift Table "+DB+"."+paramsMap("tableName").asInstanceOf[String] ,"INFO")
      df = spark.sqlContext.read.format("com.databricks.spark.redshift")
        .option("url", "jdbc:redshift://" + IP + ":" + PORT + "/" + DB)
        .option("dbtable", paramsMap("tableName").asInstanceOf[String])
        .option("user", USERNAME)
        .option("password", PASSWORD)
          .option("tempdir",paramsMap("tempdir").asInstanceOf[String])
        .load()
    }
    if (paramsMap.exists(_._1 == "query")) {
      logger.log("Redshift Database "+DB,"INFO")
      logger.log( paramsMap("query").asInstanceOf[String],"INFO")
      df = spark.sqlContext.read.format("com.databricks.spark.redshift")
        .option("url", "jdbc:redshift://" + IP + ":" + PORT + "/" + DB)
        .option("query", paramsMap("query").asInstanceOf[String])
        .option("user", USERNAME)
        .option("password", PASSWORD)
        .option("tempdir",paramsMap("tempdir").asInstanceOf[String])
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
    * @return writes the data to Redshift and returns target total count, distinct records count , columns count
    *         and total null records count
    *         please refer the readme document on how to use the paramters.
    */

  def WriteToRedshift(paramsMap: Map[String, Any], spark: SparkSession, logger: Logger, df: DataFrame): (Long,Long,Int,Int) ={
    logger.log("writing to Redshift","INFO")
    val IP = paramsMap("ip").asInstanceOf[String]
    val DB = paramsMap("schemaName").asInstanceOf[String]
    val PORT = paramsMap("port").asInstanceOf[String]
    val USERNAME = paramsMap("userName").asInstanceOf[String]
    val PASSWORD = paramsMap("password").asInstanceOf[String]
    val accessKey=paramsMap("accessKey").asInstanceOf[String]
    val secretKey=paramsMap("secretKey").asInstanceOf[String]
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    val Count=df.count()
    val DistinctCount=df.distinct().count()
    val ColumnCount=df.columns.size
    val NullCount=FetchAndMoveData.nullCount(df)
    if(paramsMap.exists(_._1=="aws_iam_role")){
      df.write.format("com.databricks.spark.redshift")
        .option("url", "jdbc:redshift://" + IP + ":" + PORT + "/" + DB)
        .option("dbtable", paramsMap("tableName").asInstanceOf[String])
        .option("user", USERNAME)
        .option("password", PASSWORD)
        .option("tempdir",paramsMap("tempdir").asInstanceOf[String])
        .option("aws_iam_role",paramsMap("aws_iam_role").asInstanceOf[String])
        .mode(paramsMap("saveMode").asInstanceOf[String])
        .save()
    }else{
    df.write.format("com.databricks.spark.redshift")
      .option("url", "jdbc:redshift://" + IP + ":" + PORT + "/" + DB)
      .option("dbtable", paramsMap("tableName").asInstanceOf[String])
      .option("tempdir",paramsMap("tempdir").asInstanceOf[String])
      .option("user", USERNAME)
      .option("password", PASSWORD)
      .mode(paramsMap("saveMode").asInstanceOf[String])
      .save()
  }
    return (Count,DistinctCount,ColumnCount,NullCount)
  }

}
