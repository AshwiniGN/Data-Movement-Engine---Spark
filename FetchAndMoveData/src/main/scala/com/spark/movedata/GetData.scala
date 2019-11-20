package com.spark.movedata

import org.apache.spark.sql.{DataFrame, SparkSession}
import  scala.collection.mutable.Map

/**
  * @author Ashwini G N  ashwininagaraj964@gmail.com,
  * GetData.scala
  * @version 1.0
  * @since 1.0
  *        GetData class helps in fetching data from different sources with the necessary parameters required mentioned in the
  *        param file
  *        connection strings / details is mentioned in the respection source system
  *        we can add more source by creating a separate functions for each
  */

class GetData(logger: Logger) {

  val transformationHandler=new TransformationHandler(logger)

  def ReadData(SourceType:String,paramsMap: Map[String, Any], spark: SparkSession): (DataFrame,Long,Long,Int,Int) = {

    try {
      if (SourceType.equalsIgnoreCase("S3")) {
        logger.log("Source Type is S3", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "sourceAccessKey")) {
          paramMap.put("accessKey", paramsMap("sourceAccessKey"))
        }
        if (paramsMap.exists(_._1 == "sourceSecretKey")) {
          paramMap.put("secretKey", paramsMap("sourceSecretKey"))
        }
        if (paramsMap.exists(_._1 == "sourceBucketPath")) {
          paramMap.put("bucketPath", paramsMap("sourceBucketPath"))
        }
        if (paramsMap.exists(_._1 == "sourceFileName")) {
          paramMap.put("fileName", paramsMap("sourceFileName"))
        }
        if (paramsMap.exists(_._1 == "sourceFileFormat")) {
          paramMap.put("fileFormat", paramsMap("sourceFileFormat"))
        }
        if (paramsMap.exists(_._1 == "sourceHeader")) {
          paramMap.put("header", paramsMap("sourceHeader"))
        }
        if (paramsMap.exists(_._1 == "sourceMultiLineFlag")) {
          paramMap.put("multiLineFlag", paramsMap("sourceMultiLineFlag"))
        }

        if (paramsMap.exists(_._1 == "sourceDelimiter")) {
          paramMap.put("delimiter", paramsMap("sourceDelimiter"))
        }
        val (df, count, distinctCount, columnCount, nullCount) = S3Bucket.ReadS3Bucket(paramMap, spark, logger)
        return (df, count, distinctCount, columnCount, nullCount)
      } else if (SourceType.equalsIgnoreCase("REMOTEHIVE")) {
        logger.log("Source Type is RemoteHive", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "sourceIP")) {
          paramMap.put("ip", paramsMap("sourceIP"))
        }
        if (paramsMap.exists(_._1 == "sourcePort")) {
          paramMap.put("port", paramsMap("sourcePort"))
        }
        if (paramsMap.exists(_._1 == "sourceSchemaName")) {
          paramMap.put("schemaName", paramsMap("sourceSchemaName"))
        }
        if (paramsMap.exists(_._1 == "sourceTableName")) {
          paramMap.put("tableName", paramsMap("sourceTableName"))
        }
        if (paramsMap.exists(_._1 == "sourceColumns")) {
          paramMap.put("columns", paramsMap("sourceColumns"))
        }
        if (paramsMap.exists(_._1 == "sourceQuery")) {
          paramMap.put("query", paramsMap("sourceQuery"))
        }
        val (df, count, distinctCount, columnCount, nullCount) = RemoteHive.ReadFromRemoteHive(paramMap, spark, logger)
        return (df, count, distinctCount, columnCount, nullCount)

      } else if (SourceType.equalsIgnoreCase("REMOTEHDFS")) {
        logger.log("Source Type is RemoteHDFS", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "sourceIP")) {
          paramMap.put("ip", paramsMap("sourceIP"))
        }
        if (paramsMap.exists(_._1 == "sourcePort")) {
          paramMap.put("port", paramsMap("sourcePort"))
        }
        if (paramsMap.exists(_._1 == "sourcePath")) {
          paramMap.put("path", paramsMap("sourcePath"))
        }
        if (paramsMap.exists(_._1 == "sourceFileFormat")) {
          paramMap.put("fileFormat", paramsMap("sourceFileFormat"))
        }
        if (paramsMap.exists(_._1 == "sourceFileName")) {
          paramMap.put("fileName", paramsMap("sourceFileName"))
        }
        if (paramsMap.exists(_._1 == "sourceHeader")) {
          paramMap.put("header", paramsMap("sourceHeader"))
        }
        if (paramsMap.exists(_._1 == "sourceDelimiter")) {
          paramMap.put("delimiter", paramsMap("sourceDelimiter"))
        }
        if (paramsMap.exists(_._1 == "sourceFileInputDateFormat")) {
          paramMap.put("fileInputDateFormat", paramsMap("sourceFileInputDateFormat"))
        }

        val (df, count, distinctCount, columnCount, nullCount) = RemoteHDFS.ReadFromRemoteHDFS(paramMap, spark, logger)
        return (df, count, distinctCount, columnCount, nullCount)
      } else if (SourceType.equalsIgnoreCase("REDSHIFT")) {
        logger.log("Source Type is Redshift", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "sourceIP")) {
          paramMap.put("ip", paramsMap("sourceIP"))
        }
        if (paramsMap.exists(_._1 == "sourcePort")) {
          paramMap.put("port", paramsMap("sourcePort"))
        }
        if (paramsMap.exists(_._1 == "sourceSchemaName")) {
          paramMap.put("schemaName", paramsMap("sourceSchemaName"))
        }
        if (paramsMap.exists(_._1 == "sourceTableName")) {
          paramMap.put("tableName", paramsMap("sourceTableName"))
        }
        if (paramsMap.exists(_._1 == "sourceUsername")) {
          paramMap.put("userName", paramsMap("sourceUsername"))
        }
        if (paramsMap.exists(_._1 == "sourcePassword")) {
          paramMap.put("password", paramsMap("sourcePassword"))
        }
        if (paramsMap.exists(_._1 == "sourceQuery")) {
          paramMap.put("query", paramsMap("sourceQuery"))
        }
        if (paramsMap.exists(_._1 == "sourceTempDir")) {
          paramMap.put("tempdir", paramsMap("sourceTempDir"))
        }
        if (paramsMap.exists(_._1 == "sourceAccessKey")) {
          paramMap.put("accessKey", paramsMap("sourceAccessKey"))
        }
        if (paramsMap.exists(_._1 == "sourceSecretKey")) {
          paramMap.put("secretKey", paramsMap("sourceSecretKey"))
        }
        val (df, count, distinctCount, columnCount, nullCount) = Redshift.ReadFromRedshfit(paramMap, spark, logger)
        return (df, count, distinctCount, columnCount, nullCount)
      } else if (SourceType.equalsIgnoreCase("MYSQL")) {
        logger.log("Source Type is MYSQL", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "sourceIP")) {
          paramMap.put("ip", paramsMap("sourceIP"))
        }
        if (paramsMap.exists(_._1 == "sourcePort")) {
          paramMap.put("port", paramsMap("sourcePort"))
        }
        if (paramsMap.exists(_._1 == "sourceSchemaName")) {
          paramMap.put("schemaName", paramsMap("sourceSchemaName"))
        }
        if (paramsMap.exists(_._1 == "sourceTableName")) {
          paramMap.put("tableName", paramsMap("sourceTableName"))
        }
        if (paramsMap.exists(_._1 == "sourceUsername")) {
          paramMap.put("userName", paramsMap("sourceUsername"))
        }
        if (paramsMap.exists(_._1 == "sourcePassword")) {
          paramMap.put("password", paramsMap("sourcePassword"))
        }
        if (paramsMap.exists(_._1 == "sourceQuery")) {
          paramMap.put("query", paramsMap("sourceQuery"))
        }
        val (df, count, distinctCount, columnCount, nullCount) = MYSQL.ReadFromMysql(paramMap, spark, logger)
        return (df, count, distinctCount, columnCount, nullCount)
      } else if (SourceType.equalsIgnoreCase("JDBC")) {
        logger.log("Source Type is JDBC URL", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "sourceJDBCUrl")) {
          paramMap.put("JDBCUrl", paramsMap("sourceJDBCUrl"))
        }
        if (paramsMap.exists(_._1 == "sourceTableName")) {
          paramMap.put("tableName", paramsMap("sourceTableName"))
        }
        if (paramsMap.exists(_._1 == "sourceUsername")) {
          paramMap.put("userName", paramsMap("sourceUsername"))
        }
        if (paramsMap.exists(_._1 == "sourcePassword")) {
          paramMap.put("password", paramsMap("sourcePassword"))
        }
        if (paramsMap.exists(_._1 == "sourceQuery")) {
          paramMap.put("query", paramsMap("sourceQuery"))
        }
        if (paramsMap.exists(_._1 == "sourceDriverName")) {
          paramMap.put("driver", paramsMap("sourceDriverName"))
        }
        val (df, count, distinctCount, columnCount, nullCount) = ConnectorJDBC.ReadFromJDBC(paramMap, spark, logger)
        return (df, count, distinctCount, columnCount, nullCount)
      }
//      else if (SourceType.equalsIgnoreCase("MONGODB") ){
//        logger.log("Source Type is MONGODB", "INFO")
//        val paramMap = scala.collection.mutable.Map[String, Any]()
//        if (paramsMap.exists(_._1 == "sourceIP")) {
//          paramMap.put("ip", paramsMap("sourceIP"))
//        }
//        if (paramsMap.exists(_._1 == "sourceSchemaName")) {
//          paramMap.put("schemaName", paramsMap("sourceSchemaName"))
//        }
//        if (paramsMap.exists(_._1 == "sourceCollectionName")) {
//          paramMap.put("collectionName", paramsMap("sourceCollectionName"))
//        }
//        val (df, count, distinctCount, columnCount, nullCount) = MongoDB.ReadFromMongoDB(paramMap, spark, logger)
//        return (df, count, distinctCount, columnCount, nullCount)
//      }
      else if(SourceType.equalsIgnoreCase("HIVE") ){
        logger.log("Source Type is Hive", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "sourceSchemaName")) {
          paramMap.put("schemaName", paramsMap("sourceSchemaName"))
        }
        if (paramsMap.exists(_._1 == "sourceTableName")) {
          paramMap.put("tableName", paramsMap("sourceTableName"))
        }
        if (paramsMap.exists(_._1 == "sourceColumns")) {
          paramMap.put("columns", paramsMap("sourceColumns"))
        }
        if (paramsMap.exists(_._1 == "sourceQuery")) {
          paramMap.put("query", paramsMap("sourceQuery"))
        }
        val (df, count, distinctCount, columnCount, nullCount) = Hive.ReadFromHive(paramMap, spark, logger)
        return (df, count, distinctCount, columnCount, nullCount)
      } else if (SourceType.equalsIgnoreCase("HDFS")) {
        logger.log("Source Type is HDFS", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "sourcePath")) {
          paramMap.put("path", paramsMap("sourcePath"))
        }
        if (paramsMap.exists(_._1 == "sourceFileFormat")) {
          paramMap.put("fileFormat", paramsMap("sourceFileFormat"))
        }
        if (paramsMap.exists(_._1 == "sourceFileName")) {
          paramMap.put("fileName", paramsMap("sourceFileName"))
        }
        if (paramsMap.exists(_._1 == "sourceHeader")) {
          paramMap.put("header", paramsMap("sourceHeader"))
        }
        if (paramsMap.exists(_._1 == "sourceDelimiter")) {
          paramMap.put("delimiter", paramsMap("sourceDelimiter"))
        }
        val (df, count, distinctCount, columnCount, nullCount) = HDFS.ReadFromHDFS(paramMap, spark, logger)
        return (df, count, distinctCount, columnCount, nullCount)
      }else {
        logger.log(SourceType + "couldnt be found. Existing..", "INFO")
        sys.exit(1)
      }
    }
    catch {
      case t :Throwable =>
        transformationHandler.raiseTransformationException("FecthAndMove",t)
    }
  }
}
