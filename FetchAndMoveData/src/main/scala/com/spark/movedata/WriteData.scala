package com.spark.movedata

import org.apache.spark.sql.{DataFrame, SparkSession}
import  scala.collection.mutable.Map

class WriteData(logger: Logger) {

  val transformationHandler=new TransformationHandler(logger)

  def WriteData(TargetType:String,paramsMap: Map[String, Any], spark: SparkSession,df: DataFrame): (Long,Long,Int,Int) = {

    try {
      val FileFormat: String = if (paramsMap.exists(_._1 == "sourceFileFormat")) {
        paramsMap("sourceFileFormat").asInstanceOf[String]
      } else {
        "NA"
      }
      if (TargetType.equalsIgnoreCase("S3")) {
        logger.log("Target Type is S3", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "targetAccessKey")) {
          paramMap.put("accessKey", paramsMap("targetAccessKey"))
        }
        if (paramsMap.exists(_._1 == "targetSecretKey")) {
          paramMap.put("secretKey", paramsMap("targetSecretKey"))
        }
        if (paramsMap.exists(_._1 == "targetBucketPath")) {
          paramMap.put("bucketPath", paramsMap("targetBucketPath"))
        }
        if (paramsMap.exists(_._1 == "targetSaveMode")) {
          paramMap.put("saveMode", paramsMap("targetSaveMode"))
        }
        val (count, distinctCount, columnCount, nullCount) = S3Bucket.WriteToS3Bucket(paramMap, spark, logger, df, FileFormat)
        return (count, distinctCount, columnCount, nullCount)
      } else if (TargetType.equalsIgnoreCase("REMOTEHIVE")) {
        logger.log("Target Type is RemoteHive", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "targetIP")) {
          paramMap.put("ip", paramsMap("targetIP"))
        }
        if (paramsMap.exists(_._1 == "targetPort")) {
          paramMap.put("port", paramsMap("targetPort"))
        }
        if (paramsMap.exists(_._1 == "targetSchemaName")) {
          paramMap.put("schemaName", paramsMap("targetSchemaName"))
        }
        if (paramsMap.exists(_._1 == "targetTableName")) {
          paramMap.put("tableName", paramsMap("targetTableName"))
        }
        if (paramsMap.exists(_._1 == "targetSaveMode")) {
          paramMap.put("saveMode", paramsMap("targetSaveMode"))
        }
        val (count, distinctCount, columnCount, nullCount) = RemoteHive.WriteToRemoteHive(paramMap, spark, logger, df)
        return (count, distinctCount, columnCount, nullCount)

      } else if (TargetType.equalsIgnoreCase("REMOTEHDFS")) {
        logger.log("Source Type is RemoteHDFS", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "targetIP")) {
          paramMap.put("ip", paramsMap("targetIP"))
        }
        if (paramsMap.exists(_._1 == "targetPort")) {
          paramMap.put("port", paramsMap("targetPort"))
        }
        if (paramsMap.exists(_._1 == "targetPath")) {
          paramMap.put("path", paramsMap("targetPath"))
        }
        val (count, distinctCount, columnCount, nullCount) = RemoteHDFS.WriteToRemoteHDFS(paramMap, spark, logger, df, FileFormat)
        return (count, distinctCount, columnCount, nullCount)
      } else if (TargetType.equalsIgnoreCase("REDSHIFT")) {
        logger.log("Target Type is Redshift", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "targetIP")) {
          paramMap.put("ip", paramsMap("targetIP"))
        }
        if (paramsMap.exists(_._1 == "targetPort")) {
          paramMap.put("port", paramsMap("targetPort"))
        }
        if (paramsMap.exists(_._1 == "targetSchemaName")) {
          paramMap.put("schemaName", paramsMap("targetSchemaName"))
        }
        if (paramsMap.exists(_._1 == "targetTableName")) {
          paramMap.put("tableName", paramsMap("targetTableName"))
        }
        if (paramsMap.exists(_._1 == "targetUsername")) {
          paramMap.put("userName", paramsMap("targetUsername"))
        }
        if (paramsMap.exists(_._1 == "targetPassword")) {
          paramMap.put("password", paramsMap("targetPassword"))
        }
        if (paramsMap.exists(_._1 == "targetAWSIAMRole")) {
          paramMap.put("aws_iam_role", paramsMap("targetAWSIAMRole"))
        }
        if (paramsMap.exists(_._1 == "targetSaveMode")) {
          paramMap.put("saveMode", paramsMap("targetSaveMode"))
        }
        if (paramsMap.exists(_._1 == "targetAccessKey")) {
          paramMap.put("accessKey", paramsMap("targetAccessKey"))
        }
        if (paramsMap.exists(_._1 == "targetSecretKey")) {
          paramMap.put("secretKey", paramsMap("targetSecretKey"))
        }
        if (paramsMap.exists(_._1 == "targetTempDir")) {
          paramMap.put("tempdir", paramsMap("targetTempDir"))
        }
        val (count, distinctCount, columnCount, nullCount) = Redshift.WriteToRedshift(paramMap, spark, logger, df)
        return (count, distinctCount, columnCount, nullCount)
      } else if (TargetType.equalsIgnoreCase("MYSQL")) {
        logger.log("Target Type is MYSQL", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "targetIP")) {
          paramMap.put("ip", paramsMap("targetIP"))
        }
        if (paramsMap.exists(_._1 == "targetPort")) {
          paramMap.put("port", paramsMap("targetPort"))
        }
        if (paramsMap.exists(_._1 == "targetSchemaName")) {
          paramMap.put("schemaName", paramsMap("targetSchemaName"))
        }
        if (paramsMap.exists(_._1 == "targetTableName")) {
          paramMap.put("tableName", paramsMap("targetTableName"))
        }
        if (paramsMap.exists(_._1 == "targetUsername")) {
          paramMap.put("userName", paramsMap("targetUsername"))
        }
        if (paramsMap.exists(_._1 == "targetPassword")) {
          paramMap.put("password", paramsMap("targetPassword"))
        }
        if (paramsMap.exists(_._1 == "targetSaveMode")) {
          paramMap.put("saveMode", paramsMap("targetSaveMode"))
        }
        val (count, distinctCount, columnCount, nullCount) = MYSQL.WriteToMysql(paramMap, spark, logger, df)
        return (count, distinctCount, columnCount, nullCount)
      } else if (TargetType.equalsIgnoreCase("JDBC")) {
        logger.log("Target Type is JDBC URL", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "targetJDBCUrl")) {
          paramMap.put("JDBCUrl", paramsMap("targetJDBCUrl"))
        }
        if (paramsMap.exists(_._1 == "targetTableName")) {
          paramMap.put("tableName", paramsMap("targetTableName"))
        }
        if (paramsMap.exists(_._1 == "targetUsername")) {
          paramMap.put("userName", paramsMap("targetUsername"))
        }
        if (paramsMap.exists(_._1 == "targetPassword")) {
          paramMap.put("password", paramsMap("targetPassword"))
        }
        if (paramsMap.exists(_._1 == "targetDriverName")) {
          paramMap.put("driver", paramsMap("targetDriverName"))
        }
        if (paramsMap.exists(_._1 == "targetSaveMode")) {
          paramMap.put("saveMode", paramsMap("targetSaveMode"))
        }
        val (count, distinctCount, columnCount, nullCount) = ConnectorJDBC.WriteUsingJDBC(paramMap, spark, logger, df)
        return (count, distinctCount, columnCount, nullCount)
      }
//      else if (TargetType.equalsIgnoreCase("MONGODB") ){
//        logger.log("Target Type is MONGODB", "INFO")
//        val paramMap = scala.collection.mutable.Map[String, Any]()
//        if (paramsMap.exists(_._1 == "targetIP")) {
//          paramMap.put("ip", paramsMap("targetIP"))
//        }
//        if (paramsMap.exists(_._1 == "targetSchemaName")) {
//          paramMap.put("schemaName", paramsMap("targetSchemaName"))
//        }
//        if (paramsMap.exists(_._1 == "targetCollectionName")) {
//          paramMap.put("collectionName", paramsMap("targetCollectionName"))
//        }
//        val (count, distinctCount, columnCount, nullCount) = MongoDB.WriteToMongoDB(paramMap, spark, logger,df)
//        return (count, distinctCount, columnCount, nullCount)
//      }
      else if (TargetType.equalsIgnoreCase("HIVE")) {
        logger.log("Target Type is Hive", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "targetSchemaName")) {
          paramMap.put("schemaName", paramsMap("targetSchemaName"))
        }
        if (paramsMap.exists(_._1 == "targetTableName")) {
          paramMap.put("tableName", paramsMap("targetTableName"))
        }
        if (paramsMap.exists(_._1 == "targetSaveMode")) {
          paramMap.put("saveMode", paramsMap("targetSaveMode"))
        }
        val (count, distinctCount, columnCount, nullCount) = Hive.WriteToHive(paramMap, spark, logger, df)
        return (count, distinctCount, columnCount, nullCount)

      }else if (TargetType.equalsIgnoreCase("HDFS")) {
        logger.log("Source Type is HDFS", "INFO")
        val paramMap = scala.collection.mutable.Map[String, Any]()
        if (paramsMap.exists(_._1 == "targetPath")) {
          paramMap.put("path", paramsMap("targetPath"))
        }
        val (count, distinctCount, columnCount, nullCount) = HDFS.WriteToHDFS(paramMap, spark, logger, df, FileFormat)
        return (count, distinctCount, columnCount, nullCount)
      }else {
        logger.log(TargetType + "couldnt be found. Existing..", "INFO")
        sys.exit(1)
      }
    }
  catch {
    case t: Throwable =>
      transformationHandler.raiseTransformationException("FecthAndMove", t)
  }
  }
}
