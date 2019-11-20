package com.spark.movedata

import org.apache.log4j.{Level, Logger, LogManager, PropertyConfigurator}

class Logger(source: String) extends Serializable {
  @transient lazy val logger = org.apache.log4j.LogManager.getLogger(source)

  def log(message: String, mode: String) =

    if (mode.equalsIgnoreCase("INFO")) {
      logger.info(message)
    }
    else if (mode.equalsIgnoreCase("DEBUG")) {
      logger.debug(message)
    }
    else if (mode.equalsIgnoreCase("WARN")) {
      logger.warn(message)
    }
    else if (mode.equalsIgnoreCase("ERROR")) {
      logger.error(message)
    }

  def audit(message: String) = logger.info(message)
}



