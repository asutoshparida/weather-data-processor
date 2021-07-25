package com.adjust.di.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.NameFileFilter
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{File, FilenameFilter}
import java.sql.DriverManager
import java.time.LocalDateTime
import java.util
import scala.collection.JavaConverters._

class GenericUtility extends LazyLogging {

  /**
   * Flush data to file
   *
   * @param fileName
   * @param filePath
   * @param encoding
   * @param data
   * @throws java.lang.Exception
   * @return boolean
   */
  @throws(classOf[Exception])
  def writeDataToCSVFile(fileName: String, filePath: String, data: java.util.List[String]): Boolean = {
    if (fileName == null || filePath == null || data == null || data.isEmpty) {
      return false
    }
    val file = new File(filePath + "/" + fileName)
    FileUtils.writeLines(file, data, System.lineSeparator(), true);
    true;
  }

  /**
   * Create a data dir in current projectContext
   *
   * @return directory path
   */
  def createDataDir(): String = {
    val userDir = System.getProperty("user.dir")
    val dirPath: String = userDir + "/data/out"
    val theDir = new File(dirPath)
    if (!theDir.exists) {
      theDir.mkdirs
    }
    dirPath
  }

  def getDateTime(): String = {
    try {
      return s"${LocalDateTime.now.getDayOfMonth()}/${LocalDateTime.now.getMonthValue()}/${LocalDateTime.now.getYear()} ${LocalDateTime.now.getHour()}:${LocalDateTime.now.getMinute()}:${LocalDateTime.now.getSecond()}"
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        return null
      }
    }
  }

  /**
   * Method to execute CUD operations on DB
   *
   * @param sqlHostURL
   * @param username
   * @param password
   * @param query
   */
  def executeSQL(configMap: Map[String, String], query: String): Unit = {
    Class.forName("org.postgresql.Driver")
    val dbURL: String = configMap.getOrElse("masterDBURL", null)
    val database: String = configMap.getOrElse("database", null)
    val sqlHostURL = dbURL + database
    val username: String = configMap.getOrElse("dbUser", null)
    val password: String = configMap.getOrElse("dbPass", null)
    val connection = DriverManager.getConnection(sqlHostURL, username, password)
    val statement = connection.createStatement
    statement.executeUpdate(query)
  }


  /**
   * Dump dataframe to mysql table
   *
   * @param sqlHostURL
   * @param userName
   * @param password
   * @param dataFrame
   * @param tableName
   */
  @throws(classOf[Exception])
  def exportToDB(sqlHostURL: String, userName: String, password: String, dataFrame: DataFrame, tableName: String): Unit = {
    val prop = new java.util.Properties
    prop.put("driver", "org.postgresql.Driver")
    prop.put("user", userName)
    prop.put("password", password)
    try {
      logger.info(s"GenericUtility # exportToMysql # Exporting record(s) to the table: $tableName")
      // dataFrame.show()
      dataFrame.write.mode("append").jdbc(sqlHostURL, tableName, prop)
      logger.info(s"# GenericUtility # exportToMysql # Exported record(s) to the table: $tableName")
    } catch {
      case ex: Throwable =>
        logger.error(s"GenericUtility # exportToMysql # exception in exporting... '$tableName' with ERROR : ${ex.getMessage}")
        ex.printStackTrace()
        throw ex
    }
  }

  /**
   * Read data from postgresql using pushdown query
   * @param configMap
   * @param pushdownQuery
   * @param sqlContext
   * @return
   */
  @throws(classOf[Exception])
  def importFromDB(configMap: Map[String, String], pushdownQuery: String, sqlContext: org.apache.spark.sql.SQLContext): DataFrame = {
    val sqlHost: String = configMap.getOrElse("masterDBURL", null).toString
    val database: String = configMap.getOrElse("database", null).toString
    val userName: String = configMap.getOrElse("dbUser", null).toString
    val password: String = configMap.getOrElse("dbPass", null).toString
    val prop = new java.util.Properties()
    prop.put("driver", "org.postgresql.Driver")
    prop.put("user", userName)
    prop.put("password", password)
    val df = sqlContext.read.jdbc(url = sqlHost + database, pushdownQuery, properties = prop)
    return df
  }


  /**
   * Get List of files in the /data/in folder
   *
   * @param path
   * @return Array[File]
   */
  @throws(classOf[Exception])
  def getFileListInDataDir(path: String, extensionType: String): List[Filexx] = {
    val userDir = System.getProperty("user.dir")
    val dirPath: File = new File(userDir + path)
    val filesList = FileUtils.listFiles(dirPath, Array(extensionType), false).asScala
    filesList.toList
  }

  /**
   * Move Files To Directory
   *
   * @param file
   * @param dirPath
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def moveFilesToDir(file: File, dirPath: String) = {
    try {
      val userDir = System.getProperty("user.dir")
      val newFile: File = new File(userDir + dirPath + file.getName)
      FileUtils.moveFile(file, newFile)
    } catch {
      case exp: Exception =>
        logger.error(s": GenericUtility # moveFilesToDir # Failed while moving file to : ${dirPath}. Exception:${exp.getMessage}")
        exp.printStackTrace()
    }
  }

  /**
   * Logs Exception to exception_log table
   *
   * @param sqlHost
   * @param userName
   * @param password
   * @param stepName
   * @param client
   * @param exp
   * @param customMsg
   */
  @throws(classOf[Exception])
  def logException(configMap: Map[String, String], stepName: String, client: String, exp: Exception, customMsg: String): Unit = {
    val sqlHost: String = configMap.getOrElse("readDBURL", null).toString
    val userName: String = configMap.getOrElse("dbUser", null).toString
    val password: String = configMap.getOrElse("dbPass", null).toString
    val exceptionMessage = exp.getMessage
    logger.info(s"GenericUtility # logException # [Exceptions]: $customMsg. Exception: $exceptionMessage")
    try {
      val logQuery =
        s"""INSERT INTO public.exception_log (date_time, etl_name, client_name, exception_message)
        VALUES ( now(), '$stepName', '$client', '${exceptionMessage.replaceAll("'", "''")}')"""
      logger.info(s"GenericUtility # logException # [Exceptions]: ****Exception Query****: $logQuery")
      //      executeSQL(sqlHost, userName, password, logQuery)
    } catch {
      case exp: Exception =>
        logger.error(s": GenericUtility # logException # Failed while logging exception to table: 'public.exception_log'. Exception:${exp.getMessage}")
        exp.printStackTrace()
    }
  }

  def getFieldValue(rowData: Row, field: String): String = {
    try {
      rowData.get(rowData.fieldIndex(field)).toString
    } catch {
      case _: Exception =>
        null
    }
  }

}