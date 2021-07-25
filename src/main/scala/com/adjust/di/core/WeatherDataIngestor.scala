package com.adjust.di.core

import com.adjust.di.helper.{DBHandler, GenericHelper}
import com.adjust.di.util.GenericUtility
import com.typesafe.scalalogging._
import org.apache.commons.io.{FileUtils, LineIterator}

import java.io.File
import java.util
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContextExecutor

object WeatherDataIngestor extends LazyLogging with App {

  var genericUtility: GenericUtility = null

  /**
   * Trigger the ingestion process
   * Here we are storing to be processed files inside application dir where in actual we willbe getting them from S3 or SFTP folders
   * Here we are storing the intermediate files in local where in actual we will be strong them on S3.
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def startDataIngestion(): Unit = {
    var ectx: ExecutionContextExecutor = null
    val configMap = GenericHelper.loadApplicationProperties()
    genericUtility = new GenericUtility();
    val filesList: List[File] = genericUtility.getFileListInDataDir(configMap.getOrElse("dataSourceLocation", null), "txt")
    if(filesList != null && filesList.size > 0) {
      logger.info("WeatherDataIngestor# startDataIngestion # Started processing for: " + filesList.size + "files")
      val headerFormat: List[GenericHelper.RecordFormat] = GenericHelper.loadRecordTypeProperties("head")
      val recordFormat: List[GenericHelper.RecordFormat] = GenericHelper.loadRecordTypeProperties("data")

      val dataFlushLimit = configMap.getOrElse("dataFlushLimit", null).toInt

      /**
       * Multiple threads to process each file using Fixed Thread Pool
       */
      if (configMap.getOrElse("isMultiProcessing", null).equalsIgnoreCase("yes")) {
        ectx = scala.concurrent.ExecutionContext.
          fromExecutor(Executors.newFixedThreadPool(configMap.getOrElse("threadPoolSize", null).toInt: Int)
          )
      }

      for (dataFile: File <- filesList) {
        try {
          /**
           * Multiple threads trying to process each file
           */
          if (configMap.getOrElse("isMultiProcessing", null).equalsIgnoreCase("yes") && ectx != null) {
            ectx.execute(new Runnable {
              def run() =
                processInFilesByLine(dataFile, headerFormat, recordFormat, dataFlushLimit, genericUtility)

              // Move file to processed directory
              genericUtility.moveFilesToDir(dataFile, configMap.getOrElse("processedSourceLocation", null))
            })
          } else {
            /**
             * sequential file Execution.
             */
            processInFilesByLine(dataFile, headerFormat, recordFormat, dataFlushLimit, genericUtility)
            // Move file to processed directory
            genericUtility.moveFilesToDir(dataFile, configMap.getOrElse("processedSourceLocation", null))
          }
        } catch {
          case ex: Exception =>
            val errorLog = s"WeatherDataIngestor # startDataIngestion # processInFilesByLine # exception in processing... '${dataFile.getName}' with ERROR : ${ex.getMessage}"
            genericUtility.logException(configMap, "processInFilesByLine", "", ex, errorLog)
            logger.error(errorLog)
            ex.printStackTrace()
        }
      }
      val structuredFilesList: List[File] = genericUtility.getFileListInDataDir(configMap.getOrElse("dataDestLocation", null), "csv")
      logger.info("WeatherDataIngestor# startDataIngestion # Starting pushStructuredDataToPostgres : " + structuredFilesList)
      for (structuredDataFile: File <- structuredFilesList) {
        try {
          pushStructuredDataToPostgres(structuredDataFile, configMap)
          // Move file to processed directory
          genericUtility.moveFilesToDir(structuredDataFile, configMap.getOrElse("processedDestLocation", null))
        } catch {
          case ex: Exception =>
            val errorLog = s"WeatherDataIngestor # startDataIngestion # pushStructuredDataToPostgres # exception in processing... '${structuredDataFile.getName}' with ERROR : ${ex.getMessage}"
            genericUtility.logException(configMap, "pushStructuredDataToPostgres", "", ex, errorLog)
            logger.error(errorLog)
            ex.printStackTrace()
        }
      }
    }
  }

  /**
   * Process each file per line then prepare 2 different csv files one for header rows, other for data rows.
   * For reading a huge file we are using apache.commons.IO.FileUtils.lineIterator,
   * which will stream the data from file instead of loading every thing to memory.
   * @param file
   * @param headerFormat
   * @param recordFormat
   * @param dataFlushLimit
   * @param genericUtility
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def processInFilesByLine(file: File, headerFormat: List[GenericHelper.RecordFormat], recordFormat: List[GenericHelper.RecordFormat], dataFlushLimit: Int, genericUtility: GenericUtility): Boolean = {
    if (file == null) {
      return false
    }
    var groupRecordIndex = 0
    var recordCounter = 0
    val inFileName: String = file.getName
    logger.info("WeatherDataIngestor# processInFilesByLine # Started processing for: " + inFileName)
    val fileNameWithoutExtension: String = inFileName.substring(0, inFileName.lastIndexOf("."))
    var rawDataRecords = ArrayBuffer[String]()
    var structuredHeaders: java.util.List[String] = new java.util.ArrayList[String]()
    var structuredData: java.util.List[String] = new java.util.ArrayList[String]()
    val dataDir = genericUtility.createDataDir()
    var structuredHeaderValue = (0, "", "")
    val it = FileUtils.lineIterator(file, "UTF-8")
    val headerFileName = "headers-" + fileNameWithoutExtension + ".csv"
    val dataFileName = "records-" + fileNameWithoutExtension + ".csv"
    try while ( {
      it.hasNext
    }) {
      val line: String = it.nextLine
      //Processing block for header rows
      if (line != null && line.startsWith("#")) {
        structuredHeaderValue = (0, "", "")
        /**
         * Because we will be writing a huge csv file, rather than
         * writing all records at a time we can flush the data at a certain limit(which is configurable)
         */
        if (recordCounter >= dataFlushLimit) {
          genericUtility.writeDataToCSVFile(headerFileName, dataDir, structuredHeaders)
          genericUtility.writeDataToCSVFile(dataFileName, dataDir, structuredData)
          structuredData = new java.util.ArrayList[String]()
          structuredHeaders = new java.util.ArrayList[String]()
          logger.info("WeatherDataIngestor# processInFilesByLine # Flushed data to csv because of dataFlushLimit: ")
        }
        structuredHeaderValue = GenericHelper.parseHeaderRecord(line, headerFormat)
        rawDataRecords = ArrayBuffer[String]()
        groupRecordIndex = 0
        structuredHeaders.add(structuredHeaderValue._3)
        //Processing block for data rows
      } else {
        groupRecordIndex += 1
        if (structuredHeaderValue._1 > 0 && groupRecordIndex <= structuredHeaderValue._1) {
          rawDataRecords.append(line)
          if (groupRecordIndex == structuredHeaderValue._1) {
            val groupDataRecords: List[String] = GenericHelper.parseDataRecord(rawDataRecords.toList, structuredHeaderValue._2, recordFormat)
            structuredData.addAll(new java.util.ArrayList[String](groupDataRecords.asJava))
          }
        }
      }
      recordCounter += 1
    }
    finally {
      // Flush the remaining data at end of the block.
      genericUtility.writeDataToCSVFile(headerFileName, dataDir, structuredHeaders)
      genericUtility.writeDataToCSVFile(dataFileName, dataDir, structuredData)
      logger.info("WeatherDataIngestor# processInFilesByLine # Flushed data to csv because of loop exit: ")
      LineIterator.closeQuietly(it)
    }
    true
  }

  /**
   * Push formated csv data to Postgres
   * @param file
   * @param configMap
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def pushStructuredDataToPostgres(file: File, configMap: Map[String, String]): Boolean = {
    if (file == null) {
      return false;
    }
    val inFileName: String = file.getName
    val fileExtension: String = inFileName.substring(inFileName.lastIndexOf(".") + 1)
    logger.info("WeatherDataIngestor# pushStructuredDataToPostgres # Started processing for: " + inFileName)
    if(inFileName.startsWith("headers-") && fileExtension.equalsIgnoreCase("csv")){
      val columnNames: String = "ID,YEAR,MONTH, DAY, HOUR, RELTIME, NUMLEV, P_SRC, NP_SRC, LAT, LON, UID"
      DBHandler.copyDataToDB(file, "weather_header", columnNames, configMap)
    } else if(inFileName.startsWith("records-") && fileExtension.equalsIgnoreCase("csv")){
      val columnNames: String = "LVLTYP1, LVLTYP2, ETIME, PRESS, PFLAG, GPH, ZFLAG, TEMP, TFLAG, RH, DPDP, WDIR, WSPD, UID"
      DBHandler.copyDataToDB(file, "weather_data", columnNames, configMap)
    }
    true
  }

  override def main(args: Array[String]): Unit = {
//    val genericUtility = new GenericUtility();
//    val file = new File("/opt/SAP-2607/weather-data-processor/data/in/USM00070261-data.txt")
//    genericUtility.moveFilesToDir(file, "/data/in/processed/")

    startDataIngestion()
  }

}
