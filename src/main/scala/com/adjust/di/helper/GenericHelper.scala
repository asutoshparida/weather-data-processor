package com.adjust.di.helper

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging._
import net.liftweb.json.{DefaultFormats, _}

import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

object GenericHelper extends LazyLogging with App {

  case class RecordFormat(columnName: String, startIndex: Int, endIndex: Int, columnType: String, columnOrder: Int)

  /**
   * parse the JSON content of Header/Data Records.
   *
   * @throws java.lang.Exception
   * @return List[RecordFormat]
   */
  @throws(classOf[Exception])
  def loadRecordTypeProperties(recordType: String): List[RecordFormat] = {
    val recordTypeProperties = ArrayBuffer[RecordFormat]()
    var conf_source: BufferedSource = null
    implicit val formats: DefaultFormats.type = DefaultFormats

    if (recordType.equalsIgnoreCase("head")) {
      conf_source = Source.fromFile("src/main/config/header_record_format.json")
    } else if (recordType.equalsIgnoreCase("data")) {
      conf_source = Source.fromFile("src/main/config/data_record_format.json")
    }
    lazy val json_str = try conf_source.mkString finally conf_source.close()

    logger.info("GenericHelper# loadRecordHeaderProperties # type " + json_str)

    val json = parse(json_str)
    val elements = (json \\ "record_format").children
    for (acct <- elements) {
      val item = new RecordFormat(
        (acct \ "column_name").extract[String],
        (acct \ "start_index").extract[String].toInt,
        (acct \ "end_index").extract[String].toInt,
        (acct \ "column_type").extract[String],
        (acct \ "column_order").extract[String].toInt)
      recordTypeProperties += item
    }
    recordTypeProperties.toList
  }

  /**
   * Parse Header Records then create a comma separated string keeping type intact.
   *
   * @param record       : String
   * @param recordFormat : List[RecordFormat]
   * @throws java.lang.Exception
   * @return (recordCount: Int, recordId: String, structuredHeader: String)
   */
  @throws(classOf[Exception])
  def parseHeaderRecord(record: String, recordFormat: List[RecordFormat]): (Int, String, String) = {
    val HEADER_RECORD_SIZE = 71
    val HEADER_COLUMN_COUNT = 11
    if (record == null || record.trim.eq("") || record.length != HEADER_RECORD_SIZE || recordFormat == null || recordFormat.isEmpty) {
      return null;
    }
    var structuredHeader: String = ""
    var recordId: String = ""
    var recordCount: Int = 0
    for (format <- recordFormat) {
      val columnName: String = format.columnName
      val startIndex: Int = format.startIndex
      val endIndex: Int = format.endIndex
      val columnType: String = format.columnType
      val columnOrder: Int = format.columnOrder

      var columnValue: String = record.substring(startIndex - 1, endIndex).trim

      if (columnName.matches("ID|YEAR|MONTH|DAY|HOUR|RELTIME")) {
        recordId = recordId + columnValue
      } else if (columnName.matches("NUMLEV")) {
        recordCount = columnValue.toInt
      }

      if (columnType.equalsIgnoreCase("Character")) {
        columnValue = "\"" + columnValue + "\""
      }

      if (columnOrder == HEADER_COLUMN_COUNT) {
        structuredHeader = structuredHeader + columnValue + "," + "\"" + recordId + "\""
      } else {
        structuredHeader = structuredHeader + columnValue + ","
      }
    }
    return (recordCount, recordId, structuredHeader)
  }


  /**
   * Parse Data Records then create a comma separated string keeping type intact.
   *
   * @param records      : List[String]
   * @param recordId     : String
   * @param recordFormat : List[RecordFormat]
   * @throws java.lang.Exception
   * @return List[String]
   */
  @throws(classOf[Exception])
  def parseDataRecord(records: List[String], recordId: String, recordFormat: List[RecordFormat]): List[String] = {
    val DATA_RECORD_SIZE = 52
    val DATA_COLUMN_COUNT = 13
    if (records == null || records.isEmpty || recordFormat == null || recordFormat.isEmpty) {
      return null;
    }
    val structuredRecords = ArrayBuffer[String]()
    for (dataRecord <- records) {
      if (dataRecord != null && dataRecord.length == DATA_RECORD_SIZE) {
        var structuredData: String = ""
        for (format <- recordFormat) {
          val startIndex: Int = format.startIndex
          val endIndex: Int = format.endIndex
          val columnType: String = format.columnType
          val columnOrder: Int = format.columnOrder

          var columnValue: String = dataRecord.substring(startIndex - 1, endIndex).trim

          if (columnType.equalsIgnoreCase("Character")) {
            columnValue = "\"" + columnValue + "\""
          }

          if (columnOrder == DATA_COLUMN_COUNT) {
            structuredData = structuredData + columnValue + "," + "\"" + recordId + "\""
          } else {
            structuredData = structuredData + columnValue + ","
          }
        }
        structuredRecords.append(structuredData)
      }
    }
    structuredRecords.toList
  }

  /**
   * parse the JSON content on application properties.
   * @throws java.lang.Exception
   * @return Map[String, String]
   */
  @throws(classOf[Exception])
  def loadApplicationProperties(): Map[String, String] = {
    val json = Source.fromFile("src/main/config/pipeline_config.json")

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val parsedJson = mapper.readValue[Map[String, String]](json.reader())
    return parsedJson
  }

}
