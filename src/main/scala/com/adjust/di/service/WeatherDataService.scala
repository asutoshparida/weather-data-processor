package com.adjust.di.service

import com.adjust.di.util.GenericUtility
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{IntegerType, TimestampType}


class WeatherDataService(utility: GenericUtility) extends LazyLogging {

  def queryGenerator(table: String, maxHeadId: String = "", lastRunHeadId: String = ""): String = table match {
    case "weather_etl_monitor" => s"(SELECT * FROM public.etl_monitor WHERE etl_name = 'weather-data' AND skip_execution != 'Y' ) weather_etl_monitor"
    case "weather_header_max" => s"(SELECT max(HEADER_ID) as max_head FROM public.weather_header ) weather_header_max"
    case "full_load_query" => s"(SELECT wh.ID, wh.YEAR, wh.MONTH, wh.DAY, wh.HOUR, wh.RELTIME, wh.NUMLEV, wh.P_SRC, wh.NP_SRC, wh.LAT, wh.LON, wd.LVLTYP1, wd.LVLTYP2, wd.ETIME, wd.PRESS, wd.PFLAG, wd.GPH, wd.ZFLAG, wd.TEMP, wd.TFLAG, wd.RH, wd.DPDP, wd.WDIR, wd.WSPD  FROM public.weather_header wh INNER JOIN public.weather_data wd ON wh.UID = wd.UID AND wh.header_id <= ${maxHeadId}) full_load_query"
    case "delta_load_query" => s"(SELECT wh.ID, wh.YEAR, wh.MONTH, wh.DAY, wh.HOUR, wh.RELTIME, wh.NUMLEV, wh.P_SRC, wh.NP_SRC, wh.LAT, wh.LON, wd.LVLTYP1, wd.LVLTYP2, wd.ETIME, wd.PRESS, wd.PFLAG, wd.GPH, wd.ZFLAG, wd.TEMP, wd.TFLAG, wd.RH, wd.DPDP, wd.WDIR, wd.WSPD  FROM public.weather_header wh INNER JOIN public.weather_data wd ON wh.UID = wd.UID AND wh.header_id <= ${maxHeadId} AND wh.header_id >= ${lastRunHeadId}) delta_load_query"
    case _ => "unknown"
  }

  /**
   * Process weather data from postgres then create a new column for partition, then flush to parquest location
   * This logic handles both full load & delta load scenarios using a monitor entry in DB.
   * @param configMap
   * @param sqlContext
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def processWeatherDataAndDumpAsParquet(configMap: Map[String, String], sqlContext: org.apache.spark.sql.SQLContext) = {

    var isFullLoad: String = null
    var incrementalColumnName: String = null
    var incrementalColumnValue: String = null
    var maxHeadId: String = null
    var dataLoadQuery: String = null

    val weatherETLMonitorQuery = queryGenerator("weather_etl_monitor")
    val monitorDf = utility.importFromDB(configMap, weatherETLMonitorQuery, sqlContext)
    val monitorDataList = monitorDf.collectAsList
    val listLength = monitorDataList.size()
    logger.info(s"WeatherDataService# processWeatherDataAndDumpAsParquet # Application configuration is Over")
    if (listLength > 0) {
      val rowData = monitorDataList.get(0)
      isFullLoad = utility.getFieldValue(rowData, "full_load")
      incrementalColumnName = utility.getFieldValue(rowData, "filter_col1_name")
      incrementalColumnValue = utility.getFieldValue(rowData, "filter_col1_value")
      logger.info(s"WeatherDataService# processWeatherDataAndDumpAsParquet # isFullLoad :"+ isFullLoad + "# incrementalColumnName :"+ incrementalColumnName + "# incrementalColumnValue :"+ incrementalColumnValue )

      val weatherHeaderMaxQuery = queryGenerator("weather_header_max")
      val maxHeadDf = utility.importFromDB(configMap, weatherHeaderMaxQuery, sqlContext)
      if(maxHeadDf.collectAsList.size() > 0){
        val maxRowData = maxHeadDf.collectAsList.get(0)
        maxHeadId = utility.getFieldValue(maxRowData, "max_head")
        if(isFullLoad.equalsIgnoreCase("Y")){
          dataLoadQuery = queryGenerator("full_load_query", maxHeadId)
        } else if(incrementalColumnName.equalsIgnoreCase("HEADER_ID")){
          dataLoadQuery = queryGenerator("delta_load_query", maxHeadId, incrementalColumnValue)
        }
        logger.info(s"WeatherDataService# processWeatherDataAndDumpAsParquet # dataLoadQuery :"+ dataLoadQuery )
        val weatherDataDF: DataFrame = utility.importFromDB(configMap, dataLoadQuery, sqlContext)
        val weatherPartitionDataDF: DataFrame = weatherDataDF.withColumn("partition_gph",
             when(col("gph") <= 1000, lit("1").cast(IntegerType))
            .when(col("gph") > 1000 && col("gph") <= 2000,lit("2").cast(IntegerType))
               .when(col("gph") > 2000 && col("gph") <= 3000,lit("3").cast(IntegerType))
               .when(col("gph") > 3000 && col("gph") <= 4000,lit("4").cast(IntegerType))
               .when(col("gph") > 4000 && col("gph") <= 5000,lit("5").cast(IntegerType))
               .when(col("gph") > 5000 && col("gph") <= 6000,lit("6").cast(IntegerType))
               .when(col("gph") > 6000 && col("gph") <= 7000,lit("7").cast(IntegerType))
               .when(col("gph") > 7000 && col("gph") <= 8000,lit("8").cast(IntegerType))
               .when(col("gph") > 8000 && col("gph") <= 9000,lit("9").cast(IntegerType))
          .otherwise(lit("0").cast(IntegerType)))

        writeDataFrameAsParquet(weatherPartitionDataDF, "partition_gph", configMap)
        updateMonitorTable(rowData, maxHeadId, configMap)
      }
    }
  }

  /**
   * update monitor table after each run
   * @param rowData
   * @param maxHeadId
   * @param configMap
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def updateMonitorTable(rowData: Row, maxHeadId: String, configMap: Map[String, String]): Unit = {
    try {
      val id = utility.getFieldValue(rowData, "id")
      val updateQuery =
        s"""UPDATE etl_monitor
                      SET run_date = current_timestamp,
                      filter_col1_value = '$maxHeadId',
                      FULL_LOAD = 'N' WHERE id = $id"""

      utility.executeSQL(configMap, updateQuery)
      logger.info(s"WeatherDataService# updateMonitorTable # updated with :" + maxHeadId);
    } catch {
      case exp: Exception =>
        val errorLog = s"Failed while updating the monitor table for maxHeadId '$maxHeadId'."
        logger.error(errorLog)
        utility.logException(configMap, "updateMonitorTable", "", exp, errorLog)
    }
  }

  /**
   * write DataFrame with partition column as Parquet
   * @param dataFrame
   * @param partitionColumn
   * @param configMap
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def writeDataFrameAsParquet(dataFrame: DataFrame, partitionColumn: String, configMap: Map[String, String]): Unit = {
    try {
      dataFrame.repartition(10).write.partitionBy(partitionColumn).parquet(configMap.getOrElse("parquetFileLocation", null))
    } catch {
      case exp: Exception =>
        val errorLog = s"Failed while writing data to parquet file."
        logger.error(errorLog)
        utility.logException(configMap, "updateMonitorTable", "", exp, errorLog)
    }
  }


}
