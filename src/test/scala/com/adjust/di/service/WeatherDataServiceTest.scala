package com.adjust.di.service

import com.adjust.di.util.GenericUtility
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Matchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito
import org.mockito.Mockito.spy

class WeatherDataServiceTest extends Serializable {

  val spark: SparkSession = SparkSession.builder.appName("WeatherDataServiceTest").master("local[*]").getOrCreate()
  val sqlContext = spark.sqlContext

  val spyGenericUtility: GenericUtility = spy( new GenericUtility() ) // Utility spy
  val spyWeatherDataService: WeatherDataService = spy( new WeatherDataService( spyGenericUtility ) ) // WeatherDataService spy

  val weatherDataDF: DataFrame = sqlContext.createDataFrame( Seq( WeatherData( "USM00070261", 1930, 8,26 ,99, 1700, 6 , "", "cdmp-usm", 648161, -1478767, 3,0,-9999, -9999, "", 250, "", -9999, "", -9999, -9999, 90,20) ,
    WeatherData( "USM00070261", 1930, 8,26 ,99, 1700, 6 , "", "cdmp-usm", 648161, -1478767, 3,0,-9999, -9999, "", 5000, "", -9999, "", -9999, -9999, 90,20) ))

  @Test
  def sqlQueryGeneratorTest() {
    val expected = s"(SELECT max(HEADER_ID) as max_head FROM public.weather_header ) weather_header_max"
    val actual = spyWeatherDataService.queryGenerator( "weather_header_max" )
    assertEquals( expected, actual )
  }


  @Test
  def updateMonitorTableTest(): Unit = {

    val paramMap: Map[String, String] = Map(
      "masterDBURL" -> "jdbc:postgresql://localhost:5432/",
      "dbUser" -> "postgres",
      "dbPass" -> "1@situnani",
      "database"-> "adjust",
      "dataSourceLocation"-> "/data/in"
    )

    val testQuery =
      """SELECT 1 AS id, 'weather-data' AS etl_name, 'N' AS skip_execution, 'Y' AS full_load, 'HEADER_ID' as filter_col1_name, '1231' as filter_col1_value """
    val testRow = spark.sql( testQuery ).collectAsList().get(0)

    Mockito.doNothing().when( spyGenericUtility ).executeSQL( any[Map[String, String]], any[String] )
    spyWeatherDataService.updateMonitorTable( testRow, "12321", paramMap )
  }

  @Test
  def processWeatherDataAndDumpAsParquetTest(): Unit = {

    val paramMap: Map[String, String] = Map(
      "masterDBURL" -> "jdbc:postgresql://localhost:5432/",
      "dbUser" -> "postgres",
      "dbPass" -> "1@situnani",
      "database"-> "adjust",
      "dataSourceLocation"-> "/data/in"
    )

    val monitorTableQuery =
      """SELECT 1 AS id, 'weather-data' AS etl_name, 'N' AS skip_execution, 'Y' AS full_load, 'HEADER_ID' as filter_col1_name, '1231' as filter_col1_value """
    val monitorTableDF = spark.sql( monitorTableQuery )

    val maxHeadIdQuery =
      """SELECT 123 AS max_head  """
    val maxHeadDF = spark.sql( maxHeadIdQuery )

    Mockito.doReturn(monitorTableDF).doReturn(maxHeadDF).doReturn(weatherDataDF).when( spyGenericUtility ).importFromDB( any[Map[String, String]], any[String], any[SQLContext] )
    Mockito.doNothing().when( spyWeatherDataService ).updateMonitorTable( any[Row], any[String], any[Map[String, String]] )
    Mockito.doNothing().when( spyWeatherDataService ).writeDataFrameAsParquet( any[DataFrame], any[String], any[Map[String, String]])
    spyWeatherDataService.processWeatherDataAndDumpAsParquet( paramMap, sqlContext)
  }



  case class WeatherData(id: String, year: Integer, month: Integer, day: Integer, hour: Integer,
    reltime: Integer, numlev: Integer, p_src: String, np_src: String, lat: Integer,
    lon: Integer, lvltyp1: Integer, lvltyp2: Integer, etime: Integer, press: Integer,
    pflag: String, gph: Integer, zflag: String, temp: Integer, tflag: String,
    rh: Integer, dpdp: Integer, wdir: Integer, wspd: Integer)

}
