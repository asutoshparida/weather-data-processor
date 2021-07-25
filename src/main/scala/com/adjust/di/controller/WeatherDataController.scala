package com.adjust.di.controller

import com.adjust.di.helper.GenericHelper
import com.adjust.di.service.WeatherDataService
import com.adjust.di.util.GenericUtility
import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WeatherDataController extends LazyLogging {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val pipelineName = "weather-data"

  def main(args: Array[String]): Unit = {
    {
      val envType = args(0)
      var sparkSession: SparkSession = null
      // Config SparkSession depending on what ENV we are running on.
      if ((envType == "DEV") || (envType == "QA")) {
        sparkSession = SparkSession.builder.appName(pipelineName).master("local[*]").getOrCreate()
      } else {
        sparkSession = SparkSession.builder.appName(pipelineName).getOrCreate()
      }
      // load the application level config as a Map[String, String]
      val configMap = GenericHelper.loadApplicationProperties()
      val sparkContext = sparkSession.sparkContext

      logger.info(s"WeatherDataController# main # Application configuration is Over")

      val genericUtility: GenericUtility = new GenericUtility()
      val weatherDataService: WeatherDataService = new WeatherDataService(genericUtility)

      try {
        weatherDataService.processWeatherDataAndDumpAsParquet(configMap, sparkSession.sqlContext)
      }
      catch {
        case exp: Exception => {
          logger.error("\n\n" + "WeatherDataController# main # ERROR :" + exp.getMessage)
          exp.printStackTrace()
        }
      }
      finally {
        logger.info("\n\n WeatherDataController# main # Stopping sparkContext \n\n")
        sparkContext.stop()
      }
    }
  }

}


