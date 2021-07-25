package com.adjust.di.helper

import com.typesafe.scalalogging.LazyLogging
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import java.io.{BufferedReader, File, FileReader}
import java.sql.{Connection, DriverManager}

object DBHandler extends LazyLogging {

  /**
   * Create postgresql jdbc connection
   * @param configMap
   * @throws java.lang.Exception
   * @return Connection
   */
  @throws(classOf[Exception])
  def getConnection(configMap: Map[String, String]): Connection = {
    Class.forName("org.postgresql.Driver")
    val dbURL: String = configMap.getOrElse("readDBURL", null).toString
    val database: String = configMap.getOrElse("database", null).toString
    val username: String = configMap.getOrElse("dbUser", null).toString
    val pass: String = configMap.getOrElse("dbPass", null).toString

    DriverManager.getConnection(s"${dbURL}${database}?user=${username}&password=${pass}")
  }

  /**
   * Execute postgresql COPY command
   * @param exportFile
   * @param tableName
   * @param configMap
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def copyDataToDB(exportFile: File, tableName: String, columnNames: String, configMap: Map[String, String]): Unit = {
    var conn: Connection = null
    try {
      conn = getConnection(configMap)
      val copyCommand = s"COPY $tableName($columnNames) FROM STDIN (DELIMITER ',',FORMAT csv)"
      logger.error("DBHandler# copyDataToDB # copyCommand :" + copyCommand)
      val rowsInserted = new CopyManager(conn.asInstanceOf[BaseConnection])
        .copyIn(copyCommand, new BufferedReader(new FileReader(exportFile.getPath)))

      println(s"$rowsInserted row(s) inserted for file $exportFile")
    } catch {
      case exp: Exception => {
        logger.error("DBHandler# copyDataToDB # ERROR :" + exp.getMessage)
      }
    } finally {
      closeConnection(conn)
    }

  }

  /**
   * Closing postgresql connection
   * @param conn
   * @throws java.lang.Exception
   */
  @throws(classOf[Exception])
  def closeConnection(conn: Connection) = {
    try {
      if (conn != null) {
        conn.close()
      }
    } catch {
      case exp: Exception => {
        logger.error("DBHandler# closeConnection # ERROR :" + exp.getMessage)
      }
    } finally {
      try {
        if (conn != null) {
          conn.close()
        }
      } catch {
        case exp: Exception => {
          logger.error("DBHandler# closeConnection # ERROR :" + exp.getMessage)
        }
      }
    }
  }
}
