package com.example.utils

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object AppUtils extends LazyLogging {
  private val gson: Gson = new Gson()
  private val mapType = new TypeToken[util.HashMap[String, String]]{}.getType
  private val mapClass: Class[_ <: util.HashMap[String, String]] = new util.HashMap[String, String]().getClass

  def writeFile(savePathString: String, records: Vector[String], append: Boolean) = {
    val savePath = Paths.get(savePathString)

    if(Files.notExists(savePath)) {
      Files.createDirectories(savePath.getParent)
    }

    val fileWriter = if (append) {
      Files.newBufferedWriter(savePath,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND)
    } else {
      Files.newBufferedWriter(savePath,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW)
    }

    records.foreach { record =>
      Try {
        fileWriter.write(record)
        fileWriter.newLine()
      } match {
        case Success(_) =>
          logger.debug(s"write record: $record")
        case Failure(e) =>
          logger.error(e.getMessage)
          logger.error(record)
      }
    }

    logger.debug("file flushing and close.")
    fileWriter.flush()
    fileWriter.close()
  }

  def readJsonFromFileToMap(readFilePathString: String): mutable.Map[String, String] = {
    val readPath = Paths.get(readFilePathString)

    if(Files.notExists(readPath)) {
      logger.error(s"file is not exist: ${readPath}")
      return mutable.Map.empty[String, String]
    }

    val fileReader = Files.newBufferedReader(readPath)

    val resultMap: mutable.Map[String, String] = Try(gson.fromJson(fileReader, mapClass).asScala) match {
      case Success(result) =>
        result
      case Failure(e) =>
        logger.error(e.getMessage)
        mutable.Map.empty[String, String]
    }

    fileReader.close()
    resultMap
  }
}