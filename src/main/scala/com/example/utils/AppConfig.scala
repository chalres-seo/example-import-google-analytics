package com.example.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

object AppConfig extends LazyLogging {
  // read application.conf
  private val conf: Config = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()

  def getGoogleCredentialJsonPath: String = conf.getString("google.credentials.path")
  def getApplicationName: String = conf.getString("application.name")
}