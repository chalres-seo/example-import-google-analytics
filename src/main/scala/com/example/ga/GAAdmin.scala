package com.example.ga

import java.io.FileInputStream

import com.example.utils.AppConfig
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.analyticsreporting.v4.{AnalyticsReporting, AnalyticsReportingScopes}

object GAAdmin {
  private val applicationName = AppConfig.getApplicationName
  private val credentialPath: String = AppConfig.getGoogleCredentialJsonPath
  private val googleCredential: GoogleCredential = this.createCredentials()
  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport()
  private val jsonFactory = GsonFactory.getDefaultInstance
  private val analyticsReportingService = this.initializeAnalyticsReporting()

  def getCredentials: GoogleCredential = {
    googleCredential
  }

  def getAnalyticsReportingService: AnalyticsReporting = {
    analyticsReportingService
  }

  private def createCredentials(): GoogleCredential = {
    GoogleCredential
      .fromStream(new FileInputStream(credentialPath))
      .createScoped(AnalyticsReportingScopes.all())
  }

  private def initializeAnalyticsReporting(): AnalyticsReporting = {
    new AnalyticsReporting.Builder(httpTransport, jsonFactory, googleCredential)
      .setApplicationName(applicationName).build()
  }
}
