package com.example.ga

import java.util

import com.example.utils.{AppConfig, AppUtils}
import com.google.api.services.analyticsreporting.v4.AnalyticsReporting
import com.google.api.services.analyticsreporting.v4.model._
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.collection.JavaConversions._

/**
  * Date string pattern:
  *   Date values can be for a specific date by using the pattern YYYY-MM-DD or
  *   relative by using today, yesterday, or the NdaysAgo pattern.
  *
  *   Values must match [0-9]{4}-[0-9]{2}-[0-9]{2}|today|yesterday|[0-9]+(daysAgo)
  *
  * Metrics and Dimensions:
  *   @see [[https://developers.google.com/analytics/devguides/reporting/core/dimsmets]]
**/
object GAReport extends LazyLogging {
  private val gaService = GAAdmin.getAnalyticsReportingService

  def createDateRange(startDate: String, endDate: String): DateRange = {
    logger.debug(s"create ga date range parameter: $startDate to $endDate")
    new DateRange()
      .setStartDate(startDate)
      .setEndDate(endDate)
  }

  def createMetric(metricName: String): Metric = {
    logger.debug(s"create ga metric parameter: $metricName")

    new Metric().setExpression(s"$metricName")
  }

  def createMetrics(metricNameList: Array[String]): Array[Metric] = {
    metricNameList.map(this.createMetric)
  }

  def createDimension(dimensionName: String): Dimension = {
    logger.debug(s"create ga dimension parameter: $dimensionName")
    new Dimension().setName(dimensionName)
  }

  def createDimensions(dimensionNameList:Option[Array[String]]): Option[Array[Dimension]] = {
    dimensionNameList.map(_.map(createDimension))
  }

  def createSortCondition(orderHeaderName: String): OrderBy = {
    logger.debug(s"create ga sort condition parameter: $orderHeaderName")
    new OrderBy().setFieldName(orderHeaderName)
  }

  def createSortConditions(orderHeaderNameList: Option[Array[String]]): Option[Array[OrderBy]] = {
    orderHeaderNameList.map(_.map(createSortCondition))
  }

  def createSegment(segmentId: String): Segment = {
    logger.debug(s"create ga segment parameter: $segmentId")
    new Segment().setSegmentId(segmentId)
  }

  def createSegments(segmentIdList: Option[Array[String]]): Option[Array[Segment]] = {
    segmentIdList.map(_.map(createSegment))
  }

  def createReportRequest(viewId: String,
                          dateRange: Array[DateRange],
                          metrics: Array[Metric],
                          dimensions: Option[Array[Dimension]],
                          sortConditions: Option[Array[OrderBy]],
                          segments: Option[Array[Segment]],
                          pageSize: Int,
                          pageToken: String
                         ): ReportRequest = {
    logger.debug(s"create report request: " +
      s"viewid: $viewId, " +
      s"daterange: ${dateRange}, " +
      s"dimensions:${dimensions}, " +
      s"sort: ${sortConditions}, " +
      s"segment: ${segments}, " +
      s"pageSize: $pageSize, " +
      s"pageToken: $pageToken")

    val reportRequest = new ReportRequest()

    reportRequest.setViewId(viewId)
    reportRequest.setDateRanges(dateRange.toList)
    reportRequest.setMetrics(metrics.toList)
    reportRequest.setPageSize(pageSize)
    reportRequest.setPageToken(pageToken)

    if (dimensions.isDefined) {
      reportRequest.setDimensions(dimensions.get.toList)
    }

    if (sortConditions.isDefined) {
      reportRequest.setOrderBys(sortConditions.get.toList)
    }

    if (segments.isDefined) {
      reportRequest.setSegments(segments.get.toList)
    }

    reportRequest
  }

  def getReportResponse(service: AnalyticsReporting, reportRequests: Array[ReportRequest]): GetReportsResponse = {
    val getReport = new GetReportsRequest()
      .setReportRequests(reportRequests.toList)

    service.reports().batchGet(getReport).execute()
  }

  def reportToVector(report: Report): Vector[String] = {
    val dimensionsHeader: String = report.getColumnHeader
      .getDimensions
      .mkString(",")
      .replace("ga", "dimension:ga")

    val metricsHeader: String = report.getColumnHeader
      .getMetricHeader
      .getMetricHeaderEntries
      .map("metric:" + _.getName)
      .mkString(",")

    val header: String = dimensionsHeader + "," + metricsHeader

    val data = report.getData
    val rows = data.getRows

    val records: Vector[String] = rows.map { row =>
      val dimensionsValues = row.getDimensions.mkString(",")
      val metrics = row.getMetrics.map(_.getValues.mkString(",")).mkString(",")
      val record = dimensionsValues + "," + metrics
      record
    }.toVector

    Vector(header) ++ records
  }

  def getHeaderFromReport(report: Report): String = {
    val dimensionsHeader: String = report.getColumnHeader
      .getDimensions
      .mkString(",")
      .replace("ga", "dimension:ga")

    val metricsHeader: String = report.getColumnHeader
      .getMetricHeader
      .getMetricHeaderEntries
      .map("metric:" + _.getName)
      .mkString(",")

    dimensionsHeader + "," + metricsHeader
  }

  def getRecordFromReport(report: Report) = {
    val data = report.getData
    val rows = data.getRows

    rows.map { row =>
      val dimensionsValues = row.getDimensions.mkString(",")
      val metrics = row.getMetrics.map(_.getValues.mkString(",")).mkString(",")
      dimensionsValues + "," + metrics
    }.toVector
  }

  //  def writeUsersDayHourlyReport(date:DateTime, viewId: String, schemaName: String): Unit = {
//    this.getSchemaReport(date, viewId, schemaName).foreach { report =>
//      val savePathString = AppConfig.getFileOutRootPath +
//        s"viewId=$viewId/date=${date.toString("yyyy-MM-dd")}/schema=$schemaName/" +
//        s"${schemaName}_create_time_${DateTime.now().toString("yyyy-MM-dd_HH:mm:ss")}.csv"
//
//      val records = GAReport.reportToVector(report)
//
//      AppUtils.writeFile(savePathString, records)
//    }
//  }
//
//  private def getSchemaReport(date: DateTime, viewId: String, schemaName: String): util.List[Report] = {
//    val dateRange = GAReport.createDateRange(date)
//
//    val metrics: Array[Metric] = GAReport.createMetrics(AppConfig.getGASchemaMetrics(schemaName))
//    val dimensions = GAReport.createDimensions(AppConfig.getGASchemaDimensions(schemaName))
//
//    val request = GAReport.createReportRequest(viewId, Array(dateRange), metrics, dimensions)
//    val response = GAReport.getReportResponse(gaService, request)
//
//    response.getReports
//  }
}