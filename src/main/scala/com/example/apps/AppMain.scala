package com.example.apps

import com.example.ga.{GAAdmin, GAReport}
import com.example.utils.AppUtils
import com.google.api.services.analyticsreporting.v4.model.{Report, ReportRequest}
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.JavaConversions._

object AppMain extends LazyLogging {
  private val gaService = GAAdmin.getAnalyticsReportingService

  def main(args: Array[String]): Unit = {
    logger.info("start app main.")

    require(args.length == 2, "Need 2 argument. 'query json file path', 'save dir path'")

    val gaQueryJsonFilePathString = args(0)
    val queryResultSavePathString = args(1)

    logger.info(s"read json file: $gaQueryJsonFilePathString")
    logger.info(s"result save path: $queryResultSavePathString")

    val gaQuery: mutable.Map[String, String] = AppUtils.readJsonFromFileToMap(gaQueryJsonFilePathString)

    val (viewId: String, startDate: String, endDate: String, metrics: Array[String]) = try {
      (gaQuery("ids"), gaQuery("start_date"), gaQuery("end_date"), gaQuery("metrics").split(","))
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        logger.error("Query parse error. App closed.")
        System.exit(1)
    }

    val (dimensions: Option[Array[String]], sort: Option[Array[String]], segments: Option[Array[String]]) = try {
      (
        gaQuery.get("dimensions").map(_.split(",")),
        gaQuery.get("sort").map(_.split(",")),
        gaQuery.get("segments").map(_.split(","))
      )
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        logger.error("Query parse error. App closed.")
        System.exit(1)
    }

    val pageRowCount = gaQuery.getOrElse("page_row_count", "10000").toInt

    logger.debug(
      s"ga query parse result: " +
      s"view: $viewId, " +
      s"start_date: $startDate, " +
      s"end_date: $endDate, " +
      s"metrics: : ${metrics.mkString(",")}, " +
      s"dimensions: ${dimensions.mkString(",")}, " +
      s"sort : ${sort.mkString(",")}, " +
      s"segments : ${segments.mkString(",")}, " +
      s"page_row_count : $pageRowCount")

    val gaDateRanges = Array(GAReport.createDateRange(startDate, endDate))
    val gaMetrics = GAReport.createMetrics(metrics)
    val gaDimensions = GAReport.createDimensions(dimensions)
    val gaSort = GAReport.createSortConditions(sort)
    val gaSegments = GAReport.createSegments(segments)

    val gaReportRequests: ReportRequest = GAReport.createReportRequest(viewId,
      gaDateRanges,
      gaMetrics,
      gaDimensions,
      gaSort,
      gaSegments,
      pageRowCount,
    "")

    val gaReportResponse: Report = GAReport.getReportResponse(gaService, Array(gaReportRequests))
      .getReports.head

    val gaReportHeader = GAReport.getHeaderFromReport(gaReportResponse)

    logger.info(s"result header: $gaReportHeader")
    AppUtils.writeFile(queryResultSavePathString, Vector(gaReportHeader), append = false)

    loop(gaReportRequests, gaReportResponse)

    @tailrec
    def loop(currentReportRequest: ReportRequest, currentReports: Report): Unit = {
      val currentReportRecord = GAReport.getRecordFromReport(currentReports)
      val currentNextToken = currentReports.getNextPageToken

      logger.info(s"ga report record count: ${currentReportRecord.size}")
      logger.info(s"ga report next page token: $currentNextToken")

      AppUtils.writeFile(queryResultSavePathString, currentReportRecord, append = true)

      if (currentNextToken != null) {
        val nextReportRequest = currentReportRequest.setPageToken(currentNextToken)
        val nextReportResponse = GAReport.getReportResponse(gaService, Array(nextReportRequest)).getReports.head
        loop(nextReportRequest, nextReportResponse)
      }
    }
  }

}