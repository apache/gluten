/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.ui

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.ui.TypeAlias._
import org.apache.spark.ui.{PagedDataSource, PagedTable, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable
import scala.xml.{Node, NodeSeq}

private[ui] class GlutenAllExecutionsPage(parent: GlutenSQLTab) extends WebUIPage("") with Logging {

  private val sqlStore = parent.sqlStore

  override def render(request: HttpServletRequest): Seq[Node] = {
    val buildInfo = sqlStore.buildInfo()
    val data = sqlStore.executionsList()

    val content = {
      val _content = mutable.ListBuffer[Node]()

      val glutenPageTable =
        executionsTable(request, "gluten", data)

      _content ++=
        <span id="gluten" class="collapse-aggregated-runningExecutions collapse-table"
              onClick="collapseTable('collapse-aggregated-runningExecutions',
              'aggregated-runningExecutions')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a href="#gluten">
              Queries:
            </a>{data.size}
          </h4>
        </span> ++
          <div class="aggregated-runningExecutions collapsible-table">
            {glutenPageTable}
          </div>

      _content
    }
    content ++=
      <script>
        function clickDetail(details) {{
          details.parentNode.querySelector('.stage-details').classList.toggle('collapsed')
        }}
      </script>

    val infos = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      buildInfo.info,
      fixedWidth = true
    )
    val summary: NodeSeq =
      <div>
        <div>
          <span class="collapse-sql-properties collapse-table"
                onClick="collapseTable('collapse-sql-properties', 'sql-properties')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Gluten Build Information</a>
            </h4>
          </span>
          <div class="sql-properties collapsible-table collapsed">
            {infos}
          </div>
        </div>
      </div>

    UIUtils.headerSparkPage(request, "Gluten SQL / DataFrame", summary ++ content, parent)
  }

  private def propertyHeader = Seq("Name", "Value")

  private def propertyRow(kv: (String, String)) = <tr>
    <td>
      {kv._1}
    </td> <td>
      {kv._2}
    </td>
  </tr>

  private def executionsTable(
      request: HttpServletRequest,
      executionTag: String,
      executionData: Seq[GlutenSQLExecutionUIData]): Seq[Node] = {

    val executionPage =
      Option(request.getParameter(s"$executionTag.page")).map(_.toInt).getOrElse(1)

    val tableHeaderId = executionTag

    try {
      new GlutenExecutionPagedTable(
        request,
        parent,
        executionData,
        tableHeaderId,
        executionTag,
        UIUtils.prependBaseUri(request, parent.basePath),
        "gluten").table(executionPage)
    } catch {
      case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
        <div class="alert alert-error">
          <p>Error while rendering execution table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
    }
  }
}

private[ui] class GlutenExecutionPagedTable(
    request: HttpServletRequest,
    parent: GlutenSQLTab,
    data: Seq[GlutenSQLExecutionUIData],
    tableHeaderId: String,
    executionTag: String,
    basePath: String,
    subPath: String)
  extends PagedTable[GlutenExecutionTableRowData] {

  private val (sortColumn, desc, pageSize) = getTableParameters(request, executionTag, "ID")

  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  override val dataSource = new GlutenExecutionDataSource(data, pageSize, sortColumn, desc)

  private val parameterPath =
    s"$basePath/$subPath/?${getParameterOtherTable(request, executionTag)}"

  override def tableId: String = s"$executionTag-table"

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$executionTag.sort=$encodedSortColumn" +
      s"&$executionTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  override def pageSizeFormField: String = s"$executionTag.pageSize"

  override def pageNumberFormField: String = s"$executionTag.page"

  override def goButtonFormPath: String =
    s"$parameterPath&$executionTag.sort=$encodedSortColumn&$executionTag.desc=$desc#$tableHeaderId"

  // Information for each header: title, sortable, tooltip
  private val headerInfo: Seq[(String, Boolean, Option[String])] = {
    Seq(
      ("ID", true, None),
      ("Description", true, None),
      ("Num Gluten Nodes", true, None),
      ("Num Fallback Nodes", true, None))
  }

  override def headers: Seq[Node] = {
    isSortColumnValid(headerInfo, sortColumn)

    headerRow(headerInfo, desc, pageSize, sortColumn, parameterPath, executionTag, tableHeaderId)
  }

  override def row(executionTableRow: GlutenExecutionTableRowData): Seq[Node] = {
    val executionUIData = executionTableRow.executionUIData

    <tr>
      <td>
        {executionUIData.executionId.toString}
      </td>
      <td>
        {descriptionCell(executionUIData)}
      </td>
      <td sorttable_customkey={executionUIData.numGlutenNodes.toString}>
        {executionUIData.numGlutenNodes.toString}
      </td>
      <td sorttable_customkey={executionUIData.numFallbackNodes.toString}>
        {executionUIData.numFallbackNodes.toString}
      </td>
    </tr>
  }

  private def descriptionCell(execution: GlutenSQLExecutionUIData): Seq[Node] = {
    val details = if (execution.description != null && execution.description.nonEmpty) {
      val concat = new PlanStringConcat()
      concat.append("== Fallback Summary ==\n")
      val fallbackSummary = execution.fallbackNodeToReason
        .map {
          case (name, reason) =>
            val id = name.substring(0, 3)
            val nodeName = name.substring(4)
            s"(${id.toInt}) $nodeName: $reason"
        }
        .mkString("\n")
      concat.append(fallbackSummary)
      if (execution.fallbackNodeToReason.isEmpty) {
        concat.append("No fallback nodes")
      }
      concat.append("\n\n")
      concat.append(execution.fallbackDescription)

      <span onclick="this.parentNode.querySelector('.stage-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
        <div class="stage-details collapsed">
        <pre>{concat.toString()}</pre>
      </div>
    } else {
      Nil
    }

    val desc = if (execution.description != null && execution.description.nonEmpty) {
      <a href={executionURL(execution.executionId)} class="description-input">
        {execution.description}</a>
    } else {
      <a href={executionURL(execution.executionId)}>{execution.executionId}</a>
    }

    <div>{desc}{details}</div>
  }

  private def executionURL(executionID: Long): String =
    s"${UIUtils.prependBaseUri(request, parent.basePath)}/SQL/execution/?id=$executionID"
}

private[ui] class GlutenExecutionTableRowData(val executionUIData: GlutenSQLExecutionUIData)

private[ui] class GlutenExecutionDataSource(
    executionData: Seq[GlutenSQLExecutionUIData],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean)
  extends PagedDataSource[GlutenExecutionTableRowData](pageSize) {

  // Convert ExecutionData to ExecutionTableRowData which contains the final contents to show
  // in the table so that we can avoid creating duplicate contents during sorting the data
  private val data = executionData.map(executionRow).sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[GlutenExecutionTableRowData] =
    data.slice(from, to)

  private def executionRow(
      executionUIData: GlutenSQLExecutionUIData): GlutenExecutionTableRowData = {
    new GlutenExecutionTableRowData(executionUIData)
  }

  /** Return Ordering according to sortColumn and desc. */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[GlutenExecutionTableRowData] = {
    val ordering: Ordering[GlutenExecutionTableRowData] = sortColumn match {
      case "ID" => Ordering.by(_.executionUIData.executionId)
      case "Description" => Ordering.by(_.executionUIData.description)
      case "Num Gluten Nodes" => Ordering.by(_.executionUIData.numGlutenNodes)
      case "Num Fallback Nodes" => Ordering.by(_.executionUIData.numFallbackNodes)
      case unknownColumn => throw QueryExecutionErrors.unknownColumnError(unknownColumn)
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
