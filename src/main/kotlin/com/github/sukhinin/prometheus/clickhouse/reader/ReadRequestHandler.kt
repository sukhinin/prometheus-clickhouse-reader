package com.github.sukhinin.prometheus.clickhouse.reader

import com.github.sukhinin.prometheus.clickhouse.reader.config.QueryConfig
import com.github.sukhinin.prometheus.clickhouse.reader.data.*
import com.github.sukhinin.prometheus.clickhouse.reader.query.ParameterizedStatementBuilder
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import javax.sql.DataSource

class ReadRequestHandler(
    private val queryConfig: QueryConfig,
    private val datasource: DataSource,
    private val statementBuilder: ParameterizedStatementBuilder
) {

    private val logger = LoggerFactory.getLogger(ReadRequestHandler::class.java)

    fun handle(request: ReadRequest): ReadResponse {
        logger.info("Request: $request")
        val results = request.queries.map(::getQueryResults)
        return ReadResponse(results)
    }

    private fun getQueryResults(query: Query): QueryResult {
        val (body, params) = statementBuilder.getForQuery(query)
        logger.info("Query: $query")
        logger.info("Statement: $body, params: $params")

        datasource.connection.use { conn ->
            val stmt = conn.prepareStatement(body)
            for (i in params.indices) {
                stmt.setString(i + 1, params[i])
            }
            return mapResultSet(stmt.executeQuery())
        }
    }

    private fun mapResultSet(resultSet: ResultSet): QueryResult {
        val columns = getResultSetColumns(resultSet)

        val results = ArrayList<Result>()
        while (resultSet.next()) {
            val result = mapCurrentResultSetRow(resultSet, columns)
            results.add(result)
        }

        val timeseries = results
            .groupBy({ it.labels }, { it.sample })
            .map { (labels, samples) -> TimeSeries(labels, samples) }

        logger.info("Mapped ${results.size} rows in ${timeseries.size} time series")
        return QueryResult(timeseries)
    }

    private fun mapCurrentResultSetRow(resultSet: ResultSet, columns: List<Column>): Result {
        val sample = Sample(resultSet.getDouble("value"), resultSet.getLong("t"))
        val labels = ArrayList<Label>()

        for ((index, name) in columns) {
            if (name == "metric") {
                addLabel(labels, name, resultSet.getString(index))
            } else if (name in queryConfig.extractTags) {
                addLabel(labels, name, resultSet.getString(index))
            }
        }

        @Suppress("UNCHECKED_CAST") val tagNames = resultSet.getArray("tags.name").array as Array<String>
        @Suppress("UNCHECKED_CAST") val tagValues = resultSet.getArray("tags.value").array as Array<String>
        check(tagNames.size == tagValues.size) { "Tags array size mismatch: ${tagNames.size} != ${tagValues.size}" }

        for ((i, name) in tagNames.withIndex()) {
            if (labels.none { it.name == name }) {
                addLabel(labels, name, tagValues[i])
            }
        }

        return Result(sample, labels)
    }

    private fun addLabel(labels: MutableList<Label>, name: String, value: String?) {
        if (!value.isNullOrEmpty()) {
            labels.add(Label(name, value))
        }
    }

    private fun getResultSetColumns(resultSet: ResultSet): List<Column> {
        val count = resultSet.metaData.columnCount
        val columns = ArrayList<Column>()
        for (i in 1..count) {
            columns.add(Column(i, resultSet.metaData.getColumnName(i)))
        }
        return columns
    }

    data class Column(val index: Int, val name: String)
    data class Result(val sample: Sample, val labels: Collection<Label>)
}
