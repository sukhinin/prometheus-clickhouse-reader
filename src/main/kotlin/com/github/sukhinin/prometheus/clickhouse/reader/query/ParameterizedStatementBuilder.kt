package com.github.sukhinin.prometheus.clickhouse.reader.query

import com.github.sukhinin.prometheus.clickhouse.reader.config.QueryConfig
import com.github.sukhinin.prometheus.clickhouse.reader.data.LabelMatcher
import com.github.sukhinin.prometheus.clickhouse.reader.data.Query

class ParameterizedStatementBuilder(private val config: QueryConfig) {
    fun getForQuery(query: Query): ParameterizedStatement {
        val selectStatement = buildDataSelectingStatement(query)
        return if (query.hints.step_ms > 0) {
            wrapWithGroupingQuery(selectStatement, query)
        } else {
            wrapWithSimpleQuery(selectStatement)
        }
    }

    private fun buildDataSelectingStatement(query: Query): ParameterizedStatement {
        val builder = StringBuilder()
        val parameters = ArrayList<String>()

        builder.append("SELECT * ")
        builder.append("FROM ${config.database}.${config.table} ")
        builder.append("WHERE timestamp >= ${query.start_timestamp_ms / 1000} ")
        builder.append("AND timestamp <= ${query.end_timestamp_ms / 1000} ")
        for (matcher in query.matchers) {
            val (expr, params) = renderFilter(matcher)
            builder.append("AND ($expr) ")
            parameters.addAll(params)
        }

        return ParameterizedStatement(
            builder.toString(),
            parameters
        )
    }

    private fun wrapWithSimpleQuery(stmt: ParameterizedStatement): ParameterizedStatement {
        val builder = StringBuilder()
        val extractedTags = config.extractTags
        val extractedColumns = if (extractedTags.isEmpty()) "" else extractedTags.joinToString(", ", ", ", " ")

        builder.append("SELECT metric, value as value, tags.name, tags.value, ")
        builder.append("toUnixTimestamp(timestamp) * 1000 AS t ")
        builder.append(extractedColumns)
        builder.append("FROM ( ")
        builder.append(stmt.body)
        builder.append(") ")
        builder.append("ORDER BY t ASC")

        return ParameterizedStatement(
            builder.toString(),
            stmt.params
        )
    }

    private fun wrapWithGroupingQuery(stmt: ParameterizedStatement, query: Query): ParameterizedStatement {
        val builder = StringBuilder()
        val step = query.hints.step_ms
        val extractedTags = config.extractTags
        val extractedColumns = if (extractedTags.isEmpty()) "" else extractedTags.joinToString(", ", ", ", " ")

        builder.append("SELECT metric, MAX(value) as value, tags.name, tags.value, ")
        builder.append("intDiv(toUnixTimestamp(timestamp) * 1000, $step) * $step AS t ")
        builder.append(extractedColumns)
        builder.append("FROM ( ")
        builder.append(stmt.body)
        builder.append(") ")
        builder.append("GROUP BY metric, tags.name, tags.value, t ")
        builder.append(extractedColumns)
        builder.append("ORDER BY t ASC")

        return ParameterizedStatement(
            builder.toString(),
            stmt.params
        )
    }

    private fun renderFilter(matcher: LabelMatcher): Pair<String, List<String>> {
        return when (matcher.type) {
            LabelMatcher.MATCH_TYPE_EQ -> renderEqualityFilter(matcher.name, matcher.value, "=")
            LabelMatcher.MATCH_TYPE_NEQ -> renderEqualityFilter(matcher.name, matcher.value, "!=")
            LabelMatcher.MATCH_TYPE_RE -> renderRegexFilter(matcher.name, matcher.value, "=", "LIKE", "OR")
            LabelMatcher.MATCH_TYPE_NRE -> renderRegexFilter(matcher.name, matcher.value, "!=", "NOT LIKE", "AND")
            else -> throw RuntimeException("Unknown matcher type: ${matcher.type}")
        }
    }

    private fun renderEqualityFilter(name: String, value: String, op: String): Pair<String, List<String>> {
        return when (name) {
            "__name__" -> Pair("metric $op ?", listOf(value))
            in config.extractTags -> Pair("$name $op ?", listOf(value))
            else -> Pair("tags.value[indexOf(tags.name, ?)] $op ?", listOf(name, value))
        }
    }

    private fun renderRegexFilter(name: String, regex: String, eqOp: String, reOp: String, boolOp: String): Pair<String, List<String>> {
        val filters = regex.split('|').map { expr ->
            if (expr.contains(".*")) {
                renderEqualityFilter(name, expr.replace(".*", "%"), reOp)
            } else {
                renderEqualityFilter(name, expr, eqOp)
            }
        }

        val expr = filters.joinToString(" $boolOp ") { (expr, _) -> expr }
        val params = filters.map { (_, params) -> params }.flatten()
        return Pair(expr, params)
    }
}
