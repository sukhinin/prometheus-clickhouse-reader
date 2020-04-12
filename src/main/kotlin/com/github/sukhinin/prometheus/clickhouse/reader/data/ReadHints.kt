package com.github.sukhinin.prometheus.clickhouse.reader.data

data class ReadHints(
    val step_ms: Long,
    val func: String?,
    val start_ms: Long,
    val end_ms: Long,
    val grouping: Collection<String>?,
    val by: Boolean?,
    val range_ms: Long
)
