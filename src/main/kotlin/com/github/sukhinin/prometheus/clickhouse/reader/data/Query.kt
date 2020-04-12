package com.github.sukhinin.prometheus.clickhouse.reader.data

data class Query(
    val start_timestamp_ms: Long,
    val end_timestamp_ms: Long,
    val matchers: Collection<LabelMatcher>,
    val hints: ReadHints
)
