package com.github.sukhinin.prometheus.clickhouse.reader.config

data class QueryConfig(
    val database: String,
    val table: String,
    val extractTags: List<String>,
    val limitRows: Long
)
