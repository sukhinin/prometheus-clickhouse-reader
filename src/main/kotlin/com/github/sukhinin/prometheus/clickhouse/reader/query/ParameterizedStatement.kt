package com.github.sukhinin.prometheus.clickhouse.reader.query

data class ParameterizedStatement(val body: String, val params: List<String>)
