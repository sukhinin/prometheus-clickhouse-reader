package com.github.sukhinin.prometheus.clickhouse.reader.config

import java.util.*

data class ClickHouseConfig(
    val url: String,
    val props: Properties
)
