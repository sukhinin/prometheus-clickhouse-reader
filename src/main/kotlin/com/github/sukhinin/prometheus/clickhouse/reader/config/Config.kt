package com.github.sukhinin.prometheus.clickhouse.reader.config

data class Config(val server: ServerConfig, val query: QueryConfig, val clickHouse: ClickHouseConfig)

