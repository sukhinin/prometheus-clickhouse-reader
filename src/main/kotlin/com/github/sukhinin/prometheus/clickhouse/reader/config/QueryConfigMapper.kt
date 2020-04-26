package com.github.sukhinin.prometheus.clickhouse.reader.config

import com.github.sukhinin.simpleconfig.getList
import com.github.sukhinin.simpleconfig.getLong

object QueryConfigMapper {
    fun from(config: com.github.sukhinin.simpleconfig.Config) = QueryConfig(
        database = config.get("database"),
        table = config.get("table"),
        extractTags = config.getList("extract.tags"),
        limitRows = config.getLong("limit.rows")
    )
}
