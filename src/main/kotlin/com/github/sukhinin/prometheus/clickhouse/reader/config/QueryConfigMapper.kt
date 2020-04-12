package com.github.sukhinin.prometheus.clickhouse.reader.config

object QueryConfigMapper {
    fun from(config: com.github.sukhinin.simpleconfig.Config) = QueryConfig(
        database = config.get("database"),
        table = config.get("table"),
        extractTags = config.getList("extract.tags")
    )
}
