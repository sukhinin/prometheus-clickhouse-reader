package com.github.sukhinin.prometheus.clickhouse.reader.config

import com.github.sukhinin.simpleconfig.scoped

object ConfigMapper {
    fun from(config: com.github.sukhinin.simpleconfig.Config) = Config(
        server = ServerConfigMapper.from(config.scoped("server")),
        query = QueryConfigMapper.from(config.scoped("query")),
        clickHouse = ClickHouseConfigMapper.from(config.scoped("clickhouse"))
    )
}
