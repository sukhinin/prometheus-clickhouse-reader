package com.github.sukhinin.prometheus.clickhouse.reader.config

import com.github.sukhinin.simpleconfig.scoped
import com.github.sukhinin.simpleconfig.toProperties

object ClickHouseConfigMapper {
    fun from(config: com.github.sukhinin.simpleconfig.Config) = ClickHouseConfig(
        url = config.get("url"),
        props = config.scoped("props").toProperties()
    )
}
