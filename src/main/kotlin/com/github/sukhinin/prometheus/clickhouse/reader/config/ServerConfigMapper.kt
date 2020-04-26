package com.github.sukhinin.prometheus.clickhouse.reader.config

import com.github.sukhinin.simpleconfig.getInteger

object ServerConfigMapper {
    fun from(config: com.github.sukhinin.simpleconfig.Config) = ServerConfig(
        port = config.getInteger("port")
    )
}
