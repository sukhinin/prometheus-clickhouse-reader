package com.github.sukhinin.prometheus.clickhouse.reader.config

object ServerConfigMapper {
    fun from(config: com.github.sukhinin.simpleconfig.Config) = ServerConfig(
        port = config.getInteger("port")
    )
}
