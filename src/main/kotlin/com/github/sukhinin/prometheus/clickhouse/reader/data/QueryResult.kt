package com.github.sukhinin.prometheus.clickhouse.reader.data

data class QueryResult(val timeseries: Collection<TimeSeries>)
