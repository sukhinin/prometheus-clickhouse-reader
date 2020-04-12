package com.github.sukhinin.prometheus.clickhouse.reader.data

data class TimeSeries(val labels: Collection<Label>, val samples: Collection<Sample>)
