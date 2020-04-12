package com.github.sukhinin.prometheus.clickhouse.reader.data

data class LabelMatcher(val type: Int, val name: String, val value: String) {
    companion object {
        const val MATCH_TYPE_EQ = 0
        const val MATCH_TYPE_NEQ = 1
        const val MATCH_TYPE_RE = 2
        const val MATCH_TYPE_NRE = 3
    }
}
