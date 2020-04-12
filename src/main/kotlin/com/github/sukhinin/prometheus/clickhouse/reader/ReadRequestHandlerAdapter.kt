package com.github.sukhinin.prometheus.clickhouse.reader

import com.fasterxml.jackson.databind.ObjectReader
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.dataformat.protobuf.ProtobufMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.github.sukhinin.prometheus.clickhouse.reader.data.ReadRequest
import com.github.sukhinin.prometheus.clickhouse.reader.data.ReadResponse
import io.javalin.http.Context
import io.javalin.http.Handler
import org.xerial.snappy.Snappy
import java.io.ByteArrayInputStream

class ReadRequestHandlerAdapter(private val handler: ReadRequestHandler): Handler {

    private val reader = createRequestReader()
    private val writer = createResponseWriter()

    private fun createRequestReader(): ObjectReader {
        val mapper = ProtobufMapper().registerModule(KotlinModule()) as ProtobufMapper
        val schema = mapper.generateSchemaFor(ReadRequest::class.java)
        return mapper.readerFor(ReadRequest::class.java).with(schema)
    }

    private fun createResponseWriter(): ObjectWriter {
        val mapper = ProtobufMapper().registerModule(KotlinModule()) as ProtobufMapper
        val schema = mapper.generateSchemaFor(ReadResponse::class.java)
        return mapper.writerFor(ReadResponse::class.java).with(schema)
    }

    override fun handle(ctx: Context) {
        val requestData = Snappy.uncompress(ctx.bodyAsBytes())
        val request = reader.readValue<ReadRequest>(requestData)
        val response = handler.handle(request)
        val responseData = writer.writeValueAsBytes(response)
        ctx.result(ByteArrayInputStream(Snappy.compress(responseData)))
    }
}
