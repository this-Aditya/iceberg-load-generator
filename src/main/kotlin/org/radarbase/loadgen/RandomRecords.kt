package org.radarbase.loadgen

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.random.Random

/**
 * Builds a random but plausible record for any Avro schema. Every field is filled by its type, with a few
 * rules by field name so that times look like times: `time` is the event time, other double fields with "time"
 * in the name are a little later, `dateTime` and `date` strings are that time as text in UTC.
 */
class RandomRecords(private val random: Random) {
    fun record(schema: Schema, time: Double): GenericData.Record {
        val record = GenericData.Record(schema)
        for (field in schema.fields) {
            record.put(field.pos(), value(field.schema(), field.name(), time, 0))
        }
        return record
    }

    private fun value(schema: Schema, name: String, time: Double, depth: Int): Any? = when (schema.type) {
        Schema.Type.UNION -> {
            val options = schema.types.filter { it.type != Schema.Type.NULL }
            val nullable = options.size < schema.types.size
            // Time fields are always filled, even where the schema allows null.
            if (options.isEmpty() || (nullable && !isTime(name) && random.nextDouble() < 0.1)) {
                null
            } else {
                value(options[random.nextInt(options.size)], name, time, depth)
            }
        }
        Schema.Type.RECORD -> GenericData.Record(schema).also { record ->
            for (field in schema.fields) {
                record.put(field.pos(), value(field.schema(), field.name(), time, depth + 1))
            }
        }
        Schema.Type.ENUM -> GenericData.EnumSymbol(schema, schema.enumSymbols[random.nextInt(schema.enumSymbols.size)])
        Schema.Type.ARRAY -> GenericData.Array<Any?>(
            schema,
            List(if (depth > 4) 0 else 1 + random.nextInt(3)) { value(schema.elementType, name, time, depth + 1) },
        )
        Schema.Type.MAP -> if (depth > 4) {
            emptyMap<String, Any?>()
        } else {
            List(1 + random.nextInt(2)) { "key$it" to value(schema.valueType, name, time, depth + 1) }.toMap()
        }
        Schema.Type.STRING -> string(schema, name, time)
        Schema.Type.BYTES -> ByteBuffer.wrap(random.nextBytes(8))
        Schema.Type.FIXED -> GenericData.Fixed(schema, random.nextBytes(schema.fixedSize))
        Schema.Type.INT -> when {
            schema.logicalType?.name == "date" -> (time / 86400).toInt()
            name.contains("offset", ignoreCase = true) -> 3600 * random.nextInt(-1, 3)
            else -> random.nextInt(0, 100)
        }
        Schema.Type.LONG -> when {
            schema.logicalType?.name?.startsWith("timestamp") == true || isTime(name) -> (time * 1000).toLong()
            else -> random.nextLong(0, 10_000)
        }
        Schema.Type.FLOAT -> random.nextDouble(0.0, 100.0).toFloat()
        Schema.Type.DOUBLE -> when {
            name == "time" -> time
            name.contains("end", ignoreCase = true) && isTime(name) -> time + 60.0
            isTime(name) -> time + random.nextDouble(0.1, 2.0)
            else -> random.nextDouble(0.0, 100.0)
        }
        Schema.Type.BOOLEAN -> random.nextBoolean()
        Schema.Type.NULL -> null
    }

    private fun string(schema: Schema, name: String, time: Double): String {
        val instant = Instant.ofEpochMilli((time * 1000).toLong())
        return when {
            schema.logicalType?.name == "uuid" -> UUID.randomUUID().toString()
            name == "dateTime" -> DATE_TIME.format(instant)
            name == "date" -> DATE.format(instant)
            name.contains("timezone", ignoreCase = true) -> "Europe/London"
            else -> "$name-${random.nextInt(1000, 10_000)}"
        }
    }

    private fun isTime(name: String) = name.contains("time", ignoreCase = true)

    companion object {
        private val DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC)
        private val DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC)
    }
}
