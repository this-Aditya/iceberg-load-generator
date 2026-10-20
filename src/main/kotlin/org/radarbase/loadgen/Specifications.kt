package org.radarbase.loadgen

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.extension
import kotlin.io.path.isRegularFile
import kotlin.io.path.name
import kotlin.math.min

/** One topic to produce: its value schema, how often one user sends a record, and which device it belongs to. */
data class TopicSpec(
    val topic: String,
    val valueSchema: Schema,
    val ratePerSecond: Double,
    val sourceGroup: String,
)

/**
 * Reads RADAR-Schemas' specifications/ folder: every data entry with a topic and a value_schema, except the
 * stream/ folder, whose topics RADAR's Kafka Streams app writes. A topic listed in several spec files (for
 * example several versions of one device) is taken once, from the file that sorts last.
 */
object Specifications {
    private val logger = LoggerFactory.getLogger(Specifications::class.java)
    private val yaml = ObjectMapper(YAMLFactory())

    fun load(config: Config): List<TopicSpec> {
        val files = Files.walk(config.specificationsDir).use { paths ->
            paths.filter { it.isRegularFile() && it.extension in setOf("yml", "yaml") }
                .filter { config.specificationsDir.relativize(it).firstOrNull()?.toString() != "stream" }
                .sorted()
                .toList()
        }
        require(files.isNotEmpty()) { "No specification files found in ${config.specificationsDir}" }

        val byTopic = LinkedHashMap<String, Pair<JsonNode, Path>>()
        for (file in files) {
            val data = yaml.readTree(file.toFile())?.get("data") ?: continue
            for (entry in data) {
                val topic = entry["topic"]?.asText() ?: continue
                if (entry["value_schema"] == null) continue
                byTopic[topic] = entry to file
            }
        }

        return byTopic.mapNotNull { (topic, value) ->
            val (entry, file) = value
            if (!config.topicInclude.matches(topic) || config.topicExclude?.matches(topic) == true) {
                return@mapNotNull null
            }
            val schema = schemaOf(entry["value_schema"].asText()) ?: run {
                logger.warn("Skipping {}: no schema class for {}", topic, entry["value_schema"].asText())
                return@mapNotNull null
            }
            TopicSpec(topic, schema, ratePerSecond(entry["sample_rate"], config), file.name.substringBeforeLast('.'))
        }
    }

    /** `.passive.phone.PhoneAcceleration` means the generated class `org.radarcns.passive.phone.PhoneAcceleration`. */
    fun schemaOf(name: String): Schema? {
        val className = if (name.startsWith(".")) "org.radarcns$name" else name
        return try {
            Class.forName(className).getMethod("getClassSchema").invoke(null) as Schema
        } catch (ex: ReflectiveOperationException) {
            null
        }
    }

    /**
     * Records per second for one user: `frequency` as given, `interval` as one record per interval, anything else
     * (`dynamic`, or no sample rate) as one record per DEFAULT_INTERVAL_SECONDS. Then RATE_FACTOR, capped at
     * MAX_RATE_HZ, so a 50 Hz sensor does not flood the cluster.
     */
    private fun ratePerSecond(sampleRate: JsonNode?, config: Config): Double {
        val base = when {
            sampleRate?.get("frequency") != null -> sampleRate["frequency"].asDouble()
            sampleRate?.get("interval") != null -> 1.0 / sampleRate["interval"].asDouble()
            else -> 1.0 / config.defaultIntervalSeconds
        }
        return min(base * config.rateFactor, config.maxRateHz)
    }
}
