package org.radarbase.loadgen

import java.nio.file.Path
import java.nio.file.Paths

/** All settings, read from environment variables. The README lists each one. */
data class Config(
    val bootstrapServers: String,
    val schemaRegistryUrl: String,
    val specificationsDir: Path,
    val projects: List<String>,
    val usersPerReplica: Int,
    val maxRateHz: Double,
    val rateFactor: Double,
    val defaultIntervalSeconds: Double,
    val threads: Int,
    val autoRegisterSchemas: Boolean,
    val createMissingTopics: Boolean,
    val topicPartitions: Int,
    val topicReplicationFactor: Short,
    val topicInclude: Regex,
    val topicExclude: Regex?,
    val replicaId: String,
    val statsIntervalSeconds: Long,
) {
    companion object {
        fun fromEnv(): Config {
            fun env(name: String, default: String): String =
                System.getenv(name)?.takeIf { it.isNotBlank() } ?: default

            return Config(
                bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "cp-kafka-headless:9092"),
                schemaRegistryUrl = env("SCHEMA_REGISTRY_URL", "http://cp-schema-registry:8081"),
                specificationsDir = Paths.get(env("SPECIFICATIONS_DIR", "/app/specifications")),
                projects = env("PROJECTS", "LoadTest-A,LoadTest-B").split(',').map { it.trim() }.filter { it.isNotEmpty() },
                usersPerReplica = env("USERS_PER_REPLICA", "50").toInt(),
                maxRateHz = env("MAX_RATE_HZ", "1.0").toDouble(),
                rateFactor = env("RATE_FACTOR", "1.0").toDouble(),
                defaultIntervalSeconds = env("DEFAULT_INTERVAL_SECONDS", "300").toDouble(),
                threads = env("THREADS", "4").toInt(),
                autoRegisterSchemas = env("AUTO_REGISTER_SCHEMAS", "true").toBoolean(),
                createMissingTopics = env("CREATE_MISSING_TOPICS", "false").toBoolean(),
                topicPartitions = env("TOPIC_PARTITIONS", "3").toInt(),
                topicReplicationFactor = env("TOPIC_REPLICATION_FACTOR", "3").toShort(),
                topicInclude = Regex(env("TOPIC_INCLUDE", ".*")),
                topicExclude = System.getenv("TOPIC_EXCLUDE")?.takeIf { it.isNotBlank() }?.let { Regex(it) },
                replicaId = env("HOSTNAME", "local"),
                statsIntervalSeconds = env("STATS_INTERVAL_SECONDS", "30").toLong(),
            )
        }
    }
}
