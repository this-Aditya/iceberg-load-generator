package org.radarbase.loadgen

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

private val logger = LoggerFactory.getLogger("org.radarbase.loadgen.Main")

fun main() {
    val config = Config.fromEnv()
    val specs = keepExistingTopics(config, Specifications.load(config))

    val producer = KafkaProducer<GenericRecord, GenericRecord>(producerProperties(config))
    val generator = Generator(config, specs, producer)
    logger.info(
        "{} topics x {} users in projects {}: about {} records/s from this replica",
        specs.size, config.usersPerReplica, config.projects, "%.1f".format(generator.plannedRatePerSecond),
    )

    val threads = generator.start()
    val stats = Executors.newSingleThreadScheduledExecutor()
    val last = LongArray(1)
    stats.scheduleAtFixedRate(
        { generator.logStats(config.statsIntervalSeconds, last) },
        config.statsIntervalSeconds, config.statsIntervalSeconds, TimeUnit.SECONDS,
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Stopping")
        generator.stop()
        threads.forEach { it.join(5_000) }
        stats.shutdown()
        producer.close(java.time.Duration.ofSeconds(10))
    })
    threads.forEach { it.join() }
}

/** Kafka refuses records for a topic that does not exist (stage has automatic topic creation off). */
private fun keepExistingTopics(config: Config, specs: List<TopicSpec>): List<TopicSpec> {
    Admin.create(mapOf<String, Any>("bootstrap.servers" to config.bootstrapServers)).use { admin ->
        val existing = admin.listTopics().names().get(30, TimeUnit.SECONDS)
        val missing = specs.map { it.topic }.filterNot { it in existing }
        if (missing.isEmpty()) return specs
        if (config.createMissingTopics) {
            logger.info("Creating {} missing topic(s): {}", missing.size, missing)
            admin.createTopics(missing.map { NewTopic(it, config.topicPartitions, config.topicReplicationFactor) })
                .all().get(60, TimeUnit.SECONDS)
            return specs
        }
        logger.warn("Skipping {} topic(s) that do not exist (set CREATE_MISSING_TOPICS=true to create them): {}", missing.size, missing)
        return specs.filter { it.topic in existing }
    }
}

private fun producerProperties(config: Config) = Properties().apply {
    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
    put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
    put("schema.registry.url", config.schemaRegistryUrl)
    put("auto.register.schemas", config.autoRegisterSchemas.toString())
    // Batch records for up to 100 ms and compress them: few large requests instead of many small ones.
    put(ProducerConfig.LINGER_MS_CONFIG, "100")
    put(ProducerConfig.BATCH_SIZE_CONFIG, "262144")
    put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    put(ProducerConfig.ACKS_CONFIG, "all")
    put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000")
}
