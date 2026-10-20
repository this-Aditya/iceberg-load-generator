package org.radarbase.loadgen

import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.SerializationException
import org.radarcns.kafka.ObservationKey
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.floor
import kotlin.random.Random

/** One user sending one topic at a steady rate. `due` collects the fraction of a record owed so far. */
class Stream(val spec: TopicSpec, val key: GenericRecord, var due: Double)

/**
 * Simulates USERS_PER_REPLICA users, spread over PROJECTS, each sending every topic. User ids are derived from
 * the pod name, so two replicas never share a user. The streams are split over THREADS threads, and every
 * thread wakes once a second and sends what its streams owe for that second. Each stream starts with a random
 * part of a record already owed, so low-rate topics are spread evenly over time instead of arriving together.
 */
class Generator(
    private val config: Config,
    specs: List<TopicSpec>,
    private val producer: KafkaProducer<GenericRecord, GenericRecord>,
) {
    private val logger = LoggerFactory.getLogger(Generator::class.java)
    private val streams: List<Stream>
    private val disabledTopics = ConcurrentHashMap.newKeySet<String>()
    private val sent = AtomicLong()
    private val failed = AtomicLong()

    @Volatile
    private var running = true

    init {
        val random = Random(config.replicaId.hashCode())
        val keySchema = ObservationKey.getClassSchema()
        streams = (0 until config.usersPerReplica).flatMap { i ->
            val project = config.projects[i % config.projects.size]
            val userId = uuid("${config.replicaId}/user-$i")
            specs.map { spec ->
                val key = GenericData.Record(keySchema).apply {
                    put("projectId", project)
                    put("userId", userId)
                    put("sourceId", uuid("$userId/${spec.sourceGroup}"))
                }
                Stream(spec, key, random.nextDouble())
            }
        }
    }

    /** Records per second this replica will send, before any topic is disabled. */
    val plannedRatePerSecond: Double get() = streams.sumOf { it.spec.ratePerSecond }

    fun start(): List<Thread> = streams.indices
        .groupBy { it % config.threads }
        .map { (n, indices) ->
            val mine = indices.map { streams[it] }
            Thread({ run(mine, Random(config.replicaId.hashCode() * 31 + n)) }, "generator-$n").apply { start() }
        }

    fun stop() {
        running = false
    }

    fun logStats(seconds: Long, last: LongArray) {
        val nowSent = sent.get()
        logger.info(
            "sent {} records ({}/s over the last {}s), {} failed, {} topic(s) disabled",
            nowSent, (nowSent - last[0]) / seconds, seconds, failed.get(), disabledTopics.size,
        )
        last[0] = nowSent
    }

    private fun run(mine: List<Stream>, random: Random) {
        val records = RandomRecords(random)
        var last = System.nanoTime()
        while (running) {
            val now = System.nanoTime()
            val elapsed = (now - last) / 1e9
            last = now
            val tickStart = System.currentTimeMillis() / 1000.0 - elapsed
            for (stream in mine) {
                if (stream.spec.topic in disabledTopics) continue
                stream.due += stream.spec.ratePerSecond * elapsed
                // After a long pause, send at most two seconds' worth instead of a burst.
                val count = minOf(floor(stream.due).toInt(), (stream.spec.ratePerSecond * 2).toInt() + 1)
                stream.due -= floor(stream.due)
                repeat(count) { k ->
                    val time = tickStart + elapsed * (k + random.nextDouble()) / count
                    send(stream, records.record(stream.spec.valueSchema, time))
                }
            }
            val sleep = 1000 - (System.nanoTime() - now) / 1_000_000
            if (sleep > 0) Thread.sleep(sleep)
        }
    }

    private fun send(stream: Stream, value: GenericRecord) {
        val topic = stream.spec.topic
        try {
            producer.send(ProducerRecord(topic, stream.key, value)) { _, ex ->
                if (ex == null) sent.incrementAndGet() else failed.incrementAndGet()
            }
        } catch (ex: SerializationException) {
            // Usually the schema registry refusing this schema for the topic. Stop this topic, keep the rest.
            if (disabledTopics.add(topic)) {
                logger.error("Disabling {}: {}", topic, generateSequence<Throwable>(ex) { it.cause }.last().message)
            }
        }
    }

    private fun uuid(name: String): String = UUID.nameUUIDFromBytes(name.toByteArray()).toString()
}
