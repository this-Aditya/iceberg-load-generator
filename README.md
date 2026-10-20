# iceberg-load-generator

Sends fake but well-formed RADAR data to Kafka, for every raw topic on RADAR-Schemas master, from
simulated participants in two projects. It exists to test the Iceberg lakehouse on stage with more
users, more topics and more projects than stage really has.

It talks to Kafka and the schema registry directly, like RADAR's own producers. From there the
data takes the normal path: the Iceberg sink writes it to tables, compaction tidies them, Trino
queries them.

## What it sends

**Topics.** Every topic in RADAR-Schemas `specifications/` (version `0.9.3`, the same commit as
master `ad05204`): 370 topics. The `stream/` folder is skipped, because RADAR's Kafka Streams app
writes those topics. A topic listed in several spec files is taken once.

**Schemas.** Each value schema comes from RADAR's published jar `radar-schemas-commons:0.9.3`.
It has one generated Java class per schema, for example `org.radarcns.passive.phone.PhoneAcceleration`.
The key is always `ObservationKey` (`projectId`, `userId`, `sourceId`).

**Users.** Each replica simulates `USERS_PER_REPLICA` users (default 50), shared out over
`PROJECTS` (default `LoadTest-A` and `LoadTest-B`, 25 users each). User ids are made from the pod
name, so two replicas never share a user. Each user has one `sourceId` per device spec file.

**Values.** Every field gets a random value of the right type. A few names get special
treatment so the data looks real:

| Field | Value |
|---|---|
| `time` | now, in seconds since 1970. Always filled, also where the schema allows null |
| other double fields with "time" in the name (`timeReceived`, ...) | a little after `time` |
| `dateTime`, `date` (text) | `time` written as `2026-10-20T14:03:11` or `2026-10-20`, in UTC |
| fields with "offset" in the name (`utcOffset`) | -3600, 0, 3600 or 7200 |
| any other optional field | empty (null) in about 1 of 10 records |

**Rate.** Each topic's spec says how often data comes:

- `frequency` (for example 32 Hz): that many records per second.
- `interval` (for example 86400 s for a daily summary): one record per interval.
- `dynamic`, or no sample rate (questionnaires and most connector topics): one record per
  `DEFAULT_INTERVAL_SECONDS` (default 300).

Then two limits apply, per user and per topic: the rate is multiplied by `RATE_FACTOR` (default 1)
and capped at `MAX_RATE_HZ` (default 1). The cap keeps a 51.2 Hz sensor from flooding Kafka.

**Even load.** Each replica splits its streams (users × topics) over `THREADS` threads. Every
thread wakes once a second and sends what its streams owe for that second, with event times
spread inside the second. Each stream starts at a random point, so a topic that sends every five
minutes does not send for all 50 users at the same moment.

## How much load

The numbers below are estimates from the spec files. At start-up the program logs its own figure:
`… about N records/s from this replica`.

| Setting | Per replica (50 users) | Per day |
|---|---|---|
| defaults (`MAX_RATE_HZ=1.0`, `DEFAULT_INTERVAL_SECONDS=300`) | about 1,200 records/s | about 100 million |
| `MAX_RATE_HZ=0.1`, `DEFAULT_INTERVAL_SECONDS=600` | about 140 records/s | about 12 million |

More replicas multiply it: `replicas: 4` with the defaults gives about 4,800 records/s.

**Watch the space this takes.** Everything stays in Kafka for its retention time (30 days on
stage), on every replica of the topic (3 on stage). The size per record has not been measured, so
check the brokers' free disk after the first hour and scale down if needed. Other readers of
these topics get the fake data too. If RADAR's S3 sink and radar-output-restructure run on stage
(not checked), they copy it into their bucket and write CSV files under the projects `LoadTest-A`
and `LoadTest-B`.

## Build and push

The image is built for `linux/amd64` (the stage nodes). The Gradle step runs natively, so this is
quick on an Apple Silicon Mac too.

```bash
cd ~/Partition/Projects/iceberg-load-generator
docker buildx build --platform linux/amd64 \
  -t docker.io/thisaditya7/iceberg-load-generator:0.1.0 --push .
```

Another RADAR-Schemas release: add `--build-arg RADAR_SCHEMAS_VERSION=0.9.4`. The schema jar and the
spec files then both come from that release.

To compile without Docker:

```bash
./gradlew installDist
```

## Run on stage

```bash
kubectl apply -f k8s/deployment.yaml
kubectl logs -f deploy/iceberg-load-generator
```

The first lines name the topics it skips because they do not exist on stage. Stage has automatic
topic creation switched off. To create them instead, set `CREATE_MISSING_TOPICS` to `"true"`: they
get 3 partitions and 3 replicas, like the other stage topics.

Every 30 seconds it logs how many records it sent, how many failed, and how many topics it
disabled. A topic is disabled when the schema registry refuses its schema (for example if stage
holds an older, incompatible version). The log names the topic and the reason, and the other
topics carry on.

**More load:** `kubectl scale deploy/iceberg-load-generator --replicas=4`.
**Stop:** `kubectl scale deploy/iceberg-load-generator --replicas=0`, or `kubectl delete -f k8s/deployment.yaml`.

User ids come from the pod name, and a Deployment gives a restarted pod a new name. So a restart
starts 50 new users. If stable users matter, change `kind: Deployment` to `kind: StatefulSet` and
add `serviceName: iceberg-load-generator`: its pods are named `-0`, `-1`, ... and keep their users.

## Where the data shows up

The Iceberg sink writes one namespace per project, so in Trino:

```sql
SHOW SCHEMAS FROM iceberg;                       -- loadtest_a, loadtest_b, ...
SHOW TABLES FROM iceberg.loadtest_a;
SELECT count(*), count(DISTINCT key_userid) FROM iceberg.loadtest_a.android_phone_acceleration;
```

## Settings

All settings are environment variables.

| Variable | Default | What it does |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `cp-kafka-headless:9092` | Kafka address |
| `SCHEMA_REGISTRY_URL` | `http://cp-schema-registry:8081` | schema registry address |
| `PROJECTS` | `LoadTest-A,LoadTest-B` | project ids, comma separated. Users are shared out over them |
| `USERS_PER_REPLICA` | `50` | users each replica simulates |
| `MAX_RATE_HZ` | `1.0` | highest rate of one user on one topic, in records per second |
| `RATE_FACTOR` | `1.0` | multiplies every topic's rate before the cap |
| `DEFAULT_INTERVAL_SECONDS` | `300` | how often a user sends on a topic with no fixed rate |
| `THREADS` | `4` | threads sending in parallel. All share one Kafka producer, which batches and compresses |
| `AUTO_REGISTER_SCHEMAS` | `true` | register a schema version the registry does not know yet |
| `CREATE_MISSING_TOPICS` | `false` | create topics that do not exist instead of skipping them |
| `TOPIC_PARTITIONS`, `TOPIC_REPLICATION_FACTOR` | `3`, `3` | used only when creating topics |
| `TOPIC_INCLUDE`, `TOPIC_EXCLUDE` | `.*`, none | regular expressions that narrow the topics, for example `TOPIC_INCLUDE=push_googlehealth_.*` |
| `STATS_INTERVAL_SECONDS` | `30` | how often the counts are logged |
| `JAVA_OPTS` | `-Xmx512m` in the manifest | JVM options |

## Code

| File | What it does |
|---|---|
| `Config.kt` | reads the environment variables |
| `Specifications.kt` | reads the spec files, finds each schema class, works out the rate |
| `RandomRecords.kt` | builds a random record for any Avro schema |
| `Generator.kt` | the users, the streams, the threads and the sending |
| `Main.kt` | checks which topics exist, sets up the producer, logs the counts, shuts down cleanly |
