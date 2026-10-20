plugins {
    kotlin("jvm") version "2.1.20"
    application
}

group = "org.radarbase"
version = "0.1.0"

// The schema classes and the specification files must come from the same RADAR-Schemas release.
// The Dockerfile passes the same version to both.
val radarSchemasVersion: String = (findProperty("radarSchemasVersion") as String?) ?: "0.9.3"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    // Generated Avro classes for every RADAR schema. Each has getClassSchema() with all types resolved.
    implementation("org.radarbase:radar-schemas-commons:$radarSchemasVersion")
    implementation("io.confluent:kafka-avro-serializer:7.8.0")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.18.2")
    implementation("org.slf4j:slf4j-simple:2.0.16")
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("org.radarbase.loadgen.MainKt")
}
