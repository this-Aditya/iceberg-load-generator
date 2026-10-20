# One version for both the schema classes (Maven) and the specification files (Git tag), so they always match.
ARG RADAR_SCHEMAS_VERSION=0.9.3

# The specification files: which topics exist, their schema and their sample rate.
FROM --platform=$BUILDPLATFORM alpine:3.20 AS specifications
ARG RADAR_SCHEMAS_VERSION
RUN apk add --no-cache git \
 && git clone --depth 1 --branch "v${RADAR_SCHEMAS_VERSION}" https://github.com/RADAR-base/RADAR-Schemas.git /schemas

# The program. Java bytecode runs on any CPU, so this stage runs natively even when building for amd64 on a Mac.
FROM --platform=$BUILDPLATFORM gradle:8.14-jdk17 AS build
ARG RADAR_SCHEMAS_VERSION
WORKDIR /code
COPY settings.gradle.kts build.gradle.kts ./
COPY src ./src
RUN gradle installDist --no-daemon -PradarSchemasVersion="${RADAR_SCHEMAS_VERSION}"

FROM eclipse-temurin:17-jre
COPY --from=build /code/build/install/iceberg-load-generator /app
COPY --from=specifications /schemas/specifications /app/specifications
ENV SPECIFICATIONS_DIR=/app/specifications
ENTRYPOINT ["/app/bin/iceberg-load-generator"]
