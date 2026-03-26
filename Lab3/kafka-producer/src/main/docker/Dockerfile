# ── Stage 1: Build ──────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Сначала только pom-файлы — кэшируем зависимости отдельным слоем
COPY pom.xml .
COPY kafka-producer/pom.xml kafka-producer/
COPY flink-job/pom.xml flink-job/
RUN mvn dependency:go-offline -B --no-transfer-progress

# Теперь исходники
COPY kafka-producer/src kafka-producer/src
COPY flink-job/src flink-job/src

# Собираем только kafka-producer (-pl), -am подтягивает parent
RUN mvn package -pl kafka-producer -am -DskipTests -B --no-transfer-progress

FROM eclipse-temurin:21-jdk AS jre-build
COPY --from=build /app/kafka-producer/target/kafka-producer-1.0-SNAPSHOT.jar /tmp/app.jar
RUN jdeps --ignore-missing-deps --print-module-deps /tmp/app.jar > /tmp/mods.txt && \
    jlink \
      --add-modules "$(cat /tmp/mods.txt)" \
      --strip-debug --no-man-pages --no-header-files \
      --compress=zip-6 \
      --output /custom-jre

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=jre-build /custom-jre /opt/jre
COPY --from=build /app/kafka-producer/target/kafka-producer-1.0-SNAPSHOT.jar /app/app.jar
ENTRYPOINT ["/opt/jre/bin/java", "-jar", "/app/app.jar"]
