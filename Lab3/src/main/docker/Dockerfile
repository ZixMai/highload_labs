# ── Stage 1: Build ──────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-24 AS build
WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline -B --no-transfer-progress

COPY src ./src
RUN mvn package -DskipTests -B --no-transfer-progress

# ── Stage 2: Минимальный JRE через jlink ────────────────────────────────
FROM eclipse-temurin:24-jdk AS jre-build

COPY --from=build /app/target/Lab3-1.0-SNAPSHOT.jar /tmp/app.jar

RUN jdeps \
      --ignore-missing-deps \
      --print-module-deps \
      /tmp/app.jar > /tmp/modules.txt && \
    MODS=$(cat /tmp/modules.txt) && \
    jlink \
      --add-modules "${MODS:-java.base}" \
      --strip-debug \
      --no-man-pages \
      --no-header-files \
      --compress=zip-6 \
      --output /custom-jre

FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app
COPY --from=jre-build /custom-jre /opt/jre
COPY --from=build /app/target/Lab3-1.0-SNAPSHOT.jar app.jar

ENTRYPOINT ["/opt/jre/bin/java", "-jar", "app.jar"]
