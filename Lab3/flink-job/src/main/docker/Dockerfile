FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml .
COPY kafka-producer/pom.xml kafka-producer/
COPY flink-job/pom.xml flink-job/
RUN mvn dependency:go-offline -B --no-transfer-progress

COPY kafka-producer/src kafka-producer/src
COPY flink-job/src flink-job/src

RUN mvn package -pl flink-job -am -DskipTests -B --no-transfer-progress

FROM flink:2.2.0-java21
COPY --from=build /app/flink-job/target/flink-job-1.0-SNAPSHOT.jar \
     /opt/flink/usrlib/flink-job.jar
