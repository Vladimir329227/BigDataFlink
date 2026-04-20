FROM maven:3.9.9-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:1.18.1-java11
USER root
COPY --from=build /build/target/flink-star-job-1.0.0.jar /opt/job/flink-star-job.jar
USER flink
WORKDIR /opt/flink
