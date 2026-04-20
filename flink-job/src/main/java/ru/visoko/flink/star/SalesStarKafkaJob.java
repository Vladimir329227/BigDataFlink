package ru.visoko.flink.star;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Flink job: Kafka JSON (one CSV row) → PostgreSQL star schema. */
public final class SalesStarKafkaJob {

    public static void main(String[] args) throws Exception {
        String bootstrap = readEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = readEnv("KAFKA_TOPIC", "sales-raw");
        String group = readEnv("KAFKA_GROUP_ID", "flink-star-schema");
        String jdbcUrl = readEnv("JDBC_URL", "jdbc:postgresql://postgres:5432/bdsnowflake");
        String jdbcUser = readEnv("JDBC_USER", "postgres");
        String jdbcPassword = readEnv("JDBC_PASSWORD", "postgres");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(readEnv("FLINK_PARALLELISM", "1")));

        KafkaSource<String> source =
                KafkaSource.<String>builder()
                        .setBootstrapServers(bootstrap)
                        .setTopics(topic)
                        .setGroupId(group)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-sales-json")
                .addSink(new PostgresStarSink(jdbcUrl, jdbcUser, jdbcPassword));

        env.execute("sales-star-schema-stream");
    }

    private static String readEnv(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? def : v;
    }
}
