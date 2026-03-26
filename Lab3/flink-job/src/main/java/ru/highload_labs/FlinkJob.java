package ru.highload_labs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import ru.highload_labs.config.AppConfig;
import ru.highload_labs.domain.RawSaleEvent;

import java.math.BigDecimal;
import java.util.Optional;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class FlinkJob {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String kafkaTopic = getEnvOrDefault("KAFKA_TOPIC", "csv-data");
        String consumerGroup = getEnvOrDefault("KAFKA_CONSUMER_GROUP", "flink-job-group");
        AppConfig appConfig = AppConfig.fromPropertiesFile("application.properties");

        log.info("=== Flink Job Started ===");
        log.info("Kafka Bootstrap Servers: {}", kafkaBootstrapServers);
        log.info("Kafka Topic: {}", kafkaTopic);
        log.info("Postgres URL: {}", appConfig.dbConfig().dbUrl());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(kafkaTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        StarSchemaHibernateSink starSchemaSink = new StarSchemaHibernateSink(appConfig);

        env.fromSource(
                        kafkaSource,
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "Kafka Source"
                )
                .map(message -> {
                    try {
                        JsonNode record = mapper.readTree(message);
                        return RawSaleEvent.builder()
                                .sourceRowId(parseLong(text(record, "id")))
                                .sourceFile(text(record, "_file"))
                                .saleDate(text(record, "sale_date"))
                                .saleCustomerSourceId(parseLong(text(record, "sale_customer_id")))
                                .saleSellerSourceId(parseLong(text(record, "sale_seller_id")))
                                .saleProductSourceId(parseLong(text(record, "sale_product_id")))
                                .saleQuantity(parseInt(text(record, "sale_quantity")))
                                .saleTotalPrice(parseDecimal(text(record, "sale_total_price")))
                                .customerFirstName(text(record, "customer_first_name"))
                                .customerLastName(text(record, "customer_last_name"))
                                .customerAge(parseInt(text(record, "customer_age")))
                                .customerEmail(text(record, "customer_email"))
                                .customerCountry(text(record, "customer_country"))
                                .customerPostalCode(text(record, "customer_postal_code"))
                                .customerPetType(text(record, "customer_pet_type"))
                                .customerPetName(text(record, "customer_pet_name"))
                                .customerPetBreed(text(record, "customer_pet_breed"))
                                .sellerFirstName(text(record, "seller_first_name"))
                                .sellerLastName(text(record, "seller_last_name"))
                                .sellerEmail(text(record, "seller_email"))
                                .sellerCountry(text(record, "seller_country"))
                                .sellerPostalCode(text(record, "seller_postal_code"))
                                .productName(text(record, "product_name"))
                                .productCategory(text(record, "product_category"))
                                .productPrice(parseDecimal(text(record, "product_price")))
                                .productQuantity(parseInt(text(record, "product_quantity")))
                                .productWeight(parseDecimal(text(record, "product_weight")))
                                .productColor(text(record, "product_color"))
                                .productSize(text(record, "product_size"))
                                .productBrand(text(record, "product_brand"))
                                .productMaterial(text(record, "product_material"))
                                .productDescription(text(record, "product_description"))
                                .productRating(parseDecimal(text(record, "product_rating")))
                                .productReviews(parseInt(text(record, "product_reviews")))
                                .productReleaseDate(text(record, "product_release_date"))
                                .productExpiryDate(text(record, "product_expiry_date"))
                                .storeName(text(record, "store_name"))
                                .storeLocation(text(record, "store_location"))
                                .storeCity(text(record, "store_city"))
                                .storeState(text(record, "store_state"))
                                .storeCountry(text(record, "store_country"))
                                .storePhone(text(record, "store_phone"))
                                .storeEmail(text(record, "store_email"))
                                .supplierName(text(record, "supplier_name"))
                                .supplierContact(text(record, "supplier_contact"))
                                .supplierEmail(text(record, "supplier_email"))
                                .supplierPhone(text(record, "supplier_phone"))
                                .supplierAddress(text(record, "supplier_address"))
                                .supplierCity(text(record, "supplier_city"))
                                .supplierCountry(text(record, "supplier_country"))
                                .petCategory(text(record, "pet_category"))
                                .build();
                    } catch (Exception e) {
                        log.error("Error parsing JSON: {}", e.getMessage());
                        throw new RuntimeException(e);
                    }
                })
                .name("Parse JSON to RawSaleEvent")
                .returns(RawSaleEvent.class)
                .sinkTo(starSchemaSink)
                .name("PostgreSQL Star Schema Sink");

        log.info("Starting Flink job execution...");
        env.execute("Kafka CSV Data Processor to PostgreSQL");
    }

    private static String getEnvOrDefault(String envName, String defaultValue) {
        String value = System.getenv(envName);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static String text(JsonNode record, String fieldName) {
        return Optional.ofNullable(record.get(fieldName))
                .filter(node -> !node.isNull())
                .map(JsonNode::asText)
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .orElse(null);
    }

    private static Integer parseInt(String value) {
        return Optional.ofNullable(value)
                .map(String::trim)
                .filter(v -> !v.isEmpty())
                .flatMap(FlinkJob::tryParseInt)
                .orElse(null);
    }

    private static BigDecimal parseDecimal(String value) {
        return Optional.ofNullable(value)
                .map(String::trim)
                .filter(v -> !v.isEmpty())
                .flatMap(FlinkJob::tryParseDecimal)
                .orElse(null);
    }

    private static Long parseLong(String value) {
        return Optional.ofNullable(value)
                .map(String::trim)
                .filter(v -> !v.isEmpty())
                .flatMap(FlinkJob::tryParseLong)
                .orElse(null);
    }

    private static Optional<Integer> tryParseInt(String value) {
        try {
            return Optional.of(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static Optional<BigDecimal> tryParseDecimal(String value) {
        try {
            return Optional.of(new BigDecimal(value));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static Optional<Long> tryParseLong(String value) {
        try {
            return Optional.of(Long.parseLong(value));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
