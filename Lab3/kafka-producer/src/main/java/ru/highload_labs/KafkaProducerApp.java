package ru.highload_labs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

@Slf4j
public class KafkaProducerApp {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static long totalMessagesSent = 0;

    public static void main(String[] args) {
        String bootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = getEnvOrDefault("KAFKA_TOPIC", "csv-data");
        String csvPath = getEnvOrDefault("CSV_PATH", "lab_task_repo/BigDataFlink/исходные данные");

        Properties props = getProperties(bootstrapServers);

        log.info("=== Kafka CSV Producer Started ===");
        log.info("Bootstrap Servers: {}", bootstrapServers);
        log.info("Topic: {}", topic);
        log.info("CSV Path: {}", csvPath);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            processCSVFiles(producer, csvPath, topic);
            
            producer.flush();
            log.info("=== All CSV files processed. Total messages sent: {} ===", totalMessagesSent);
        } catch (Exception e) {
            log.error("Failed to process CSV", e);
            System.exit(1);
        }
    }

    private static Properties getProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);

        props.put("auto.create.topics.enable", "true");
        return props;
    }

    private static void processCSVFiles(KafkaProducer<String, String> producer, String csvPath, String topic) 
            throws Exception {
        Path dirPath = Paths.get(csvPath);
        
        if (!Files.exists(dirPath)) {
            log.error("Directory not found: {}", csvPath);
            return;
        }

        try (Stream<Path> paths = Files.list(dirPath)) {
            paths.filter(p -> p.toString().endsWith(".csv"))
                 .sorted()
                 .forEach(csvFile -> {
                     try {
                         processSingleCSV(producer, csvFile.toFile(), topic);
                     } catch (Exception e) {
                         log.error("Error processing file {}", csvFile, e);
                     }
                 });
        }
    }

    private static void processSingleCSV(KafkaProducer<String, String> producer, File csvFile, String topic) 
            throws Exception {
        String fileName = csvFile.getName();
        log.info("Processing file: {}", fileName);

        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> allLines = reader.readAll();
            
            if (allLines.isEmpty()) {
                log.warn("File is empty: {}", fileName);
                return;
            }

            String[] headers = allLines.getFirst();
            int fileLineCount = 0;

            for (int i = 1; i < allLines.size(); i++) {
                String[] values = allLines.get(i);
                
                Map<String, Object> messageData = new HashMap<>();
                messageData.put("_file", fileName);
                
                for (int j = 0; j < headers.length && j < values.length; j++) {
                    messageData.put(headers[j], values[j]);
                }

                String jsonValue = mapper.writeValueAsString(messageData);
                String key = fileName + "-" + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonValue);
                producer.send(record).get();

                fileLineCount++;
                totalMessagesSent++;

                if (fileLineCount % 100 == 0) {
                    log.debug("Sent {} rows from {}", fileLineCount, fileName);
                }
            }

            log.info("Completed: {} rows sent from {}", fileLineCount, fileName);
        }
    }

    private static String getEnvOrDefault(String envName, String defaultValue) {
        String value = System.getenv(envName);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}




