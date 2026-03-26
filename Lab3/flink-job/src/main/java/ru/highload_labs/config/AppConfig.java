package ru.highload_labs.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public record AppConfig(
        DbConfig dbConfig,
        HibernateConfig hibernateConfig,
        int sinkBatchSize
) implements Serializable {
    public static AppConfig fromPropertiesFile(String resourceName) {
        Properties properties = new Properties();

        try (InputStream stream = AppConfig.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (stream == null) {
                throw new IllegalStateException("Cannot find resource: " + resourceName);
            }
            properties.load(stream);
        } catch (IOException e) {
            throw new RuntimeException("Cannot load properties from resource: " + resourceName, e);
        }

        DbConfig dbConfig = new DbConfig(
                require(properties, "app.db.url"),
                require(properties, "app.db.username"),
                require(properties, "app.db.password")
        );

        HibernateConfig hibernateConfig = new HibernateConfig(
                properties.getProperty("app.hibernate.driver-class", "org.postgresql.Driver"),
                properties.getProperty("app.hibernate.hbm2ddl-auto", "none"),
                properties.getProperty("app.hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect"),
                Boolean.parseBoolean(properties.getProperty("app.hibernate.show-sql", "false")),
                Boolean.parseBoolean(properties.getProperty("app.hibernate.format-sql", "false")),
                properties.getProperty("app.hibernate.current-session-context-class", "thread"),
                properties.getProperty("app.hibernate.physical-naming-strategy", "org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy")
        );

        int sinkBatchSize = Integer.parseInt(properties.getProperty("app.sink.batch-size", "100"));

        return new AppConfig(dbConfig, hibernateConfig, sinkBatchSize);
    }

    private static String require(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required property: " + key);
        }
        return value;
    }
}

