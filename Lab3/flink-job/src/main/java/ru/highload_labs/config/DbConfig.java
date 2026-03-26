package ru.highload_labs.config;

import java.io.Serializable;

public record DbConfig(
        String dbUrl,
        String dbUsername,
        String dbPassword
) implements Serializable {
}

