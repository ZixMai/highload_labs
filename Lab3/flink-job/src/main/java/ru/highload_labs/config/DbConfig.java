package ru.highload_labs.config;

public record DbConfig(
        String dbUrl,
        String dbUsername,
        String dbPassword
) {
}

