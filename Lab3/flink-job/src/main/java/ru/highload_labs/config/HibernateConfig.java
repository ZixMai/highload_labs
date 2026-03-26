package ru.highload_labs.config;

public record HibernateConfig(
        String driverClass,
        String hbm2ddlAuto,
        String dialect,
        boolean showSql,
        boolean formatSql,
        String currentSessionContextClass,
        String physicalNamingStrategy
) {
}

