package ru.highload_labs.config;

import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import ru.highload_labs.domain.DimCategory;
import ru.highload_labs.domain.DimCustomer;
import ru.highload_labs.domain.DimPet;
import ru.highload_labs.domain.DimProduct;
import ru.highload_labs.domain.DimSeller;
import ru.highload_labs.domain.DimStore;
import ru.highload_labs.domain.DimSupplier;
import ru.highload_labs.domain.FactSale;

import java.util.HashMap;
import java.util.Map;

public final class HibernateSessionFactoryProvider {
    private HibernateSessionFactoryProvider() {
    }

    public static SessionFactory build(AppConfig appConfig) {
        Map<String, Object> settings = new HashMap<>();

        settings.put("hibernate.connection.driver_class", appConfig.hibernateConfig().driverClass());
        settings.put("hibernate.connection.url", appConfig.dbConfig().dbUrl());
        settings.put("hibernate.connection.username", appConfig.dbConfig().dbUsername());
        settings.put("hibernate.connection.password", appConfig.dbConfig().dbPassword());
        settings.put("hibernate.dialect", appConfig.hibernateConfig().dialect());
        settings.put("hibernate.show_sql", Boolean.toString(appConfig.hibernateConfig().showSql()));
        settings.put("hibernate.format_sql", Boolean.toString(appConfig.hibernateConfig().formatSql()));
        settings.put("hibernate.hbm2ddl.auto", appConfig.hibernateConfig().hbm2ddlAuto());
        settings.put("hibernate.auto_commit", "true");
        settings.put("hibernate.current_session_context_class", appConfig.hibernateConfig().currentSessionContextClass());
        settings.put("hibernate.physical_naming_strategy", appConfig.hibernateConfig().physicalNamingStrategy());

        StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
                .applySettings(settings)
                .build();

        try {
            return new MetadataSources(registry)
                    .addAnnotatedClass(FactSale.class)
                    .addAnnotatedClass(DimCustomer.class)
                    .addAnnotatedClass(DimSeller.class)
                    .addAnnotatedClass(DimProduct.class)
                    .addAnnotatedClass(DimSupplier.class)
                    .addAnnotatedClass(DimStore.class)
                    .addAnnotatedClass(DimCategory.class)
                    .addAnnotatedClass(DimPet.class)
                    .buildMetadata()
                    .buildSessionFactory();
        } catch (Exception e) {
            StandardServiceRegistryBuilder.destroy(registry);
            throw new RuntimeException("Failed to build Hibernate SessionFactory", e);
        }
    }
}

