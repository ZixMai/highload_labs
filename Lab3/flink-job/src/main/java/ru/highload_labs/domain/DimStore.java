package ru.highload_labs.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Entity
@Table(name = "dim_store")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DimStore implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "store_name")
    private String storeName;

    @Column(name = "store_location")
    private String storeLocation;

    @Column(name = "store_city")
    private String storeCity;

    @Column(name = "store_state")
    private String storeState;

    @Column(name = "store_country")
    private String storeCountry;

    @Column(name = "store_phone")
    private String storePhone;

    @Column(name = "store_email")
    private String storeEmail;

    @Column(name = "source_file")
    private String sourceFile;

    @Column(name = "source_id")
    private Long sourceId;
}



