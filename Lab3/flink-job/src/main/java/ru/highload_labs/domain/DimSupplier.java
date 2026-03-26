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
@Table(name = "dim_supplier")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DimSupplier implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "supplier_name")
    private String supplierName;

    @Column(name = "supplier_contact")
    private String supplierContact;

    @Column(name = "supplier_email")
    private String supplierEmail;

    @Column(name = "supplier_phone")
    private String supplierPhone;

    @Column(name = "supplier_address")
    private String supplierAddress;

    @Column(name = "supplier_city")
    private String supplierCity;

    @Column(name = "supplier_country")
    private String supplierCountry;

    @Column(name = "source_file")
    private String sourceFile;

    @Column(name = "source_id")
    private Long sourceId;
}



