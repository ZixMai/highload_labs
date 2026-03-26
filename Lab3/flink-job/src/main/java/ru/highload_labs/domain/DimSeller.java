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
@Table(name = "dim_seller")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DimSeller implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "seller_first_name")
    private String firstName;

    @Column(name = "seller_last_name")
    private String lastName;

    @Column(name = "seller_email")
    private String email;

    @Column(name = "seller_country")
    private String country;

    @Column(name = "seller_postal_code")
    private String postalCode;

    @Column(name = "source_file")
    private String sourceFile;

    @Column(name = "source_id")
    private Long sourceId;
}



