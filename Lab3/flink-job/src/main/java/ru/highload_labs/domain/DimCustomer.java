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
@Table(name = "dim_customer")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DimCustomer implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "customer_first_name")
    private String firstName;

    @Column(name = "customer_last_name")
    private String lastName;

    @Column(name = "customer_age")
    private Integer age;

    @Column(name = "customer_email")
    private String email;

    @Column(name = "customer_country")
    private String country;

    @Column(name = "customer_postal_code")
    private String postalCode;

    @Column(name = "customer_pet_id")
    private Long petId;

    @Column(name = "source_file")
    private String sourceFile;

    @Column(name = "source_id")
    private Long sourceId;
}


