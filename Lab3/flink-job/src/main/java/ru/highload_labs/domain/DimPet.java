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
@Table(name = "dim_pet")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DimPet implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "pet_type")
    private String petType;

    @Column(name = "pet_name")
    private String petName;

    @Column(name = "pet_breed")
    private String petBreed;

    @Column(name = "source_file")
    private String sourceFile;
}



