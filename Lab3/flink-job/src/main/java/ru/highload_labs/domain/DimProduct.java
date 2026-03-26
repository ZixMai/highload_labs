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
import java.math.BigDecimal;

@Entity
@Table(name = "dim_product")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DimProduct implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "product_name")
    private String productName;

    @Column(name = "product_category")
    private String productCategory;

    @Column(name = "product_price")
    private BigDecimal productPrice;

    @Column(name = "product_quantity")
    private Integer productQuantity;

    @Column(name = "product_weight")
    private BigDecimal productWeight;

    @Column(name = "product_color")
    private String productColor;

    @Column(name = "product_size")
    private String productSize;

    @Column(name = "product_brand")
    private String productBrand;

    @Column(name = "product_material")
    private String productMaterial;

    @Column(name = "product_description")
    private String productDescription;

    @Column(name = "product_rating")
    private BigDecimal productRating;

    @Column(name = "product_reviews")
    private Integer productReviews;

    @Column(name = "product_release_date")
    private String productReleaseDate;

    @Column(name = "product_expiry_date")
    private String productExpiryDate;

    @Column(name = "source_file")
    private String sourceFile;

    @Column(name = "source_id")
    private Long sourceId;
}



