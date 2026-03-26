package ru.highload_labs.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RawSaleEvent implements Serializable {
    private Long sourceRowId;
    private String sourceFile;

    private String saleDate;
    private Long saleCustomerSourceId;
    private Long saleSellerSourceId;
    private Long saleProductSourceId;
    private Integer saleQuantity;
    private BigDecimal saleTotalPrice;

    private String customerFirstName;
    private String customerLastName;
    private Integer customerAge;
    private String customerEmail;
    private String customerCountry;
    private String customerPostalCode;

    private String customerPetType;
    private String customerPetName;
    private String customerPetBreed;

    private String sellerFirstName;
    private String sellerLastName;
    private String sellerEmail;
    private String sellerCountry;
    private String sellerPostalCode;

    private String productName;
    private String productCategory;
    private BigDecimal productPrice;
    private Integer productQuantity;
    private BigDecimal productWeight;
    private String productColor;
    private String productSize;
    private String productBrand;
    private String productMaterial;
    private String productDescription;
    private BigDecimal productRating;
    private Integer productReviews;
    private String productReleaseDate;
    private String productExpiryDate;

    private String storeName;
    private String storeLocation;
    private String storeCity;
    private String storeState;
    private String storeCountry;
    private String storePhone;
    private String storeEmail;

    private String supplierName;
    private String supplierContact;
    private String supplierEmail;
    private String supplierPhone;
    private String supplierAddress;
    private String supplierCity;
    private String supplierCountry;

    private String petCategory;
}


