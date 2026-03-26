package ru.highload_labs.domain;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Entity
@Table(name = "fact_sale")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FactSale implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "sale_date")
    private String saleDate;

    @Column(name = "sale_customer_id")
    private Long saleCustomerId;

    @Column(name = "sale_product_id")
    private Long saleProductId;

    @Column(name = "sale_seller_id")
    private Long saleSellerId;

    @Column(name = "sale_quantity")
    private Integer saleQuantity;

    @Column(name = "sale_total_price")
    private BigDecimal saleTotalPrice;

    @Column(name = "sale_supplier_id")
    private Long saleSupplierId;

    @Column(name = "sale_store_id")
    private Long saleStoreId;

    @Column(name = "sale_category_id")
    private Long saleCategoryId;

    @Column(name = "source_file")
    private String sourceFile;
}


