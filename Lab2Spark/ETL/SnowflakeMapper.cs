using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Lab2Spark.ETL;

public static class SnowflakeMapper
{
    public static async Task TransformToSnowflake(this SparkSession spark)
    {
        const string UnknownText = "__UNKNOWN__";

        var mockData = spark.ReadTable("mock_data")
            .WithColumn("source_row_uid", MonotonicallyIncreasingId());
        mockData.CreateOrReplaceTempView("mock_data");

        await PostgresSchema.DropForeignKeys();
        
        
        var dimCategory = mockData
            .Select(
                When(Col("pet_category").IsNull().Or(Trim(Col("pet_category")) == ""), Lit(UnknownText))
                    .Otherwise(Trim(Col("pet_category")))
                    .Alias("category"),
                Col("source_file").Alias("source_file"),
                Col("source_row_uid").Alias("source_row_uid")
            );

        dimCategory.WriteTable("dim_category", SaveMode.Overwrite);
        dimCategory.CreateOrReplaceTempView("dim_category");

        
        var dimPet = mockData
            .Select(
                When(mockData["customer_pet_type"].IsNull().Or(Trim(mockData["customer_pet_type"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["customer_pet_type"]))
                    .Alias("pet_type"),
                When(mockData["customer_pet_name"].IsNull().Or(Trim(mockData["customer_pet_name"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["customer_pet_name"]))
                    .Alias("pet_name"),
                When(mockData["customer_pet_breed"].IsNull().Or(Trim(mockData["customer_pet_breed"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["customer_pet_breed"]))
                    .Alias("pet_breed"),
                mockData["source_file"].Alias("source_file"),
                mockData["source_row_uid"].Alias("source_row_uid")
            );

        dimPet.WriteTable("dim_pet", SaveMode.Overwrite);
        dimPet.CreateOrReplaceTempView("dim_pet");
        
        await PostgresSchema.CreatePrimaryKeys(null, "dim_pet");
        var dimPetDbForCustomer = spark.ReadTable("dim_pet");

        var dimCustomer = mockData
            .Join(
                dimPetDbForCustomer,
                dimPetDbForCustomer["source_row_uid"].EqualTo(mockData["source_row_uid"]),
                "left")
            .Select(
                Trim(mockData["customer_first_name"]).Alias("customer_first_name"),
                Trim(mockData["customer_last_name"]).Alias("customer_last_name"),
                mockData["customer_age"].Alias("customer_age"),
                Trim(mockData["customer_email"]).Alias("customer_email"),
                Trim(mockData["customer_country"]).Alias("customer_country"),
                Trim(mockData["customer_postal_code"]).Alias("customer_postal_code"),
                dimPetDbForCustomer["id"].Alias("pet_id"),
                mockData["source_file"].Alias("source_file"),
                mockData["sale_customer_id"].Alias("source_id"),
                mockData["source_row_uid"].Alias("source_row_uid")
            );

        dimCustomer.WriteTable("dim_customer", SaveMode.Overwrite);
        dimCustomer.CreateOrReplaceTempView("dim_customer");

        
        var dimProduct = mockData
            .Select(
                Trim(mockData["product_name"]).Alias("product_name"),
                Trim(mockData["product_category"]).Alias("product_category"),
                mockData["product_price"].Alias("product_price"),
                mockData["product_quantity"].Alias("product_quantity"),
                mockData["product_weight"].Alias("product_weight"),
                Trim(mockData["product_color"]).Alias("product_color"),
                Trim(mockData["product_size"]).Alias("product_size"),
                Trim(mockData["product_brand"]).Alias("product_brand"),
                Trim(mockData["product_material"]).Alias("product_material"),
                Trim(mockData["product_description"]).Alias("product_description"),
                mockData["product_rating"].Alias("product_rating"),
                mockData["product_reviews"].Alias("product_reviews"),
                When(
                        mockData["product_release_date"].RLike("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$"),
                        ToDate(mockData["product_release_date"], "M/d/yyyy"))
                    .Otherwise(Lit(null))
                    .Alias("product_release_date"),
                When(
                        mockData["product_expiry_date"].RLike("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$"),
                        ToDate(mockData["product_expiry_date"], "M/d/yyyy"))
                    .Otherwise(Lit(null))
                    .Alias("product_expiry_date"),
                mockData["source_file"].Alias("source_file"),
                mockData["sale_product_id"].Alias("source_id"),
                mockData["source_row_uid"].Alias("source_row_uid")
            );

        dimProduct.WriteTable("dim_product", SaveMode.Overwrite);
        dimProduct.CreateOrReplaceTempView("dim_product");

        
        var dimSeller = mockData
            .Select(
                Trim(mockData["seller_first_name"]).Alias("seller_first_name"),
                Trim(mockData["seller_last_name"]).Alias("seller_last_name"),
                Trim(mockData["seller_email"]).Alias("seller_email"),
                Trim(mockData["seller_country"]).Alias("seller_country"),
                Trim(mockData["seller_postal_code"]).Alias("seller_postal_code"),
                mockData["source_file"].Alias("source_file"),
                mockData["sale_seller_id"].Alias("source_id"),
                mockData["source_row_uid"].Alias("source_row_uid")
            );

        dimSeller.WriteTable("dim_seller", SaveMode.Overwrite);
        dimSeller.CreateOrReplaceTempView("dim_seller");

        
        var dimStore = mockData
            .Select(
                When(mockData["store_name"].IsNull().Or(Trim(mockData["store_name"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["store_name"]))
                    .Alias("store_name"),
                When(mockData["store_location"].IsNull().Or(Trim(mockData["store_location"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["store_location"]))
                    .Alias("store_location"),
                When(mockData["store_city"].IsNull().Or(Trim(mockData["store_city"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["store_city"]))
                    .Alias("store_city"),
                When(mockData["store_state"].IsNull().Or(Trim(mockData["store_state"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["store_state"]))
                    .Alias("store_state"),
                When(mockData["store_country"].IsNull().Or(Trim(mockData["store_country"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["store_country"]))
                    .Alias("store_country"),
                When(mockData["store_phone"].IsNull().Or(Trim(mockData["store_phone"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["store_phone"]))
                    .Alias("store_phone"),
                When(mockData["store_email"].IsNull().Or(Trim(mockData["store_email"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["store_email"]))
                    .Alias("store_email"),
                mockData["source_file"].Alias("source_file"),
                mockData["source_row_uid"].Alias("source_row_uid")
            );

        dimStore.WriteTable("dim_store", SaveMode.Overwrite);
        dimStore.CreateOrReplaceTempView("dim_store");

        
        var dimSupplier = mockData
            .Select(
                When(mockData["supplier_name"].IsNull().Or(Trim(mockData["supplier_name"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["supplier_name"]))
                    .Alias("supplier_name"),
                When(mockData["supplier_contact"].IsNull().Or(Trim(mockData["supplier_contact"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["supplier_contact"]))
                    .Alias("supplier_contact"),
                When(mockData["supplier_email"].IsNull().Or(Trim(mockData["supplier_email"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["supplier_email"]))
                    .Alias("supplier_email"),
                When(mockData["supplier_phone"].IsNull().Or(Trim(mockData["supplier_phone"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["supplier_phone"]))
                    .Alias("supplier_phone"),
                When(mockData["supplier_address"].IsNull().Or(Trim(mockData["supplier_address"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["supplier_address"]))
                    .Alias("supplier_address"),
                When(mockData["supplier_city"].IsNull().Or(Trim(mockData["supplier_city"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["supplier_city"]))
                    .Alias("supplier_city"),
                When(mockData["supplier_country"].IsNull().Or(Trim(mockData["supplier_country"]) == ""), Lit(UnknownText))
                    .Otherwise(Trim(mockData["supplier_country"]))
                    .Alias("supplier_country"),
                mockData["source_file"].Alias("source_file"),
                mockData["source_row_uid"].Alias("source_row_uid")
            );

        dimSupplier.WriteTable("dim_supplier", SaveMode.Overwrite);
        dimSupplier.CreateOrReplaceTempView("dim_supplier");

        await PostgresSchema.CreatePrimaryKeys(
            null,
            "dim_category",
            "dim_pet",
            "dim_customer",
            "dim_seller",
            "dim_product",
            "dim_store",
            "dim_supplier"
        );

        
        var dimCategoryDb = spark.ReadTable("dim_category");
        var dimCustomerDb = spark.ReadTable("dim_customer");
        var dimSellerDb   = spark.ReadTable("dim_seller");
        var dimProductDb  = spark.ReadTable("dim_product");
        var dimStoreDb    = spark.ReadTable("dim_store");
        var dimSupplierDb = spark.ReadTable("dim_supplier");

        dimCategoryDb.CreateOrReplaceTempView("dim_category_db");
        dimCustomerDb.CreateOrReplaceTempView("dim_customer_db");
        dimSellerDb.CreateOrReplaceTempView("dim_seller_db");
        dimProductDb.CreateOrReplaceTempView("dim_product_db");
        dimStoreDb.CreateOrReplaceTempView("dim_store_db");
        dimSupplierDb.CreateOrReplaceTempView("dim_supplier_db");

        
        var factSale = mockData
            .Join(
                dimCategoryDb,
                dimCategoryDb["source_row_uid"].EqualTo(mockData["source_row_uid"]),
                "left")
            .Join(
                dimCustomerDb,
                dimCustomerDb["source_row_uid"].EqualTo(mockData["source_row_uid"]),
                "left")
            .Join(
                dimSellerDb,
                dimSellerDb["source_row_uid"].EqualTo(mockData["source_row_uid"]),
                "left")
            .Join(
                dimProductDb,
                dimProductDb["source_row_uid"].EqualTo(mockData["source_row_uid"]),
                "left")
            .Join(
                dimStoreDb,
                dimStoreDb["source_row_uid"].EqualTo(mockData["source_row_uid"]),
                "left")
            .Join(
                dimSupplierDb,
                dimSupplierDb["source_row_uid"].EqualTo(mockData["source_row_uid"]),
                "left")
            .Select(
                mockData["sale_date"].Alias("sale_date"),
                dimCustomerDb["id"].Alias("customer_id"),
                dimSellerDb["id"].Alias("seller_id"),
                dimProductDb["id"].Alias("product_id"),
                dimStoreDb["id"].Alias("store_id"),
                dimSupplierDb["id"].Alias("supplier_id"),
                dimCategoryDb["id"].Alias("category_id"),
                mockData["sale_quantity"].Alias("sale_quantity"),
                mockData["sale_total_price"].Alias("sale_total_price")
            );

        factSale.WriteTable("fact_sale", SaveMode.Overwrite);

        await PostgresSchema.CreatePrimaryKeys(null, "fact_sale");

        await PostgresSchema.CreateForeignKeys();
    }
}
