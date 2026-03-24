using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace Lab2Spark.ETL;

public static class WriteReports
{
    public static void WriteReportsToClickhouse(this SparkSession spark)
    {
        var factSale = spark.ReadTable("fact_sale");
        var dimCustomer = spark.ReadTable("dim_customer");
        var dimProduct = spark.ReadTable("dim_product");
        var dimStore = spark.ReadTable("dim_store");
        var dimSupplier = spark.ReadTable("dim_supplier");
        var dimCategory = spark.ReadTable("dim_category");

        var salesByProduct = factSale.Alias("fs")
            .Join(dimProduct.Alias("dp"), Col("fs.product_id").EqualTo(Col("dp.id")), "left")
            .Join(dimCategory.Alias("dc"), Col("fs.category_id").EqualTo(Col("dc.id")), "left")
            .GroupBy(
                Col("fs.product_id"),
                Col("dp.product_name"),
                Col("dc.category"))
            .Agg(
                Sum(Col("fs.sale_total_price")).Alias("total_revenue"),
                Sum(Col("fs.sale_quantity")).Alias("total_sales_quantity"),
                Count(Lit(1)).Alias("sales_count"),
                Avg(Col("dp.product_rating")).Alias("avg_product_rating"),
                Max(Col("dp.product_reviews")).Alias("product_reviews"));

        var productTopWindow = Microsoft.Spark.Sql.Expressions.Window.OrderBy(Col("total_sales_quantity").Desc(), Col("total_revenue").Desc());
        salesByProduct = salesByProduct
            .WithColumn("product_rank_by_sales", RowNumber().Over(productTopWindow))
            .WithColumn("is_top_10_product", When(Col("product_rank_by_sales") <= 10, Lit(1)).Otherwise(Lit(0)));

        salesByProduct.WriteTableToClickhouse("rpt_sales_product", "product_id");

        var countryDistribution = dimCustomer
            .GroupBy(Col("customer_country"))
            .Agg(CountDistinct(Col("id")).Alias("customers_in_country"));

        var salesByCustomer = factSale.Alias("fs")
            .Join(dimCustomer.Alias("dc"), Col("fs.customer_id").EqualTo(Col("dc.id")), "left")
            .GroupBy(
                Col("fs.customer_id"),
                Col("dc.customer_first_name"),
                Col("dc.customer_last_name"),
                Col("dc.customer_country"))
            .Agg(
                Sum(Col("fs.sale_total_price")).Alias("total_purchase_amount"),
                Count(Lit(1)).Alias("orders_count"),
                Avg(Col("fs.sale_total_price")).Alias("avg_check"))
            .Join(countryDistribution.Alias("cd"), Col("dc.customer_country").EqualTo(Col("cd.customer_country")), "left")
            .Drop(Col("cd.customer_country"));

        var customerTopWindow = Microsoft.Spark.Sql.Expressions.Window.OrderBy(Col("total_purchase_amount").Desc());
        salesByCustomer = salesByCustomer
            .WithColumn("customer_rank_by_revenue", RowNumber().Over(customerTopWindow))
            .WithColumn("is_top_10_customer", When(Col("customer_rank_by_revenue") <= 10, Lit(1)).Otherwise(Lit(0)));

        salesByCustomer.WriteTableToClickhouse("rpt_sales_customer", "customer_id");

        var salesByTime = factSale
            .WithColumn("sale_dt", ToDate(Col("sale_date"), "M/d/yyyy"))
            .WithColumn("sale_year", Year(Col("sale_dt")))
            .WithColumn("sale_month", Month(Col("sale_dt")))
            .GroupBy(Col("sale_year"), Col("sale_month"))
            .Agg(
                Sum(Col("sale_total_price")).Alias("monthly_revenue"),
                Sum(Col("sale_quantity")).Alias("monthly_sales_quantity"),
                Count(Lit(1)).Alias("orders_count"),
                Avg(Col("sale_total_price")).Alias("avg_order_size"));

        var monthTrendWindow = Microsoft.Spark.Sql.Expressions.Window.OrderBy(Col("sale_year"), Col("sale_month"));
        salesByTime = salesByTime
            .WithColumn("prev_month_revenue", Lag(Col("monthly_revenue"), 1).Over(monthTrendWindow))
            .WithColumn("prev_year_same_month_revenue", Lag(Col("monthly_revenue"), 12).Over(monthTrendWindow))
            .WithColumn(
                "revenue_delta_vs_prev_month",
                When(Col("prev_month_revenue").IsNull(), Lit(null))
                    .Otherwise(Col("monthly_revenue") - Col("prev_month_revenue")))
            .WithColumn(
                "revenue_delta_vs_prev_year",
                When(Col("prev_year_same_month_revenue").IsNull(), Lit(null))
                    .Otherwise(Col("monthly_revenue") - Col("prev_year_same_month_revenue")))
            .WithColumn("prev_month_revenue", Coalesce(Col("prev_month_revenue"), Lit(0)))
            .WithColumn("prev_year_same_month_revenue", Coalesce(Col("prev_year_same_month_revenue"), Lit(0)))
            .WithColumn("revenue_delta_vs_prev_month", Coalesce(Col("revenue_delta_vs_prev_month"), Lit(0)))
            .WithColumn("revenue_delta_vs_prev_year", Coalesce(Col("revenue_delta_vs_prev_year"), Lit(0)));

        salesByTime.WriteTableToClickhouse("rpt_sales_time", "(sale_year, sale_month)");

        var salesByStore = factSale.Alias("fs")
            .Join(dimStore.Alias("ds"), Col("fs.store_id").EqualTo(Col("ds.id")), "left")
            .GroupBy(
                Col("fs.store_id"),
                Col("ds.store_name"),
                Col("ds.store_city"),
                Col("ds.store_country"))
            .Agg(
                Sum(Col("fs.sale_total_price")).Alias("total_revenue"),
                Count(Lit(1)).Alias("orders_count"),
                Avg(Col("fs.sale_total_price")).Alias("avg_check"));

        var storeTopWindow = Microsoft.Spark.Sql.Expressions.Window.OrderBy(Col("total_revenue").Desc());
        var storeCityWindow = Microsoft.Spark.Sql.Expressions.Window.PartitionBy(Col("store_city"));
        var storeCountryWindow = Microsoft.Spark.Sql.Expressions.Window.PartitionBy(Col("store_country"));
        salesByStore = salesByStore
            .WithColumn("store_rank_by_revenue", RowNumber().Over(storeTopWindow))
            .WithColumn("is_top_5_store", When(Col("store_rank_by_revenue") <= 5, Lit(1)).Otherwise(Lit(0)))
            .WithColumn("city_total_revenue", Sum(Col("total_revenue")).Over(storeCityWindow))
            .WithColumn("country_total_revenue", Sum(Col("total_revenue")).Over(storeCountryWindow));

        salesByStore.WriteTableToClickhouse("rpt_sales_store", "store_id");

        var salesBySupplier = factSale.Alias("fs")
            .Join(dimSupplier.Alias("dsu"), Col("fs.supplier_id").EqualTo(Col("dsu.id")), "left")
            .Join(dimProduct.Alias("dp"), Col("fs.product_id").EqualTo(Col("dp.id")), "left")
            .GroupBy(
                Col("fs.supplier_id"),
                Col("dsu.supplier_name"),
                Col("dsu.supplier_country"))
            .Agg(
                Sum(Col("fs.sale_total_price")).Alias("total_revenue"),
                Count(Lit(1)).Alias("orders_count"),
                Avg(Col("dp.product_price")).Alias("avg_product_price"));

        var supplierTopWindow = Microsoft.Spark.Sql.Expressions.Window.OrderBy(Col("total_revenue").Desc());
        var supplierCountryWindow = Microsoft.Spark.Sql.Expressions.Window.PartitionBy(Col("supplier_country"));
        salesBySupplier = salesBySupplier
            .WithColumn("supplier_rank_by_revenue", RowNumber().Over(supplierTopWindow))
            .WithColumn("is_top_5_supplier", When(Col("supplier_rank_by_revenue") <= 5, Lit(1)).Otherwise(Lit(0)))
            .WithColumn("country_total_revenue", Sum(Col("total_revenue")).Over(supplierCountryWindow));

        salesBySupplier.WriteTableToClickhouse("rpt_sales_supplier", "supplier_id");

        var qualityByProduct = factSale.Alias("fs")
            .Join(dimProduct.Alias("dp"), Col("fs.product_id").EqualTo(Col("dp.id")), "left")
            .GroupBy(Col("fs.product_id"), Col("dp.product_name"))
            .Agg(
                Sum(Col("fs.sale_quantity")).Alias("total_sales_quantity"),
                Sum(Col("fs.sale_total_price")).Alias("total_revenue"),
                Avg(Col("dp.product_rating")).Alias("avg_product_rating"),
                Max(Col("dp.product_reviews")).Alias("product_reviews"));

        var ratingDescWindow = Microsoft.Spark.Sql.Expressions.Window.OrderBy(Col("avg_product_rating").Desc(), Col("product_reviews").Desc());
        var ratingAscWindow = Microsoft.Spark.Sql.Expressions.Window.OrderBy(Col("avg_product_rating").Asc(), Col("product_reviews").Desc());
        var reviewWindow = Microsoft.Spark.Sql.Expressions.Window.OrderBy(Col("product_reviews").Desc());
        qualityByProduct = qualityByProduct
            .WithColumn("highest_rating_rank", RowNumber().Over(ratingDescWindow))
            .WithColumn("lowest_rating_rank", RowNumber().Over(ratingAscWindow))
            .WithColumn("reviews_rank", RowNumber().Over(reviewWindow))
            .WithColumn("is_highest_rated_product", When(Col("highest_rating_rank") == 1, Lit(1)).Otherwise(Lit(0)))
            .WithColumn("is_lowest_rated_product", When(Col("lowest_rating_rank") == 1, Lit(1)).Otherwise(Lit(0)))
            .WithColumn("is_most_reviewed_product", When(Col("reviews_rank") == 1, Lit(1)).Otherwise(Lit(0)));

        var qualityCorr = qualityByProduct
            .Agg(Corr(Col("avg_product_rating"), Col("total_sales_quantity")).Alias("rating_sales_correlation"));

        qualityByProduct = qualityByProduct.CrossJoin(qualityCorr);

        qualityByProduct.WriteTableToClickhouse("rpt_product_quality", "product_id");
    }
}