using Microsoft.Spark.Sql;

namespace Lab2Spark.ETL;

public static class SparkUtils
{
    public static DataFrame ReadTable(this SparkSession spark, string table,
        string url = "jdbc:postgresql://postgres:5432/postgres", string user = "postgres", string pwd = "postgres") =>
        spark.Read()
            .Format("jdbc")
            .Option("url", url)
            .Option("dbtable", table)
            .Option("user", user)
            .Option("password", pwd)
            .Option("driver", "org.postgresql.Driver")
            .Load();

    public static void WriteTable(this DataFrame df, string table, SaveMode mode = SaveMode.Append,
        string url = "jdbc:postgresql://postgres:5432/postgres", string user = "postgres", string pwd = "postgres")
    {
        var writer = df.Write()
            .Format("jdbc")
            .Option("url", url)
            .Option("dbtable", table)
            .Option("user", user)
            .Option("password", pwd)
            .Option("driver", "org.postgresql.Driver");


        writer.Mode(mode).Save();
    }

    public static void WriteTableToClickhouse(
        this DataFrame df,
        string table,
        string orderBy,
        SaveMode mode = SaveMode.Overwrite,
        string url = "jdbc:clickhouse://clickhouse:8123/lab2?compress=0",
        string user = "admin",
        string pwd = "xekg_eh9xyJDJFHJKSDHKqy_xa?rd!en66")
    {
        var writer = df.Write()
            .Format("jdbc")
            .Option("url", url)
            .Option("dbtable", table)
            .Option("user", user)
            .Option("password", pwd)
            .Option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .Option("createTableOptions", $"ENGINE = MergeTree() ORDER BY ({orderBy})");

        writer.Mode(mode).Save();
    }
}