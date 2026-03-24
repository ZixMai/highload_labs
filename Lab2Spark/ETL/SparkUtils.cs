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
}