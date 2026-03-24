using Lab2Spark.ETL;
using Microsoft.Spark.Sql;

var spark = SparkSession
    .Builder()
    .AppName("SparkLab2")
    .Config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .GetOrCreate();

await spark.TransformToSnowflake();
