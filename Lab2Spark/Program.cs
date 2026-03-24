using Lab2Spark.ETL;
using Microsoft.Spark.Sql;

var spark = SparkSession
    .Builder()
    .AppName("SparkLab2")
    .GetOrCreate();

await spark.TransformToSnowflake();
