import argparse

from pyspark.sql import SparkSession

import config


def get_azure_spark_connection(storage_account_name, storage_account_key):
    spark = (
        SparkSession.builder
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-azure:2.7.3')
            .config('spark.hadoop.fs.azure', "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            .config("spark.hadoop.fs.azure.account.key." + storage_account_name + ".blob.core.windows.net",
                    storage_account_key)
            .appName("AzureSparkDemo")
            .getOrCreate())

    (spark.sparkContext._jsc.hadoopConfiguration().set("fs.wasbs.impl",
                                                       "org.apache.hadoop.fs.azure.NativeAzureFileSystem"))
    return spark


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="input file to parse", type=str,
                        default="wasbs://demo@datamindeddata.blob.core.windows.net/raw/airlines/2008.csv.bz2")
    parser.add_argument("-o", "--output", help="result file to write", type=str,
                        default="wasbs://demo@datamindeddata.blob.core.windows.net/aggregated/airlines/2008.parquet")
    args = parser.parse_args()
    spark = get_azure_spark_connection(config.STORAGE_ACCOUNT_NAME, config.STORAGE_ACCOUNT_KEY)
    df = (spark.read.option("header", "true")
          .option("delimiter", ",")
          .option("inferSchema", "true")
          .csv(args.input))
    df.registerTempTable("airlines")
    result = spark.sql("""
      select Year, Month, DayofMonth, avg(ArrDelay) as avg_ArrDelay, avg(DepDelay) as avg_DepDelay
      from airlines 
      group by Year, Month, DayofMonth
""")
    result.repartition(1).write.mode("overwrite").parquet(args.output)
