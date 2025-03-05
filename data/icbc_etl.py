import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id


def main(spark):
    path = 'raw/ICBC_BC_full.csv'

    df_schema = types.StructType([
    StructField("Crash Breakdown 2", StringType(), True),
    StructField("Date Of Loss Year", IntegerType(), True),
    StructField("Animal Flag", StringType(), True),
    StructField("Crash Severity", StringType(), True),
    StructField("Cyclist Flag", StringType(), True),
    StructField("Day Of Week", StringType(), True),
    StructField("Derived Crash Configuration", StringType(), True),
    StructField("Heavy Vehicle Flag", StringType(), True),
    StructField("Intersection Crash", StringType(), True),
    StructField("Month Of Year", StringType(), True),
    StructField("Motorcycle Flag", StringType(), True),
    StructField("Municipality Name (ifnull)", StringType(), True),
    StructField("Parked Vehicle Flag", StringType(), True),
    StructField("Parking Lot Flag", StringType(), True),
    StructField("Pedestrian Flag", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Street Full Name (ifnull)", StringType(), True),
    StructField("Time Category", StringType(), True),
    StructField("Municipality Name", StringType(), True),
    StructField("Road Location Description", StringType(), True),
    StructField("Street Full Name", StringType(), True),
    StructField("Metric Selector", IntegerType(), True),
    StructField("Total Crashes", IntegerType(), True),
    StructField("Total Victims", IntegerType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Cross Street Full Name", StringType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Mid Block Crash", StringType(), True),
    StructField("Municipality With Boundary", StringType(), True)
])
    
    
    icbc_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("encoding", "UTF-16") \
                        .csv(path, schema=df_schema) \
                        .repartition(120)
    
    #icbc_df_parquet = spark.read.schema(df_schema).parquet('parquet/icbc')
    #icbc_df_parquet.show()
                        

    #icbc_df.show(1)
    
    # Checking for duplicate columns
    icbc_df = icbc_df.withColumn('is_duplicate', 
                                 F.when(F.col('Street Full Name') == F.col('Street Full Name (ifnull)'), F.lit(True))
                                 .otherwise(F.lit(False)))
    
    diff_rows = icbc_df.filter(F.col('is_duplicate') == False).select('Street Full Name', 'Street Full Name (ifnull)')
    
    
    # Drop unnecessary columns
    icbc_df = icbc_df.drop('Crash Breakdown 2', 'Municipality With Boundary', 'Animal Flag', 'Cyclist Flag', 'Heavy Vehicle Flag', 'Motorcycle Flag', \
        'Parked Vehicle Flag', 'Parking Lot Flag', 'Pedestrian Flag', 'Metric Selector', 'Municipality Name (ifnull)', 'Cross Street Full Name', 'Street Full Name')
    

    #icbc_df.select('Crash Severity', 'Street Full Name', 'Month Of Year').show(truncate=False)
    
    # Generate unique ID as key column
    icbc_df = icbc_df.withColumn('id', monotonically_increasing_id())
    
    # TODO check nulls
    icbc_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in icbc_df.columns]).show()
    
    total_rows = icbc_df.count()
    print(f'Total rows: {total_rows}')
    
    #icbc_df.write.options(compression='LZ4', mode='overwrite').parquet("parquet/icbc")
    
    
    
                       



if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('icbc etl') \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(spark)