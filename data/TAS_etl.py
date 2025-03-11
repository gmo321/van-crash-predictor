import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.sql import functions as F



def main(spark):
    #no_cf_path = 'raw/TAS_no_CF.csv'
    #no_city_path = 'raw/TAS_no_city.csv'
    #entity_path = 'raw/TAS_entity.csv'
    
    no_cf_df_schema = types.StructType([
    StructField("Municipality", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Accident Type", StringType(), True),
    StructField("Collision Type", StringType(), True),
    StructField("Crash Configuration", StringType(), True),
    StructField("Cyclist Involved", StringType(), True),
    StructField("Hit And Run Indicator", StringType(), True),
    StructField("Impact With Animal", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Motorcycle Involved", StringType(), True),
    StructField("Pedestrian Involved", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Road Condition", StringType(), True),
    StructField("Weather", StringType(), True),
    StructField("Crash Count", StringType(), True),
    StructField("Total Casualty", StringType(), True),
    StructField("Total Vehicles Involved", StringType(), True),
    StructField("In Parking Lot", StringType(), True),
    StructField("Land Use", StringType(), True),
    StructField("Light", StringType(), True),
    StructField("On Road", StringType(), True),
    StructField("Pedestrian Activity", StringType(), True),
    StructField("Road Character", StringType(), True),
    StructField("Road Class", StringType(), True),
    StructField("Road Surface", StringType(), True),
    StructField("Speed Advisory", StringType(), True),
    StructField("Speed Zone", StringType(), True),
    StructField("Traffic Control", StringType(), True),
    StructField("Traffic Flow", StringType(), True)
])
  
    no_city_schema = types.StructType([
    StructField("Region", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Accident Type", StringType(), True),
    StructField("Alcohol Involved", StringType(), True),
    StructField("Collision Type", StringType(), True),
    StructField("Crash Configuration", StringType(), True),
    StructField("Cyclist Involved", StringType(), True),
    StructField("Distraction Involved", StringType(), True),
    StructField("Driving Too Fast", StringType(), True),
    StructField("Drug Involved", StringType(), True),
    StructField("Exceeding Speed", StringType(), True),
    StructField("Excessive Speed", StringType(), True),
    StructField("Fell Asleep", StringType(), True),
    StructField("Impact With Animal", StringType(), True),
    StructField("Impaired Involved", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Motorcycle Involved", StringType(), True),
    StructField("Pedestrian Involved", StringType(), True),
    StructField("Road Condition", StringType(), True),
    StructField("Speed Involved", StringType(), True),
    StructField("Weather", StringType(), True),
    StructField("Crash Count", StringType(), True),
    StructField("Total Casualty", StringType(), True),
    StructField("Total Vehicles Involved", StringType(), True),
    StructField("Communication Video Equipment", StringType(), True),
    StructField("Driver In Ext Distraction", StringType(), True),
    StructField("Driver Inattentive", StringType(), True),
    StructField("Driving Without Due Care", StringType(), True),
    StructField("Hit And Run Indicator", StringType(), True),
    StructField("In Parking Lot", StringType(), True),
    StructField("Land Use", StringType(), True),
    StructField("Light", StringType(), True),
    StructField("On Road", StringType(), True),
    StructField("Pedestrian Activity", StringType(), True),
    StructField("Road Character", StringType(), True),
    StructField("Road Class", StringType(), True),
    StructField("Road Surface", StringType(), True),
    StructField("Speed Advisory", StringType(), True),
    StructField("Speed Zone", StringType(), True),
    StructField("Traffic Control", StringType(), True),
    StructField("Traffic Flow", StringType(), True)
])
    
    entity_schema = types.StructType([
    StructField("Region", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Accident Type", StringType(), True),
    StructField("Age Range", StringType(), True),
    StructField("Contributing Factor 1", StringType(), True),
    StructField("Contributing Factor 2", StringType(), True),
    StructField("Contributing Factor 3", StringType(), True),
    StructField("Contributing Factor 4", StringType(), True),
    StructField("Crash Configuration", StringType(), True),
    StructField("Entity Type", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Vehicle Type", StringType(), True),
    StructField("Vehicle Use", StringType(), True),
    StructField("Entity Count", StringType(), True),
    StructField("Collision Type", StringType(), True),
    StructField("Damage Location", StringType(), True),
    StructField("Damage Severity", StringType(), True),
    StructField("Driver License Jurisdiction", StringType(), True),
    StructField("Pre Action", StringType(), True),
    StructField("Travel Direction", StringType(), True),
    StructField("Vehicle Body Style", StringType(), True),
    StructField("Vehicle Jurisdiction", StringType(), True),
    StructField("Vehicle Make", StringType(), True),
    StructField("Vehicle Model Year", StringType(), True)
])
    
    
    no_cf_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("ignoreLeadingWhiteSpace", "true") \
                        .option("encoding", "UTF-16") \
                        .csv("s3a://van-crash-data/TAS/raw/TAS_no_CF.csv", schema=no_cf_df_schema).repartition(60)
                        
    no_city_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("encoding", "UTF-16") \
                        .csv("s3a://van-crash-data/TAS/raw/TAS_no_city.csv", schema=no_city_schema).repartition(60)
                        
                        
    entity_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("encoding", "UTF-16") \
                        .csv("s3a://van-crash-data/TAS/raw/TAS_entity.csv", schema=entity_schema).repartition(60)
                        
                        
    #total_rows_1 = no_cf_df.count() 107020 
    #total_rows_2 = no_city_df.count() 106841
    #total_rows_3 = entity_df.count() 196395
    #print(f'Total rows: {total_rows_1} \n',
    #      f'Total rows: {total_rows_2} \n',
    #      f'Total rows: {total_rows_3} \n')

    #entity_df.select(["Travel Direction"]).show(10, truncate=False)
    no_cf_df = no_cf_df.drop('In Parking Lot', 'Crash Count', 'Land Use', 'Light', 'On Road', 'Pedestrian Activity', 'Road Character', 'Road Class', 'Speed Advisory')
    
    no_city_df = no_city_df.drop('In Parking Lot', 'Land Use', 'Light', 'On Road', 'Pedestrian Activity', 'Road Character', 'Road Class', 
                                 'Communication Video Equipment', 'Speed Advisory')
    
    entity_df = entity_df.drop('Driver License Jurisdiction', 'Vehicle Jurisdiction', "Entity Type", "Entity Count", 'Vehicle Body Style', "Travel Direction")
    
    

    
    
    
    # Check for nulls
    #no_cf_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in no_cf_df.columns]).show() # Speed zone - 30
    #no_city_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in no_city_df.columns]).show()
    #entity_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in entity_df.columns]).show() # Collision Type - 88
    
    # Drop null rows
    no_cf_df = no_cf_df.dropna()
    no_city_df = no_city_df.dropna()
    entity_df = entity_df.dropna()
    
    
    # Renaming columns for consistency    
    no_cf_df = no_cf_df.withColumnsRenamed({'Municipality': 'municipality', 
                                          'Year': 'year', 
                                          'Accident Type': 'accident_type', 
                                          'Collision Type': 'collision_type',
                                          'Crash Configuration': 'crash_configuration', 
                                          'Cyclist Involved': 'cyclist_involved',
                                          'Hit And Run Indicator':'hit_and_run_indicator', 
                                          'Impact With Animal': 'impact_with_animal',
                                          'Month': 'month',
                                          'Motorcycle Involved': 'motorcycle_involved',
                                          'Pedestrian Involved':'pedestrian_involved',
                                          'Region':'region',
                                          'Road Condition':'road_condition',
                                          'Weather':'weather',
                                          'Total Casualty': 'total_casualty',
                                          'Total Vehicles Involved': 'total_vehicles_involved',
                                          'Road Surface':'road_surface',
                                          'Speed Zone':'speed_zone',
                                          'Traffic Control':'traffic_control',
                                          'Traffic Flow':'traffic_flow'})
    
    no_city_df = no_city_df.withColumnsRenamed({'Region': 'region', 
                                                'Year': 'year', 
                                                'Accident Type': 'accident_type', 
                                                'Alcohol Involved': 'alcohol_involved', 
                                                'Collision Type': 'collision_type', 
                                                'Crash Configuration': 'crash_configuration', 
                                                'Cyclist Involved': 'cyclist_involved', 
                                                'Distraction Involved': 'distraction_involved', 
                                                'Driving Too Fast': 'driving_too_fast', 
                                                'Drug Involved': 'drug_involved', 
                                                'Exceeding Speed': 'exceeding_speed', 
                                                'Excessive Speed': 'excessive_speed', 
                                                'Fell Asleep': 'fell_asleep', 
                                                'Impact With Animal': 'impact_with_animal', 
                                                'Impaired Involved': 'impaired_involved', 
                                                'Month': 'month', 
                                                'Motorcycle Involved': 'motorcycle_involved', 
                                                'Pedestrian Involved': 'pedestrian_involved', 
                                                'Road Condition': 'road_condition', 
                                                'Speed Involved': 'speed_involved', 
                                                'Weather': 'weather', 
                                                'Crash Count': 'crash_count', 
                                                'Total Casualty': 'total_casualty', 
                                                'Total Vehicles Involved': 'total_vehicles_involved', 
                                                'Driver In Ext Distraction': 'driver_in_ext_distraction', 
                                                'Driver Inattentive': 'driver_inattentive', 
                                                'Driving Without Due Care': 'driving_without_due_care', 
                                                'Hit And Run Indicator': 'hit_and_run_indicator', 
                                                'Road Surface': 'road_surface', 
                                                'Speed Zone': 'speed_zone', 
                                                'Traffic Control': 'traffic_control', 
                                                'Traffic Flow': 'traffic_flow'})
    
    entity_df = entity_df.withColumnsRenamed({'Region': 'region',
                                            'Year': 'year',
                                            'Accident Type': 'accident_type',
                                            'Age Range': 'age_range',
                                            'Contributing Factor 1': 'contributing_factor_1',
                                            'Contributing Factor 2': 'contributing_factor_2',
                                            'Contributing Factor 3': 'contributing_factor_3',
                                            'Contributing Factor 4': 'contributing_factor_4',
                                            'Crash Configuration': 'crash_configuration',
                                            'Gender': 'gender',
                                            'Month': 'month',
                                            'Vehicle Type': 'vehicle_type',
                                            'Vehicle Use': 'vehicle_use',
                                            'Collision Type': 'collision_type',
                                            'Damage Location': 'damage_location',
                                            'Damage Severity': 'damage_severity',
                                            'Pre Action': 'pre_action',
                                            'Vehicle Make': 'vehicle_make',
                                            'Vehicle Model Year': 'vehicle_model_year'})
    
    
    
    # Regex to remove unwanted (non-ASCII) characters
    no_cf_df = no_cf_df.withColumn("traffic_flow", F.regexp_replace(F.col("traffic_flow"), "[^\x00-\x7F]", ""))
    entity_df = entity_df.withColumn("vehicle_model_year", F.regexp_replace(F.col("vehicle_model_year"), "[^\x00-\x7F]", ""))
    
    no_cf_df.show(3, truncate=False)
    #no_city_df.show(3, truncate=False)
    #entity_df.show(3, truncate=False)
    

    # Clean column 'Speed Zone' to extract speed in Km/H
    no_cf_df = no_cf_df.withColumn('speed_zone',
                                   F.when(F.col("speed_zone").rlike(r"\d+\s*Km/H"),  
                                    F.regexp_extract(F.col("speed_zone"), r"(\d+)\s*Km/H", 1)
                                ).otherwise(F.lit(None))
    )
    
    #no_cf_df = no_cf_df.select(['Speed Zone']).show(50, truncate=False)
    
    # See where the nulls are concentrated
    #no_cf_df.filter(F.col("Speed Zone").isNull()).groupBy("Municipality").count().show()

    # TODO some columns have nulls or "Not applicable" etc. 
    #no_cf_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in no_cf_df.columns]).show()
    # speed zone nulls: 17079
    
    #[('Municipality', 'string'), ('Year', 'string'), ('Accident Type', 'string'), ('Collision Type', 'string'),
    # ('Crash Configuration', 'string'), ('Cyclist Involved', 'string'), ('Hit And Run Indicator', 'string'), 
    # ('Impact With Animal', 'string'), ('Month', 'string'), ('Motorcycle Involved', 'string'), ('Pedestrian Involved', 'string'), 
    # ('Region', 'string'), ('Road Condition', 'string'), ('Weather', 'string'), ('Total Casualty', 'string'),
    # ('Total Vehicles Involved', 'string'), ('Road Surface', 'string'), ('Speed Advisory', 'string'), ('Speed Zone', 'string'),
    # ('Traffic Control', 'string'), ('Traffic Flow', 'string')]

    # TODO Merge Datasets:       
    
    
    #print(no_cf_df.columns)
    #['Municipality', 'Year', 'Accident Type', 'Collision Type', 'Crash Configuration', 'Cyclist Involved', 'Hit And Run Indicator', 'Impact With Animal', 
    # 'Month', 'Motorcycle Involved', 'Pedestrian Involved', 'Region', 'Road Condition', 'Weather', 'Total Casualty', 'Total Vehicles Involved', 
    # 'Road Surface', 'Speed Zone', 'Traffic Control', 'Traffic Flow']
    #print(no_city_df.columns)
    #['Region', 'Year', 'Accident Type', 'Alcohol Involved', 'Collision Type', 'Crash Configuration', 'Cyclist Involved', 'Distraction Involved', 
    # 'Driving Too Fast', 'Drug Involved', 'Exceeding Speed', 'Excessive Speed', 'Fell Asleep', 'Impact With Animal', 'Impaired Involved', 
    # 'Month', 'Motorcycle Involved', 'Pedestrian Involved', 'Road Condition', 'Speed Involved', 'Weather', 'Crash Count', 'Total Casualty', 
    # 'Total Vehicles Involved', 'Driver In Ext Distraction', 'Driver Inattentive', 'Driving Without Due Care', 'Hit And Run Indicator', 
    # 'Road Surface', 'Speed Advisory', 'Speed Zone', 'Traffic Control', 'Traffic Flow']
    #print(entity_df.columns)
    #['Region', 'Year', 'Accident Type', 'Age Range', 'Contributing Factor 1', 'Contributing Factor 2', 'Contributing Factor 3', 'Contributing Factor 4', 
    # 'Crash Configuration', 'Gender', 'Month', 'Vehicle Type', 'Vehicle Use', 'Collision Type', 'Damage Location', 'Damage Severity', 'Pre Action', 
    # 'Vehicle Make', 'Vehicle Model Year']
    
    #no_cf_df = no_cf_df.withColumnsRenamed({'Cyclist Involved': 'Cyclist Involved_cf', 
    #                                      'Hit And Run Indicator': 'Hit And Run Indicator_cf',
    #                                      'Impact With Animal': 'Impact With Animal_cf',
    #                                      'Motorcycle Involved': 'Motorcycle Involved_cf',
    #                                      'Pedestrian Involved': 'Pedestrian Involved_cf',
    #                                      'Road Condition': 'Road Condition_cf',
    #                                      'Weather': 'Weather_cf',
    #                                      'Total Casualty': 'Total Casualty_cf',
    #                                      'Total Vehicles Involved': 'Total Vehicles Involved_cf',
    #                                      'Road Surface': 'Road Surface_cf',
    #                                      'Speed Zone': 'Speed Zone_cf',
    #                                      'Traffic Control': 'Traffic Control_cf',
    #                                      'Traffic Flow': 'Traffic Flow_cf'
    #                                      }) 


    # Common Columns: {'Cyclist Involved', 'Hit And Run Indicator', 'Impact With Animal', 'Motorcycle Involved', 
    # 'Pedestrian Involved', 'Road Condition', 'Weather', 'Total Casualty', 'Total Vehicles Involved', 
    # 'Road Surface', 'Speed Zone', 'Traffic Control', 'Traffic Flow'}
                   

    #merged_df = no_cf_df.join(no_city_df, on=["Region", "Year", "Month", "Accident Type", "Collision Type",
    #                                          "Crash Configuration"], how="inner")
    
    #merged_df.show(2, truncate=False)
    
    #print(merged_df.count()) #2434147
    
    
    
    # Select only the columns you need, dropping duplicates
    
    #merged_df_clean = merged_df.select(
    #    "Region", "Year", "Month", "Accident Type",
    #    "Collision Type", "Crash Configuration", "Cyclist Involved", 
    #    "Hit And Run Indicator", "Impact With Animal", "Motorcycle Involved", 
    #    "Pedestrian Involved", "Road Condition", "Weather", "Total Casualty", 
    #    "Total Vehicles Involved", "Road Surface", "Speed Zone", "Traffic Control", 
    #    "Traffic Flow",  # Columns from no_cf_df
    #    "Alcohol Involved", "Distraction Involved", "Driving Too Fast", 
    #    "Drug Involved", "Exceeding Speed", "Excessive Speed", "Fell Asleep", 
    #    "Impaired Involved", "Driver In Ext Distraction", "Driver Inattentive", 
    #    "Driving Without Due Care", "Crash Count"  # Columns from no_city_df
    #)
                                            
    
    # Write parquet files
    #no_cf_df.write.parquet("data/parquet/TAS/no_cf", compression='LZ4', mode='overwrite')
    #no_city_df.write.parquet("data/parquet/TAS/no_city", compression='LZ4', mode='overwrite')
    #entity_df.write.parquet("data/parquet/TAS/entity", compression='LZ4', mode='overwrite')
    
    # Read parquet files
    #no_cf_df_parquet = spark.read.parquet('data/parquet/TAS/no_city')
    
    #no_cf_df_parquet.show()


if __name__ == '__main__':  
    spark = SparkSession.builder \
        .appName('Traffic Accident System etl') \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(spark)