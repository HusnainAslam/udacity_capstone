from pyspark.sql import SparkSession, DataFrame
import configparser
import pandas as pd
import logging
from typing import Dict, List, Tuple
from pyspark.sql.functions import udf, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pathlib import Path
from datetime import datetime, timedelta
from sql_queries import *

logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='/Users/hussnainaslam/husnain-data/udacity/capstone/myapp.log',
                    filemode='w')
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


config = configparser.ConfigParser()
config.read('conf.cfg')

SAS_LABLES_DESCRIPTION_FILE_PATH = config.get('DATA', 'SAS_LABLES_DESCRIPTION_FILE_PATH')
I94_DATA_FILES_PATH = config.get('DATA', 'I94_DATA_FILES_PATH')
I94_PARQUET_DATA_FILES_PATH = config.get('DATA', 'I94_PARQUET_DATA_FILES_PATH')
US_CITIES_DEMOGRAPHICS_DATA_PATH = config.get('DATA', 'US_CITIES_DEMOGRAPHICS_DATA_PATH')
AIRPORT_CODES_DATA_PATH = config.get('DATA', 'AIRPORT_CODES_DATA_PATH')
OUTPUT_DATA_DIR_PATH = config.get('DATA', 'OUTPUT_DATA_DIR_PATH')

I94_labels = ['I94MODE', 'I94VISA', 'I94ADDR', 'I94PORT', 'I94RES']


def get_spark_session() -> SparkSession:
    """
    Create or Get Spark Session
    :return spark session
    """
    spark = SparkSession.builder.\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()

    return spark


def parse_sas_label_file() -> Dict[str, List[Tuple[str, str]]]:
    """
    Parse all Required labels from Label Description file
    :return: Dictionary, Label as Key, value as list of tuples
    """
    code_values = {}

    with open(SAS_LABLES_DESCRIPTION_FILE_PATH) as file:
        content = file.read()

    for lbl in I94_labels:
        # Pick data from label to onward until ;
        label_data = content[content.index(lbl):]
        label_data = label_data[:label_data.index(';')]

        lines = label_data.split('\n')
        code_value_pairs = list()
        for line in lines:
            parts = line.split('=')
            if len(parts) != 2:
                # Skip comment or other lines with no codes mapping
                continue
            code = parts[0].strip().strip("'").strip()
            value = parts[1].strip().strip("'").strip()
            code_value_pairs.append((code, value,))

        code_values[lbl] = code_value_pairs

    return code_values


def clean_and_stage_i94_travel_mode(spark, code_values):
    """
    Read all data under I94Mode label, Clean data and Create view
    :param spark: SparkSession Object
    :param code_values: Label description Dict
    :return: None
    """
    schema = StructType([
        StructField("mode_id", StringType()),
        StructField("mode_name", StringType())
    ])
    df = spark.createDataFrame(data=code_values['I94MODE'], schema=schema)
    df = df.dropna().dropDuplicates()
    df.createOrReplaceTempView('tbl_travel_mode_stage')


def clean_and_stage_i94_states(spark, code_values):
    """
    Read all data under I94ADDR label, Clean data exclude 99 code and Create view
    :param spark: SparkSession Object
    :param code_values: Label description Dict
    :return: None
    """
    schema = StructType([
        StructField("state_code", StringType()),
        StructField("state_name", StringType())
    ])
    df = spark.createDataFrame(data=code_values['I94ADDR'], schema=schema)
    df = df.filter('state_code != "99"').dropna().dropDuplicates()
    df.createOrReplaceTempView('tbl_state_stage')


def clean_and_stage_i94_visas(spark, code_values):
    """
    Read all data under I94VISA label, Clean data, Drop duplicates and Create view
    :param spark: SparkSession Object
    :param code_values: Label description Dict
    :return: None
    """
    schema = StructType([
        StructField("visa_type_id", StringType()),
        StructField("visa_type", StringType())
    ])
    df = spark.createDataFrame(data=code_values['I94VISA'], schema=schema)
    df = df.dropna().dropDuplicates()
    df.createOrReplaceTempView('tbl_visa_stage')


def clean_and_stage_i94_ports(spark, code_values):
    """
    Read all data under I94PORT label,
    Split Port_name as city_name and state_code
    Clean data, Drop duplicates and Create view
    :param spark: SparkSession Object
    :param code_values: Label description Dict
    :return: None
    """
    schema = StructType([
        StructField("port_code", StringType()),
        StructField("port_name", StringType())
    ])
    df = spark.createDataFrame(data=code_values['I94PORT'], schema=schema)
    city_name = udf(lambda port_name: port_name.split(',')[0].strip() if port_name else None)
    state_code = udf(lambda port_name: port_name.split(',')[1].strip() if (port_name and len(port_name.split(',')) > 1) else None)

    df = df.withColumn('city_name', city_name(df.port_name))\
        .withColumn('state_code', state_code(df.port_name)) \
        .drop('port_name')\
        .dropna().dropDuplicates()
    df.createOrReplaceTempView('tbl_port_stage')


def clean_and_stage_i94_countries(spark, code_values):
    """
    Read all data under I94RES label,
    Clean data, Drop duplicates and Create view
    :param spark: SparkSession Object
    :param code_values: Label description Dict
    :return: None
    """
    schema = StructType([
        StructField("country_code", StringType()),
        StructField("country_name", StringType())
    ])
    df = spark.createDataFrame(data=code_values['I94RES'], schema=schema)
    df = df.withColumn('country_name', regexp_replace('country_name', '^No Country.*|INVALID.*|Collapsed.*', 'NA'))\
        .dropna().dropDuplicates()
    df.createOrReplaceTempView('tbl_county_stage')


def clean_and_stage_i94_data(spark):
    """
    Read all immigration data from given file path,
    Convert dates to iso format
    Clean data, Drop duplicates and Create view
    :param spark: SparkSession Object
    :return: None
    """
    if Path(I94_PARQUET_DATA_FILES_PATH).exists():
        logger.info(f"Reading PARQUET data from: {I94_PARQUET_DATA_FILES_PATH}")
        df = spark.read.parquet(I94_PARQUET_DATA_FILES_PATH)
    else:
        logger.info(f"Reading SAS7BDAT file form path: {I94_DATA_FILES_PATH}")
        df = spark.read.format('com.github.saurfang.sas.spark').load(I94_DATA_FILES_PATH)
    iso_format_date = udf(lambda dt: (datetime(1960, 1, 1).date() + timedelta(dt)).isoformat() if dt else None)
    valid_birth_year = udf(lambda yr: yr if (yr and 1900 <= yr <= 2016) else None)

    df = df.withColumn('arrdate', iso_format_date(df.arrdate)) \
        .withColumn('depdate', iso_format_date(df.depdate)) \
        .withColumn("biryear", valid_birth_year(df.biryear)) \
        .dropDuplicates().dropna(how='all')
    df.createOrReplaceTempView('tbl_I94_data_stage')


def clean_and_stage_cities_demographics(spark):
    """
    Read all cities demographics data from given file path,
    Clean data, Drop duplicates and Create view
    :param spark: SparkSession Object
    :return: None
    """
    schema = StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("median_age", DoubleType()),
        StructField("male_population", IntegerType()),
        StructField("female_population", IntegerType()),
        StructField("total_population", IntegerType()),
        StructField("number_of_veterans", IntegerType()),
        StructField("foreign_born", IntegerType()),
        StructField("average_household_size", DoubleType()),
        StructField("state_code", StringType()),
        StructField("race", StringType()),
        StructField("count", IntegerType())
    ])
    df = spark.read.csv(US_CITIES_DEMOGRAPHICS_DATA_PATH, sep=';', header=True, schema=schema)
    df = df.dropDuplicates().dropna(how='all')
    df.createOrReplaceTempView('tbl_cities_demographics_stage')


def clean_and_stage_airport_codes(spark):
    """
    Read all Airport Codes data from given file path,
    Clean data, Drop duplicates and Create view
    :param spark: SparkSession Object
    :return: None
    """
    schema = StructType([
        StructField("ident", StringType()),
        StructField("type", StringType()),
        StructField("name", StringType()),
        StructField("elevation_ft", DoubleType()),
        StructField("continent", StringType()),
        StructField("country_code", StringType()),
        StructField("iso_region", StringType()),
        StructField("municipality", StringType()),
        StructField("gps_code", StringType()),
        StructField("iata_code", StringType()),
        StructField("local_code", StringType()),
        StructField("coordinates", StringType())
    ])
    df = spark.read.csv(AIRPORT_CODES_DATA_PATH, sep=';', header=True, schema=schema)
    latitude = udf(lambda coordinates: coordinates.split(',')[0].strip() if coordinates else None)
    longitude = udf(lambda coordinates: coordinates.split(',')[1].strip() if coordinates else None)
    state_code = udf(lambda region: region.split('-')[1].strip() if region else None)
    df = df.withColumn('state_code', state_code(df.iso_region))\
        .withColumn('latitude', latitude(df.coordinates))\
        .withColumn('longitude', longitude(df.coordinates))\
        .drop('iso_region')\
        .drop('coordinates')\
        .dropDuplicates().dropna(how='all')
    df.createOrReplaceTempView('tbl_airport_data_stage')


def create_I94_fact_table(spark):
    """
    Create I94 fact table from query
    :param spark: SparkSession Object
    :return: None
    """
    data_table = spark.sql(I94_data_fact_query())
    data_table.createOrReplaceTempView('tbl_I94_data')


def create_city_demographic_dim_table(spark):
    """
    Create demographic dimension table from query
    :param spark: SparkSession Object
    :return: None
    """
    data_table = spark.sql(city_demographic_dim_query())
    data_table.createOrReplaceTempView('tbl_city_demographic')


def check_data_quality(spark: SparkSession):
     """
    Performs two data quality checks against dataframes.
    1. Each table is not empty.
    2. Validate that immigrations fact table contains only valid values as per dimensional tables.
    :param spark: SparkSession Object
    :return: None
    """
    tables = ['tbl_travel_mode_stage', 'tbl_state_stage', 'tbl_visa_stage', 'tbl_port_stage', 'tbl_county_stage',
              'tbl_airport_data_stage', 'tbl_I94_data', 'tbl_city_demographic']
    for name in tables:
        data = spark.sql(f"SELECT * FROM {name} LIMIT 1")
        if data.count() == 0:
            raise Exception(f"Data Quality Check Failed: {name} is empty")

    if spark.sql('Select DISTINCT visa_type_code FROM tbl_I94_data').count() <= \
            spark.sql('SELECT DISTINCT visa_type_id FROM tbl_visa_stage').count():
        raise Exception(f"Inconsistent visa type for I94_data")

    if spark.sql('Select DISTINCT us_state_code FROM tbl_I94_data').count() <= \
            spark.sql('SELECT DISTINCT state_code FROM tbl_state_stage').count():
        raise Exception("Inconsistent state_code for I94_data")

    if spark.sql('Select DISTINCT travel_mode_code FROM tbl_I94_data').count() <= \
            spark.sql('SELECT DISTINCT mode_id FROM tbl_travel_mode_stage').count():
        raise Exception("Inconsistent travel_mode_code for I94_data")

    if spark.sql('Select DISTINCT origin_country_code FROM tbl_I94_data').count() <= \
            spark.sql('SELECT DISTINCT country_code FROM tbl_county_stage').count():
        raise Exception("Inconsistent origin_country_code for I94_data")

    if spark.sql('Select DISTINCT port_code FROM tbl_I94_data').count() <= \
            spark.sql('SELECT DISTINCT port_code FROM tbl_port_stage').count():
        raise Exception("Inconsistent port_code for I94_data")


def store_output(spark):
    Path(OUTPUT_DATA_DIR_PATH).mkdir(parents=True, exist_ok=True)
    tables = ['tbl_travel_mode_stage', 'tbl_state_stage', 'tbl_visa_stage', 'tbl_port_stage', 'tbl_county_stage',
              'tbl_airport_data_stage', 'tbl_city_demographic']
    for table in tables:
        spark.table(table).write.mode('overwrite').\
            parquet(OUTPUT_DATA_DIR_PATH + table.replace('tbl_', 'dim_').replace('_stage', '.parquet'))

    spark.table('tbl_I94_data').write.mode('overwrite').partitionBy('entry_year', 'entry_month', 'port_code').\
        parquet(OUTPUT_DATA_DIR_PATH + 'fact_immigration.parquet')


def execute():
    logger.info("Start Parsing SaS Labels file")
    code_values = parse_sas_label_file()
    logger.info("Completed Label File Parsing")
    spark = get_spark_session()
    logger.info("Created Spark Session")

    clean_and_stage_i94_travel_mode(spark, code_values)
    logger.info("Clean and Staged Travel Mode Data")
    clean_and_stage_i94_states(spark, code_values)
    logger.info("Clean and Staged States Data")
    clean_and_stage_i94_visas(spark, code_values)
    logger.info("Clean and Staged Visas Data")
    clean_and_stage_i94_ports(spark, code_values)
    logger.info("Clean and Staged Ports Data")
    clean_and_stage_i94_countries(spark, code_values)
    logger.info("Clean and Staged Countries Data")
    clean_and_stage_i94_data(spark)
    logger.info("Clean and Staged Immigration Data")
    clean_and_stage_cities_demographics(spark)
    logger.info("Clean and Staged Cities Demographic Data")
    clean_and_stage_airport_codes(spark)
    logger.info("Clean and Staged Airport Code Data")
    create_I94_fact_table(spark)
    logger.info("Created Immigration Fact Data Table View")
    create_city_demographic_dim_table(spark)
    logger.info("Created Cities Demographic Data Table View")

    logger.info("Starting Data Quality Checks")
    check_data_quality(spark)
    logger.info("Data Quality Checks Completed Successfully")

    logger.info("Starts Storing Fact and Dim Tables in File")
    store_output(spark)
    logger.info("Stored Fact and Dim Tables in File")

    logger.info("Job Completed Successfully!")


if __name__ == '__main__':
    execute()

