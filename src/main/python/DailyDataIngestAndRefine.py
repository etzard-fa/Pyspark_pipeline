from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.main.python.pysparkfunctions import read_schema
import configparser


# Initiating spark session
spark = SparkSession.builder.appName("DailyDataIngest").master("local").getOrCreate()

# Reading the configs
config = configparser.ConfigParser()
config.read(r'../configs/config.ini')
inputLocation = config.get('paths', 'inputLocation')
landingSchemaFromConf = config.get('schema', 'landingFileSchema')

landingFileSchema = read_schema(landingSchemaFromConf)

landingFileDF = spark.read \
    .schema(landingFileSchema) \
    .option("header", "false") \
    .option("delimiter", "|") \
    .csv(inputLocation)

landingFileDF.show()
