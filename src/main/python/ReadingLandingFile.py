from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.main.python.pysparkfunctions import read_schema
import configparser


# Initiating spark session
spark = SparkSession.builder.appName("ReadingLandingFile").master("local").getOrCreate()

# Reading the configs
config = configparser.ConfigParser()
config.read(r'../configs/config.ini')
inputLocation = config.get('paths', 'inputLocation')
landingSchemaFromConf = config.get('schema', 'landingFileSchema')

landingFileSchema = read_schema(landingSchemaFromConf)

# Reading landing zone
landingFileSchema = StructType([
   StructField('Sale_ID', StringType(), True),
    StructField('Product_ID', StringType(), True),
   StructField('Quantity_Sold', IntegerType(), True),
   StructField('Vendor_ID', StringType(), True),
  StructField('Sale_Date', TimestampType(), True),
   StructField('Sale_Amount', DoubleType(), True),
   StructField('Sale_Currency', StringType(), True)
])

landingFileDF = spark.read \
    .schema(landingFileSchema) \
    .option("header", "false") \
    .option("delimiter", "|") \
    .csv(inputLocation)

landingFileDF.show()
