from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.sql import SparkSession

import pyodbc

schema = StructType([
    StructField("BIRTHDAY_PROMO_TO", DateType(), True),
    StructField("DFL_TELCO", IntegerType(), True),
    StructField("DFL_INSURANCE_HOME", IntegerType(), True),
    StructField("CUSTOMER_ID", StringType(), True),
    StructField("DFL_LUZ", IntegerType(), True),
    StructField("BIRTHDAY_DISCOUNT_REASON", StringType(), True),
    StructField("BIRTHDAY_DISCOUNT_DESCRIPTION", StringType(), True),
    StructField("PERMISSION_COMUNICATION", IntegerType(), True),
    StructField("DFL_GAS", IntegerType(), True),
    StructField("MAIL", StringType(), True),
    StructField("PROMO_BIRTHDAY", IntegerType(), True),
    StructField("BIRTHDAY_PROMO_SINCE", DateType(), True),
    StructField("CUSTOMER_ID_PH", IntegerType(), True)])

schemaSales = StructType([
    StructField("CUSTOMER_ID", IntegerType(), True),
    StructField("DISCOUNT_REASON", StringType(), True),
    StructField("DISCOUNT_DESC", StringType(), True),
    StructField("CLOSED_DATE", StringType(), True),
    StructField("FAMILIA_LTV", StringType(), True),
    StructField("SHOP_ID", IntegerType(), True),
    StructField("SHOP_CITY", StringType(), True),
    StructField("TRANSACTION", StringType(), True),
    StructField("PRODUCT_QUANTITY", StringType(), True),
    StructField("TOTAL_DISCOUNT_AMOUNT", FloatType(), True),
    StructField("TOTAL_RETAIL_AMOUNT", FloatType(), True)])

# Start Spark Session
spark = SparkSession \
    .builder \
    .appName("Spark") \
    .getOrCreate()

# Kafka Consumer
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales-details-topic") \
    .load() \
    .select(from_json(col("value").cast("string"), schemaSales).alias("data")) \
    .select("data.*")


# Spark reads from Kafka Topic
'''df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customer-details-topic") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")'''


# Print data in console
'''df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()'''

# NOTA GABRIEL: DOWNLOAD ODBC Driver 13.1 for SQL Server

# ODBC connection string to Azure SQL Server
conn = 'Driver={ODBC Driver 13 for SQL Server};Server=tcp:tfgalga.database.windows.net,1433;' \
       'Database=ClientesYPromociones;Uid=dataLake-alga;Pwd={Tfgserver!};Encrypt=yes;' \
       'TrustServerCertificate=no;Connection Timeout=30;'


# Writing in SQL Server each interval time

def foreach_batch_function(df, epoch_id):

    data = df.select('*').collect()

    connStr = pyodbc.connect(conn, autocommit=False)
    cursor = connStr.cursor()

    for row in data:
        cursor.execute("INSERT INTO dbo.sales_details("
                       "[CUSTOMER_ID_PH],"
                       "[DISCOUNT_REASON],"
                       "[DISCOUNT_DESC],"
                       "[CLOSED_DATE],"
                       "[FAMILIA_LTV],"
                       "[TRANSACTIONS],"
                       "[PRODUCT_QUANTITY],"
                       "[TOTAL_DISCOUNT_AMOUNT],"
                       "[TOTAL_RETAIL_AMOUNT],"
                       "[SHOP_ID],"
                       "[SHOP_CITY])"
                       "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       row['CUSTOMER_ID'], row['DISCOUNT_REASON'], row['DISCOUNT_DESC'], row['CLOSED_DATE'],
                       row['FAMILIA_LTV'], row['TRANSACTION'], row['PRODUCT_QUANTITY'],
                       row['TOTAL_DISCOUNT_AMOUNT'], row['TOTAL_RETAIL_AMOUNT'], row['SHOP_ID'], row['SHOP_CITY'])

        '''cursor.execute("INSERT INTO dbo.customer_details("
                       "[BIRTHDAY_PROMO_TO],"
                       "[DFL_TELCO],"
                       "[DFL_INSURANCE_HOME],"
                       "[CUSTOMER_ID],"
                       "[DFL_LUZ],"
                       "[BIRTHDAY_DISCOUNT_REASON],"
                       "[BIRTHDAY_DISCOUNT_DESCRIPTION],"
                       "[PERMISSION_COMUNICATION],"
                       "[DFL_GAS],"
                       "[MAIL],"
                       "[PROMO_BIRTHDAY],"
                       "[BIRTHDAY_PROMO_SINCE],"
                       "[CUSTOMER_ID_PH]) "
                       "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       row['BIRTHDAY_PROMO_TO'], row['DFL_TELCO'], row['DFL_INSURANCE_HOME'], row['CUSTOMER_ID'],
                       row['DFL_LUZ'], row['BIRTHDAY_DISCOUNT_REASON'], row['BIRTHDAY_DISCOUNT_DESCRIPTION'],
                       row['PERMISSION_COMUNICATION'],
                       row['DFL_GAS'], row['MAIL'], row['PROMO_BIRTHDAY'], row['BIRTHDAY_PROMO_SINCE'],
                       row['CUSTOMER_ID_PH'])'''

    connStr.commit()

    cursor.close()
    connStr.close()


df.writeStream.foreachBatch(foreach_batch_function).trigger(processingTime='20 seconds').start().awaitTermination()
