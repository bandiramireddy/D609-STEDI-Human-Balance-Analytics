import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_accelerometer_source = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_accelerometer_source")

# Script generated for node customer_trusted
customer_trusted_customer_source = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_customer_source")

# Script generated for node customer_join_accelerometer
SqlQuery2614 = '''

SELECT DISTINCT
    c.serialnumber,
    c.sharewithpublicasofdate,
    c.birthday,
    c.registrationdate,
    c.sharewithresearchasofdate,
    c.customername,
    c.email,
    c.lastupdatedate,
    c.phone
FROM c
INNER JOIN a
    ON c.email = a.`user`

'''
customer_join_accelerometer_customer_sql = sparkSqlQuery(glueContext, query = SqlQuery2614, mapping = {"c":customer_trusted_customer_source, "a":accelerometer_trusted_accelerometer_source}, transformation_ctx = "customer_join_accelerometer_customer_sql")

# Script generated for node customer_curated
customer_curated_customer_target = glueContext.getSink(path="s3://stedi-human-balance-analytics12/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_customer_target")
customer_curated_customer_target.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_customer_target.setFormat("json")
customer_curated_customer_target.writeFrame(customer_join_accelerometer_customer_sql)
job.commit()