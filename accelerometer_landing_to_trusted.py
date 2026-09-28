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

# Script generated for node accelerometer_landing
accelerometer_landing_accelerometer_source = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_accelerometer_source")

# Script generated for node customer_trusted
customer_trusted_customer_source = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_customer_source")

# Script generated for node accelerometer_join_customer
SqlQuery2663 = '''

SELECT
    a.`timestamp`,
    a.`user`,
    a.x,
    a.y,
    a.z
FROM a
INNER JOIN c
    ON a.`user` = c.email

'''
accelerometer_join_customer_accelerometer_sql = sparkSqlQuery(glueContext, query = SqlQuery2663, mapping = {"a":accelerometer_landing_accelerometer_source, "c":customer_trusted_customer_source}, transformation_ctx = "accelerometer_join_customer_accelerometer_sql")

# Script generated for node accelerometer_trusted
accelerometer_trusted_accelerometer_target = glueContext.getSink(path="s3://stedi-human-balance-analytics12/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_accelerometer_target")
accelerometer_trusted_accelerometer_target.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_accelerometer_target.setFormat("json")
accelerometer_trusted_accelerometer_target.writeFrame(accelerometer_join_customer_accelerometer_sql)
job.commit()