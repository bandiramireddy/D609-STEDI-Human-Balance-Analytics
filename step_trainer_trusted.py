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

# Script generated for node step_trainer_landing
step_trainer_landing_step_source = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_step_source")

# Script generated for node customer_curated
customer_curated_customer_source = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_customer_source")

# Script generated for node step_trainer_join_customer
SqlQuery2697 = '''

SELECT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject
FROM s
INNER JOIN c
    ON s.serialnumber = c.serialnumber

'''
step_trainer_join_customer_step_sql = sparkSqlQuery(glueContext, query = SqlQuery2697, mapping = {"s":step_trainer_landing_step_source, "c":customer_curated_customer_source}, transformation_ctx = "step_trainer_join_customer_step_sql")

# Script generated for node step_trainer_trusted
step_trainer_trusted_step_target = glueContext.getSink(path="s3://stedi-human-balance-analytics12/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_step_target")
step_trainer_trusted_step_target.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_step_target.setFormat("json")
step_trainer_trusted_step_target.writeFrame(step_trainer_join_customer_step_sql)
job.commit()