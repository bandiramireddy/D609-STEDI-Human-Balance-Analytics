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

# Script generated for node step_trainer_trusted
step_trainer_trusted_step_source = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_step_source")

# Script generated for node step_accelerometer_timestamp_join
SqlQuery2569 = '''

SELECT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.`timestamp`,
    a.`user`,
    a.x,
    a.y,
    a.z
FROM s
INNER JOIN a
    ON s.sensorreadingtime = a.`timestamp`

'''
step_accelerometer_timestamp_join_ml_sql = sparkSqlQuery(glueContext, query = SqlQuery2569, mapping = {"s":step_trainer_trusted_step_source, "a":accelerometer_trusted_accelerometer_source}, transformation_ctx = "step_accelerometer_timestamp_join_ml_sql")

# Script generated for node machine_learning_curated
machine_learning_curated_ml_target = glueContext.getSink(path="s3://stedi-human-balance-analytics12/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_ml_target")
machine_learning_curated_ml_target.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_ml_target.setFormat("json")
machine_learning_curated_ml_target.writeFrame(step_accelerometer_timestamp_join_ml_sql)
job.commit()