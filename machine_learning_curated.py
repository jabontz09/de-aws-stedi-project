import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1754665658918 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1754665658918")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1754665694177 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1754665694177")

# Script generated for node SQL Query
SqlQuery5019 = '''
select * 
from accelerometer_trusted a
join step_trainer_trusted s 
    on a.timestamp = s.sensorreadingtime

'''
SQLQuery_node1754666215210 = sparkSqlQuery(glueContext, query = SqlQuery5019, mapping = {"accelerometer_trusted":accelerometer_trusted_node1754665658918, "step_trainer_trusted":step_trainer_trusted_node1754665694177}, transformation_ctx = "SQLQuery_node1754666215210")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1754666215210, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754665447607", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1754666033218 = glueContext.getSink(path="s3://jb-testspark/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1754666033218")
AmazonS3_node1754666033218.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
AmazonS3_node1754666033218.setFormat("json")
AmazonS3_node1754666033218.writeFrame(SQLQuery_node1754666215210)
job.commit()