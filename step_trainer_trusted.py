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

# Script generated for node step_trainer_landing
step_trainer_landing_node1754663662248 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1754663662248")

# Script generated for node customer_curated
customer_curated_node1754663896462 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_curated", transformation_ctx="customer_curated_node1754663896462")

# Script generated for node SQL Query
SqlQuery5153 = '''
select s.* 
from step_trainer_landing s
join customer_curated c 
    on s.serialnumber = c.serialnumber

'''
SQLQuery_node1754663915365 = sparkSqlQuery(glueContext, query = SqlQuery5153, mapping = {"customer_curated":customer_curated_node1754663896462, "step_trainer_landing":step_trainer_landing_node1754663662248}, transformation_ctx = "SQLQuery_node1754663915365")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1754663915365, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754662918805", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1754665112689 = glueContext.getSink(path="s3://jb-testspark/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1754665112689")
AmazonS3_node1754665112689.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
AmazonS3_node1754665112689.setFormat("json")
AmazonS3_node1754665112689.writeFrame(SQLQuery_node1754663915365)
job.commit()