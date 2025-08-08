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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1754614935956 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1754614935956")

# Script generated for node Amazon S3
AmazonS3_node1754614887895 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jb-testspark/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1754614887895")

# Script generated for node SQL Query
SqlQuery5213 = '''
select distinct
    c.customername, 
    c.email,
    c.phone,
    c.birthday,
    c.serialnumber,
    c.registrationdate,
    c.lastupdatedate,
    c.sharewithresearchasofdate,
    c.sharewithpublicasofdate,
    c.sharewithfriendsasofdate
from accelerometer_landing l
inner join customer_trusted c
    on l.user = c.email
'''
SQLQuery_node1754615206593 = sparkSqlQuery(glueContext, query = SqlQuery5213, mapping = {"customer_trusted":AWSGlueDataCatalog_node1754614935956, "accelerometer_landing":AmazonS3_node1754614887895}, transformation_ctx = "SQLQuery_node1754615206593")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1754615206593, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754614742900", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1754615020034 = glueContext.getSink(path="s3://jb-testspark/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1754615020034")
AmazonS3_node1754615020034.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_curated")
AmazonS3_node1754615020034.setFormat("json")
AmazonS3_node1754615020034.writeFrame(SQLQuery_node1754615206593)
job.commit()