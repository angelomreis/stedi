import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node customer_trusted
customer_trusted_node1738267640480 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-bkt/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1738267640480")

# Script generated for node SQL Query
SqlQuery6161 = '''
select * from ct
where serialnumber is not null
'''
SQLQuery_node1738267713209 = sparkSqlQuery(glueContext, query = SqlQuery6161, mapping = {"ct":customer_trusted_node1738267640480}, transformation_ctx = "SQLQuery_node1738267713209")

# Script generated for node Drop Duplicates
DropDuplicates_node1738268133625 =  DynamicFrame.fromDF(SQLQuery_node1738267713209.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1738268133625")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1738268133625, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738262772389", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1738268239911 = glueContext.getSink(path="s3://udacity-bkt/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1738268239911")
customer_curated_node1738268239911.setCatalogInfo(catalogDatabase="udacity-db",catalogTableName="customer_curated")
customer_curated_node1738268239911.setFormat("json")
customer_curated_node1738268239911.writeFrame(DropDuplicates_node1738268133625)
job.commit()