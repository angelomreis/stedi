import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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
customer_trusted_node1738263695389 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-bkt/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1738263695389")

# Script generated for node accelerometer_landing
accelerometer_landing_node1738263459142 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-bkt/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1738263459142")

# Script generated for node Join
Join_node1738265271615 = Join.apply(frame1=accelerometer_landing_node1738263459142, frame2=customer_trusted_node1738263695389, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1738265271615")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=Join_node1738265271615, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738262772389", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1738265360504 = glueContext.getSink(path="s3://udacity-bkt/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1738265360504")
accelerometer_trusted_node1738265360504.setCatalogInfo(catalogDatabase="udacity-db",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1738265360504.setFormat("json")
accelerometer_trusted_node1738265360504.writeFrame(Join_node1738265271615)
job.commit()
