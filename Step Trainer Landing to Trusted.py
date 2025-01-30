import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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
step_trainer_landing_node1738268913050 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-bkt/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1738268913050")

# Script generated for node customer_curated
customer_curated_node1738268948063 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-bkt/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1738268948063")

# Script generated for node Join
step_trainer_landing_node1738268913050DF = step_trainer_landing_node1738268913050.toDF()
customer_curated_node1738268948063DF = customer_curated_node1738268948063.toDF()
Join_node1738268999724 = DynamicFrame.fromDF(step_trainer_landing_node1738268913050DF.join(customer_curated_node1738268948063DF, (step_trainer_landing_node1738268913050DF['serialnumber'] == customer_curated_node1738268948063DF['serialnumber']), "left"), glueContext, "Join_node1738268999724")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=Join_node1738268999724, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738262772389", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1738269255837 = glueContext.getSink(path="s3://udacity-bkt/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1738269255837")
step_trainer_trusted_node1738269255837.setCatalogInfo(catalogDatabase="udacity-db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1738269255837.setFormat("json")
step_trainer_trusted_node1738269255837.writeFrame(Join_node1738268999724)
job.commit()