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

# Script generated for node customer_curated
customer_curated_node1738270522141 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-bkt/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1738270522141")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1738270772396 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-bkt/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1738270772396")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1738270137457 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-bkt/step_trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1738270137457")

# Script generated for node SQL Query
SqlQuery6374 = '''
select * from act
join stt on act.timestamp = stt.sensorreadingtime
join cuc on cuc.serialnumber = stt.serialnumber
'''
SQLQuery_node1738271800631 = sparkSqlQuery(glueContext, query = SqlQuery6374, mapping = {"stt":step_trainer_trusted_node1738270137457, "cuc":customer_curated_node1738270522141, "act":accelerometer_trusted_node1738270772396}, transformation_ctx = "SQLQuery_node1738271800631")

# Script generated for node Drop Duplicates
DropDuplicates_node1738324623733 =  DynamicFrame.fromDF(SQLQuery_node1738271800631.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1738324623733")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1738324623733, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738262772389", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1738272515209 = glueContext.getSink(path="s3://udacity-bkt/step_trainer/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1738272515209")
machine_learning_curated_node1738272515209.setCatalogInfo(catalogDatabase="udacity-db",catalogTableName="machine_learning_curated")
machine_learning_curated_node1738272515209.setFormat("json")
machine_learning_curated_node1738272515209.writeFrame(DropDuplicates_node1738324623733)
job.commit()