import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "etl_virtual_database", table_name = "input_folder", transformation_ctx = "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("price", "long", "price", "long"), ("area", "long", "area", "long"), ("bedrooms", "long", "bedrooms", "long"), ("bathrooms", "long", "bathrooms", "long"), ("stories", "long", "stories", "long"), ("mainroad", "string", "mainroad", "string"), ("guestroom", "string", "guestroom", "string"), ("basement", "string", "basement", "string"), ("hotwaterheating", "string", "hotwaterheating", "string"), ("airconditioning", "string", "airconditioning", "string"), ("parking", "long", "parking", "long"), ("prefarea", "string", "prefarea", "string"), ("furnishingstatus", "string", "furnishingstatus", "string")], transformation_ctx = "applymapping1")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["price", "area", "bedrooms", "bathrooms", "stories", "mainroad", "guestroom", "basement", "hotwaterheating", "airconditioning", "parking", "prefarea", "furnishingstatus"], transformation_ctx = "selectfields2")

resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "etl_virtual_database", table_name = "input_folder", transformation_ctx = "resolvechoice3")

datasink4 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice3, database = "etl_virtual_database", table_name = "input_folder", transformation_ctx = "datasink4")
job.commit()