# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6b9edcf9-3110-4a55-a20f-41e46a56a503",
# META       "default_lakehouse_name": "Fund1_Fundraising_SL",
# META       "default_lakehouse_workspace_id": "14d00f5c-2c04-4897-ba2b-221c7834ad8b",
# META       "known_lakehouses": [
# META         {
# META           "id": "6b9edcf9-3110-4a55-a20f-41e46a56a503"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run Fund1_Fundraising_Config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "editable": false
# META }

# MARKDOWN ********************

# # Default configuration

# MARKDOWN ********************

# ## Cofiguration

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
import logging

# ------------------------------------------------------------
# Configuration Data
# ------------------------------------------------------------
configuration_data = [
    ("e5d8f734-b17f-4cc8-b8c2-fc9738add78f", "Currency", "USD"),
    ("d0da94c0-c22c-46f8-a958-0f03cff6a69f", "CurrencySymbol", "$"),
    ("aec9bc8e-d40d-45c6-8def-46eb088b250b", "FiscalYearStartMonth", "7"),
    ("5cb4d467-ffd6-4370-a8c5-c1e5e802fd46", "IsCurrencySymbolPrefix", "true")
]

# Create DataFrame with schema and timestamps
df_configuration = spark.createDataFrame(
    [Row(ConfigurationId=id, Name=name, Value=value) for id, name, value in configuration_data]
).withColumn("CreatedDate", current_timestamp()) \
 .withColumn("ModifiedDate", current_timestamp())

# Register temp view and execute MERGE
df_configuration.createOrReplaceTempView("default_configuration")

# MERGE values
configuration_table_name = get_full_table_name(silver_lakehouse_name, "Configuration")
spark.sql(f"""
    MERGE INTO {configuration_table_name} AS target
    USING default_configuration AS source
      ON target.Name = source.Name
    WHEN NOT MATCHED THEN
      INSERT (
        ConfigurationId,
        CreatedDate,
        ModifiedDate,
        Name,
        Value
      )
      VALUES (
        source.ConfigurationId,
        source.CreatedDate,
        source.ModifiedDate,
        source.Name,
        source.Value
      )
""")

# Clean up temporary view
spark.sql("DROP VIEW IF EXISTS default_configuration")

logging.info("✅ Default Configuration created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Channels

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
import logging

# ------------------------------------------------------------
# Channel Data
# ------------------------------------------------------------
channel_names = [
    ("Email",          "4ba4a473-aeb9-4743-b4b5-1a2ec92f2323"),
    ("Social Media",   "a13b7e00-c20d-448b-b056-4b38d3e4efc4"),
    ("Direct Mail",    "5d79d9d1-35ec-407b-bb7c-c108dc463f00"),
    ("Phone Call",     "ce8121cb-ad89-4dd6-8dd2-c6897c99bf57"),
]

# Create DataFrame with schema and timestamps
df_channel = spark.createDataFrame(
    [Row(ChannelId=id, Name=name) for name, id in channel_names]
).withColumn("CreatedDate", current_timestamp()) \
 .withColumn("ModifiedDate", current_timestamp())

# Register temp view and execute MERGE
df_channel.createOrReplaceTempView("default_channel")

# MERGE values
channel_table_name = get_full_table_name(silver_lakehouse_name, "Channel")
spark.sql(f"""
    MERGE INTO {channel_table_name} AS target
    USING default_channel AS source
      ON target.Name = source.Name
    WHEN NOT MATCHED THEN
      INSERT (
        ChannelId,
        CreatedDate,
        ModifiedDate,
        Name
      )
      VALUES (
        source.ChannelId,
        source.CreatedDate,
        source.ModifiedDate,
        source.Name
      )
""")

# Clean up temporary view
spark.sql("DROP VIEW IF EXISTS default_channel")

logging.info("✅ Default Channels created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Default segmentation

# MARKDOWN ********************

# ## Constituent Segment Type

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
import logging

# ------------------------------------------------------------
# ConstituentSegmentType Data
# ------------------------------------------------------------
segment_type_data = [
    ("000550c9-e37e-4750-a7b3-0720bfdd8755", "Gift Recurrance"),
    ("64502acd-04d7-43ce-9fc2-8fe469bbe80b", "Lifetime Giving Range"),
    ("89da35f8-d031-491b-be2b-00e42a2a58f5", "Gift Significance"),
    ("9ea4b1dd-08cb-42b1-ad90-ab452f21b11f", "Age Range")
]

# Create DataFrame with schema and timestamps
df_segment_type = spark.createDataFrame(
    [Row(ConstituentSegmentTypeId=id, Name=name) for id, name in segment_type_data]
).withColumn("CreatedDate", current_timestamp()) \
 .withColumn("ModifiedDate", current_timestamp())

# Register temp view and execute MERGE
df_segment_type.createOrReplaceTempView("default_segment_types")

# MERGE values
segment_type_table_name = get_full_table_name(silver_lakehouse_name, "ConstituentSegmentType")
spark.sql(f"""
    MERGE INTO {segment_type_table_name} AS target
    USING default_segment_types AS source
      ON target.Name = source.Name
    WHEN NOT MATCHED THEN
      INSERT (
        ConstituentSegmentTypeId,
        CreatedDate,
        ModifiedDate,
        Name
      )
      VALUES (
        source.ConstituentSegmentTypeId,
        source.CreatedDate,
        source.ModifiedDate,
        source.Name
      )
""")

# Clean up temporary view
spark.sql("DROP VIEW IF EXISTS default_segment_types")

logging.info("✅ Default ConstituentSegmentTypes created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constituent Segment

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
import logging

# ------------------------------------------------------------
# ConstituentSegment Data
# ------------------------------------------------------------

gift_recurrence_segments = [
    ("f963f3d1-92a2-48f5-8000-a6a73687025f", "Prospect", 1),
    ("f61288ed-b087-40cf-a9b9-06714a664f23", "New Donor", 2),
    ("dc43a4c9-5942-4a86-9f1a-4ab7cec6f7c8", "LYBNT FY", 3),
    ("2de18bf7-a352-4540-a238-fdfe97177a11", "LYBNT CY", 4),
    ("2c87a98f-f72a-42b2-a2df-8c77741f0181", "LYBNT T12M", 5),
    ("f9eb97e9-9210-435a-ad2d-82c771907102", "Active", 6),
    ("810eb5c3-a898-4bfe-bc2d-67e7021deef9", "Recurring Donor", 7),
    ("2b1280dc-574b-40b0-ad10-dc2b8e9cc6aa", "Lapsed Donor", 8)
]

lifetime_giving_range_segments = [
    ("6584d841-17c2-4720-853d-719d5bfffea2", "<$250", 101),
    ("87e6e616-0e60-495d-af24-0ca869c51f78", "$250–$999", 102),
    ("122aa8f6-1081-47d7-bd1b-7241aa5784d7", "$1,000–$4,999", 103),
    ("8df6054a-1c8f-4db0-8208-742ca294bcd1", "$5,000–$9,999", 104),
    ("e6921693-e1ce-404e-9572-bd3316acd9a0", "$10,000–$24,999", 105),
    ("ef2e3c01-63e6-4fc7-b85d-bbe56cd4efeb", "$25,000–$49,999", 106),
    ("dcb11147-29de-46c6-a242-01f778cb9327", "$50,000–$99,999", 107),
    ("9dee3abd-3fc7-4f34-9eef-6f370f9c7e73", "$100,000–$499,999", 108),
    ("41c3481e-3acf-4861-a6bc-469eb94b5c39", "$500,000–$999,999", 109),
    ("a061b8b9-2aee-484a-92ba-0cef6f153e77", "$1,000,000+", 110)
]

gift_significance_segments = []

age_range_segments = [
    ("54da67f7-3db1-4337-8051-9241467f93ef", "<18", 301),
    ("69401d7c-9505-4c60-8879-3ca11d9dd482", "18-24", 302),
    ("5960fba8-0b78-420f-8730-e5c78a8d9c51", "25-34", 303),
    ("c806d565-6390-4c88-8c0e-5396dff48794", "35-44", 304),
    ("4bc89ab7-c3bc-4063-995b-ca25cf3819d4", "45-54", 305),
    ("4c372788-6417-42a1-b079-0aed5dc05f4a", "55-64", 306),
    ("7014a1e8-07cb-4b94-867a-d156b0a14938", "65-74", 307),
    ("3a7ee966-bda0-46be-a4cd-5c109d548b84", ">75", 308),
    ("6d421a90-b50a-4dac-b230-2dbebb4e3623", "Unclassified", None)
]

# Define segment types and their corresponding segments
segment_types_data = {
    "Gift Recurrance": gift_recurrence_segments,
    "Lifetime Giving Range": lifetime_giving_range_segments,
    "Gift Significance": gift_significance_segments,
    "Age Range": age_range_segments
}

# Create segment data with type names more efficiently
segment_data = [
    Row(
        ConstituentSegmentId=segment_id,
        ConstituentSegmentTypeName=type_name,
        Name=segment_name,
        Order=sort_order
    )
    for type_name, segments in segment_types_data.items()
    for segment in segments
    for segment_id, segment_name, sort_order in [segment if len(segment) == 3 else (*segment, None)]
]

# Create DataFrame with timestamps in one operation
df_segment = spark.createDataFrame(segment_data) \
    .withColumn("CreatedDate", current_timestamp()) \
    .withColumn("ModifiedDate", current_timestamp())

# Register temp view and join in one operation
df_segment.createOrReplaceTempView("default_segments_raw")

segment_type_table_name = get_full_table_name(silver_lakehouse_name, "ConstituentSegmentType")
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW default_segments AS
    SELECT 
        s.ConstituentSegmentId,
        st.ConstituentSegmentTypeId,
        s.CreatedDate,
        s.ModifiedDate,
        s.Name,
        s.Order
    FROM default_segments_raw s
    INNER JOIN {segment_type_table_name} st 
        ON s.ConstituentSegmentTypeName = st.Name
""")

# MERGE values
full_table_name = get_full_table_name(silver_lakehouse_name, "ConstituentSegment")
spark.sql(f"""
    MERGE INTO {full_table_name} AS target
    USING default_segments AS source
      ON target.Name = source.Name 
         AND target.ConstituentSegmentTypeId = source.ConstituentSegmentTypeId
    WHEN NOT MATCHED THEN
      INSERT (
        ConstituentSegmentId,
        ConstituentSegmentTypeId,
        CreatedDate,
        ModifiedDate,
        Name,
        Order
      )
      VALUES (
        source.ConstituentSegmentId,
        source.ConstituentSegmentTypeId,
        source.CreatedDate,
        source.ModifiedDate,
        source.Name,
        source.Order
      )
""")

# Clean up temporary views
spark.sql("DROP VIEW IF EXISTS default_segments")
spark.sql("DROP VIEW IF EXISTS default_segments_raw")

logging.info("✅ Default ConstituentSegments created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
