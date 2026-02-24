# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ac2bc0cd-6c00-4cab-a361-3ade98085130",
# META       "default_lakehouse_name": "Fund1_Fundraising_GD",
# META       "default_lakehouse_workspace_id": "14d00f5c-2c04-4897-ba2b-221c7834ad8b",
# META       "known_lakehouses": [
# META         {
# META           "id": "ac2bc0cd-6c00-4cab-a361-3ade98085130"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Config

# CELL ********************

%run Fund1_Fundraising_Config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constituent segment types

# CELL ********************

segment_type_df = get_gold_table("DimConstituentSegmentType")
display(segment_type_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constituent segments

# CELL ********************

segment_df = get_gold_table("DimConstituentSegment")
ordered_df = segment_df.orderBy("TypeKey", "ConstituentSegmentKey")
display(ordered_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constituents - Datamarkt

# CELL ********************

const_df = get_gold_table("dm_Constituent")
display(const_df.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Age ranges

# CELL ********************

from pyspark.sql.functions import col, when, lit, xxhash64
from delta.tables import DeltaTable

# Dynamically find ConstituentSegmentTypeKey for "Age Range"
age_range_type_row = get_gold_table("DimConstituentSegmentType") \
    .filter(col("ConstituentSegmentType") == "Age Range") \
    .select("ConstituentSegmentTypeKey") \
    .first()

if age_range_type_row is None:
    raise ValueError("❌ Segment type 'Age Range' not found.")

age_range_type_key = age_range_type_row["ConstituentSegmentTypeKey"]

# Load required tables
constituent_df = get_gold_table("dm_Constituent").select("ConstituentKey", "ConstituentName", "Age")

segment_df = get_gold_table("DimConstituentSegment") \
    .filter(col("TypeKey") == age_range_type_key) \
    .select("ConstituentSegmentKey", "ConstituentSegmentName", "TypeKey")

segment_type_df = get_gold_table("DimConstituentSegmentType")

# Add age boundaries based on segment name
segment_df = segment_df.withColumn("AgeMin", when(col("ConstituentSegmentName") == "<18", lit(0))
    .when(col("ConstituentSegmentName") == "18-24", lit(18))
    .when(col("ConstituentSegmentName") == "25-34", lit(25))
    .when(col("ConstituentSegmentName") == "35-44", lit(35))
    .when(col("ConstituentSegmentName") == "45-54", lit(45))
    .when(col("ConstituentSegmentName") == "55-64", lit(55))
    .when(col("ConstituentSegmentName") == "65-74", lit(65))
    .when(col("ConstituentSegmentName") == ">=75", lit(75))
)

segment_df = segment_df.withColumn("AgeMax", when(col("ConstituentSegmentName") == "<18", lit(17))
    .when(col("ConstituentSegmentName") == "18-24", lit(24))
    .when(col("ConstituentSegmentName") == "25-34", lit(34))
    .when(col("ConstituentSegmentName") == "35-44", lit(44))
    .when(col("ConstituentSegmentName") == "45-54", lit(54))
    .when(col("ConstituentSegmentName") == "55-64", lit(64))
    .when(col("ConstituentSegmentName") == "65-74", lit(74))
    .when(col("ConstituentSegmentName") == ">75", lit(200))
)

# Join constituents with the corresponding age segment
classified_df = constituent_df.join(segment_df,
    (col("Age").isNotNull()) &
    (col("Age") >= col("AgeMin")) & (col("Age") <= col("AgeMax")),
    "left"
)

# Get "Unclassified" segment key dynamically
unclassified_row = get_gold_table("DimConstituentSegment") \
    .filter((col("TypeKey") == age_range_type_key) & (col("ConstituentSegmentName") == "Unclassified")) \
    .select("ConstituentSegmentKey").first()

if unclassified_row is None:
    raise ValueError("❌ Segment 'Unclassified' not found in Age Range segments.")

unclassified_key = unclassified_row["ConstituentSegmentKey"]

# Use matched segment or fallback to "Unclassified"
classified_df = classified_df.withColumn("FinalSegmentKey",
    when(col("ConstituentSegmentKey").isNotNull(), col("ConstituentSegmentKey"))
    .otherwise(lit(unclassified_key))
)

# Prepare bridge table for inserting new records
new_bridge_df = classified_df.select(
    col("ConstituentKey"),
    col("FinalSegmentKey").alias("ConstituentSegmentKey")
).withColumn(
    "ConstituentSegmentBridgeKey",
    xxhash64(col("ConstituentKey"), col("ConstituentSegmentKey")).cast("bigint")
)


# Get all keys for "Age Range" segments to be removed before insert
segment_keys_to_remove = get_gold_table("DimConstituentSegment") \
    .filter(col("TypeKey") == age_range_type_key) \
    .select("ConstituentSegmentKey").rdd.flatMap(lambda x: x).collect()

print("📌 Removing existing Age Range segments with keys:", segment_keys_to_remove)

# Remove old values
bridge_table = DeltaTable.forName(spark, f"{gold_lakehouse_name}.DimConstituentSegmentBridge")
bridge_table.delete(f"ConstituentSegmentKey IN ({','.join(map(str, segment_keys_to_remove))})  AND ConstituentSegmentMappingId IS NULL")
new_bridge_df.write.format("delta").mode("append").saveAsTable(f"{gold_lakehouse_name}.DimConstituentSegmentBridge")

print("✅ Segment update complete.")

# Debug output – join to get human-readable segment name and type
output_df = classified_df \
    .join(
        segment_df.select(
            col("ConstituentSegmentKey").alias("segKey"),
            col("TypeKey").alias("SegmentTypeKey"),
            col("ConstituentSegmentName")
        ).alias("seg"),
        classified_df["FinalSegmentKey"] == col("seg.segKey"),
        "left"
    ) \
    .join(
        segment_type_df.select(
            col("ConstituentSegmentTypeKey"),
            col("ConstituentSegmentType")
        ).alias("stype"),
        col("seg.SegmentTypeKey") == col("stype.ConstituentSegmentTypeKey"),
        "left"
    ) \
    .select(
        col("ConstituentKey"),
        col("ConstituentName"),
        col("Age"),
        col("FinalSegmentKey").alias("SegmentKey"),
        col("stype.ConstituentSegmentType").alias("SegmentType"),
        col("seg.ConstituentSegmentName").alias("SegmentName")
    ) \
    .orderBy("ConstituentKey")

# Show final output
display(output_df.limit(200))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Giving ranges

# CELL ********************

from pyspark.sql.functions import col, when, lit, xxhash64
from delta.tables import DeltaTable

# Load input tables
constituent_df = get_gold_table("dm_Constituent").select("ConstituentKey", "ConstituentName", "LifetimeDonationAmount")

# Replace nulls with 0
constituent_df = constituent_df.withColumn(
    "LifetimeDonationAmount",
    coalesce(col("LifetimeDonationAmount"), lit(0))
)

segment_df_raw = get_gold_table("DimConstituentSegment").alias("segment")
segment_type_df = get_gold_table("DimConstituentSegmentType").alias("stype")

# Select only Lifetime Giving Range segments
segment_df = segment_df_raw.join(
    segment_type_df,
    segment_df_raw["TypeKey"] == segment_type_df["ConstituentSegmentTypeKey"],
    "inner"
).filter(col("ConstituentSegmentType") == "Lifetime Giving Range") \
 .select(
     col("segment.ConstituentSegmentKey"),
     col("segment.ConstituentSegmentName"),
     col("segment.TypeKey")
)

# Assign numeric ranges
segment_df = segment_df.withColumn("AmountMin", when(col("ConstituentSegmentName") == "<$250", 0)
    .when(col("ConstituentSegmentName") == "$250–$999", 250)
    .when(col("ConstituentSegmentName") == "$1,000–$4,999", 1000)
    .when(col("ConstituentSegmentName") == "$5,000–$9,999", 5000)
    .when(col("ConstituentSegmentName") == "$10,000–$24,999", 10000)
    .when(col("ConstituentSegmentName") == "$25,000–$49,999", 25000)
    .when(col("ConstituentSegmentName") == "$50,000–$99,999", 50000)
    .when(col("ConstituentSegmentName") == "$100,000–$499,999", 100000)
    .when(col("ConstituentSegmentName") == "$500,000–$999,999", 500000)
    .when(col("ConstituentSegmentName") == "$1,000,000+", 1000000))

segment_df = segment_df.withColumn("AmountMax", when(col("ConstituentSegmentName") == "<$250", 249)
    .when(col("ConstituentSegmentName") == "$250–$999", 999)
    .when(col("ConstituentSegmentName") == "$1,000–$4,999", 4999)
    .when(col("ConstituentSegmentName") == "$5,000–$9,999", 9999)
    .when(col("ConstituentSegmentName") == "$10,000–$24,999", 24999)
    .when(col("ConstituentSegmentName") == "$25,000–$49,999", 49999)
    .when(col("ConstituentSegmentName") == "$50,000–$99,999", 99999)
    .when(col("ConstituentSegmentName") == "$100,000–$499,999", 499999)
    .when(col("ConstituentSegmentName") == "$500,000–$999,999", 999999)
    .when(col("ConstituentSegmentName") == "$1,000,000+", 999999999))

# Join with constituents
classified_df = constituent_df.join(
    segment_df,
    (col("LifetimeDonationAmount").isNotNull()) &
    (col("LifetimeDonationAmount") >= col("AmountMin")) &
    (col("LifetimeDonationAmount") <= col("AmountMax")),
    "left"
)

# Prepare final output
new_bridge_df = classified_df.select(
    col("ConstituentKey"),
    col("ConstituentSegmentKey")
).withColumn(
    "ConstituentSegmentBridgeKey",
    xxhash64(col("ConstituentKey"), col("ConstituentSegmentKey")).cast("bigint")
)

# Remove old values
segment_keys_to_remove = segment_df.select("ConstituentSegmentKey").rdd.flatMap(lambda x: x).collect()
bridge_table = DeltaTable.forName(spark, f"{gold_lakehouse_name}.DimConstituentSegmentBridge")
bridge_table.delete(f"ConstituentSegmentKey IN ({','.join(map(str, segment_keys_to_remove))}) AND ConstituentSegmentMappingId IS NULL")

# Insert new mappings
new_bridge_df.write.format("delta").mode("append").saveAsTable(f"{gold_lakehouse_name}.DimConstituentSegmentBridge")

# Display debug info
display(classified_df.select(
    "ConstituentKey", "ConstituentName", "LifetimeDonationAmount",
    "ConstituentSegmentKey", "ConstituentSegmentName"
).orderBy("ConstituentKey").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# # Gift Recurrance

# CELL ********************

from pyspark.sql.functions import col, lit, when, xxhash64, max as spark_max, min as spark_min, coalesce
from pyspark.sql.types import LongType
from delta.tables import DeltaTable
from datetime import datetime, timedelta
from functools import reduce
from pyspark.sql import DataFrame

# Load base tables
config_df = get_gold_table("Configuration")
constituent_df = get_gold_table("dm_Constituent").select("ConstituentKey", "IsNewDonor", "LastDonationDateKey")
donation_df = get_gold_table("FactDonation").select("ConstituentKey", "DonationDateKey", "IsReccuring")
date_df = get_gold_table("DimDate").select("DateKey", "Date", "Year", "FiscalYear")
segment_df = get_gold_table("DimConstituentSegment").alias("seg")
segment_type_df = get_gold_table("DimConstituentSegmentType").alias("stype")

# Get Gift Recurrance segment type key
gift_type_key = segment_type_df.filter(col("ConstituentSegmentType") == "Gift Recurrance") \
    .select("ConstituentSegmentTypeKey").first()["ConstituentSegmentTypeKey"]

# Filter only Gift Recurrance segments
segment_df = segment_df.filter(col("seg.TypeKey") == lit(gift_type_key)) \
    .select("ConstituentSegmentKey", "ConstituentSegmentName")

segment_key_map = {
    row["ConstituentSegmentName"].strip().lower(): row["ConstituentSegmentKey"]
    for row in segment_df.collect()
}

# Date setup
fiscal_start_month = int(config_df.filter(col("Name") == "FiscalYearStartMonth").select("Value").first()["Value"])
today = datetime.today()
one_year_ago = today - timedelta(days=365)
two_years_ago = today - timedelta(days=730)
current_year = today.year
previous_year = current_year - 1

# Determine fiscal year
fiscal_today = date_df.filter(col("Date") == lit(today.date())).select("FiscalYear").first()
fiscal_year = fiscal_today["FiscalYear"] if fiscal_today else current_year
fiscal_prev_year = fiscal_year - 1

# Join donations with DimDate
joined_donations = donation_df.join(date_df, donation_df.DonationDateKey == date_df.DateKey, "left") \
    .select("ConstituentKey", "DonationDateKey", "IsReccuring", "Year", "FiscalYear", "Date")

# Aggregate metrics
agg_df = joined_donations.groupBy("ConstituentKey").agg(
    spark_max(when(col("Date") >= lit(one_year_ago), lit(1))).alias("HasRecentDonation"),
    spark_max(when(col("Date") >= lit(two_years_ago), lit(1))).alias("HasDonation24m"),
    spark_min(when(col("Date") < lit(two_years_ago), lit(1))).alias("HasOldDonation"),
    spark_max(when(col("IsReccuring") == True, lit(1))).alias("HasRecurring"),
    spark_max(when(col("Year") == previous_year, lit(1))).alias("HasPrevYearCY"),
    spark_max(when(col("Year") == current_year, lit(1))).alias("HasCurrYearCY"),
    spark_max(when(col("FiscalYear") == fiscal_prev_year, lit(1))).alias("HasPrevYearFY"),
    spark_max(when(col("FiscalYear") == fiscal_year, lit(1))).alias("HasCurrYearFY")
)

# Classify into segments
classified_df = constituent_df.join(agg_df, "ConstituentKey", "left")

multi_segment_df = classified_df.select(
    "ConstituentKey", "IsNewDonor", "LastDonationDateKey",
    "HasRecentDonation", "HasRecurring", "HasPrevYearCY", "HasCurrYearCY",
    "HasPrevYearFY", "HasCurrYearFY", "HasDonation24m", "HasOldDonation"
)

segment_rows = []

if "new donor" in segment_key_map:
    segment_rows.append(multi_segment_df.filter(col("IsNewDonor") == True)
                        .withColumn("ConstituentSegmentKey", lit(segment_key_map["new donor"])))

if "recurring donor" in segment_key_map:
    segment_rows.append(multi_segment_df.filter(col("HasRecurring") == 1)
                        .withColumn("ConstituentSegmentKey", lit(segment_key_map["recurring donor"])))

if "active" in segment_key_map:
    segment_rows.append(multi_segment_df.filter(col("HasRecentDonation") == 1)
                        .withColumn("ConstituentSegmentKey", lit(segment_key_map["active"])))

if "lybnt t12m" in segment_key_map:
    segment_rows.append(multi_segment_df.filter((col("HasPrevYearCY") == 1) & (coalesce(col("HasRecentDonation"), lit(0)) != 1))
                        .withColumn("ConstituentSegmentKey", lit(segment_key_map["lybnt t12m"])))

if "lybnt cy" in segment_key_map:
    segment_rows.append(multi_segment_df.filter((col("HasPrevYearCY") == 1) & (coalesce(col("HasCurrYearCY"), lit(0)) != 1))
                        .withColumn("ConstituentSegmentKey", lit(segment_key_map["lybnt cy"])))

if "lybnt fy" in segment_key_map:
    segment_rows.append(multi_segment_df.filter((col("HasPrevYearFY") == 1) & (coalesce(col("HasCurrYearFY"), lit(0)) != 1))
                        .withColumn("ConstituentSegmentKey", lit(segment_key_map["lybnt fy"])))

if "lapsed donor" in segment_key_map:
    segment_rows.append(multi_segment_df.filter((coalesce(col("HasDonation24m"), lit(0)) != 1) & (col("HasOldDonation") == 1))
                        .withColumn("ConstituentSegmentKey", lit(segment_key_map["lapsed donor"])))

if "prospect" in segment_key_map:
    segment_rows.append(multi_segment_df.filter(col("LastDonationDateKey").isNull())
                        .withColumn("ConstituentSegmentKey", lit(segment_key_map["prospect"])))

# Combine all rows
union_df = reduce(DataFrame.unionAll, segment_rows)

# Final bridge DF
new_bridge_df = union_df.select("ConstituentKey", "ConstituentSegmentKey") \
    .dropDuplicates() \
    .withColumn("ConstituentSegmentBridgeKey", xxhash64(col("ConstituentKey"), col("ConstituentSegmentKey")).cast("bigint")) \
    .withColumn("ConstituentKey", col("ConstituentKey").cast(LongType())) \
    .withColumn("ConstituentSegmentKey", col("ConstituentSegmentKey").cast(LongType())) \
    .withColumn("ConstituentSegmentBridgeKey", col("ConstituentSegmentBridgeKey").cast(LongType()))

# Remove old values from bridge
bridge_table = DeltaTable.forName(spark, f"{gold_lakehouse_name}.DimConstituentSegmentBridge")
bridge_table.delete(f"ConstituentSegmentKey IN ({','.join(map(str, segment_key_map.values()))}) AND ConstituentSegmentMappingId IS NULL")

# Insert updated bridge records
new_bridge_df.write \
    .format("delta") \
    .mode("append") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{gold_lakehouse_name}.DimConstituentSegmentBridge")

# Optional debug preview
constituent_named_df = get_gold_table("dm_Constituent").select("ConstituentKey", "ConstituentName")
segment_type_df = get_gold_table("DimConstituentSegmentType")

output_df = new_bridge_df \
    .join(segment_df, "ConstituentSegmentKey", "left") \
    .join(segment_type_df, segment_type_df["ConstituentSegmentTypeKey"] == gift_type_key, "left") \
    .join(constituent_named_df, "ConstituentKey", "left") \
    .select("ConstituentKey", "ConstituentName", "ConstituentSegmentKey", "ConstituentSegmentType", "ConstituentSegmentName")

display(output_df.orderBy("ConstituentKey").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test Gift Recurrance

# CELL ********************

def show_donations_by_segment(segment_name: str, donation_columns=None):
    """
    Display donations for the first constituent that belongs to the given segment name.
    
    :param segment_name: Name of the segment in DimConstituentSegment (e.g., "Recurring Donor")
    :param donation_columns: Optional list of columns to select from FactDonation
    """
    if donation_columns is None:
        donation_columns = [
            "Amount",
            "ConstituentKey",
            "DonationDateKey",
            "DonationId",
            "DonationKey",
            "DonationName",
            "IsReccuring",
            "SourceKey"
        ]

    # Find segment key
    segment_key_row = get_gold_table("DimConstituentSegment") \
        .filter(col("ConstituentSegmentName") == segment_name) \
        .select("ConstituentSegmentKey") \
        .first()

    if segment_key_row is None:
        print(f"⚠️ Segment '{segment_name}' not found.")
        return

    segment_key = segment_key_row["ConstituentSegmentKey"]

    # Find first constituent in this segment
    constituent_row = get_gold_table("DimConstituentSegmentBridge") \
        .filter(col("ConstituentSegmentKey") == segment_key) \
        .select("ConstituentKey") \
        .first()

    if constituent_row is None:
        print(f"⚠️ No constituent found in segment '{segment_name}'.")
        return

    constituent_key = constituent_row["ConstituentKey"]

    # Display donations for this constituent
    donation_debug_df = get_gold_table("FactDonation") \
        .filter(col("ConstituentKey") == constituent_key) \
        .select(*donation_columns)

    print(f"✅ Showing donations for ConstituentKey = {constituent_key} in segment '{segment_name}':")
    display(donation_debug_df)

    const_df = get_gold_table("dm_Constituent").filter(col("ConstituentKey") == constituent_key)
    display(const_df.limit(1))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test: Recurring Donor

# MARKDOWN ********************

# has at least one recurring transaction

# CELL ********************

show_donations_by_segment("Recurring Donor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test: New Donor

# MARKDOWN ********************

# IsNewDonor == TRUE (first gift within last 12 months)

# CELL ********************

show_donations_by_segment("New Donor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test: Lapsed Donor

# MARKDOWN ********************

# no transaction in last 24 months but there are older ones

# CELL ********************

show_donations_by_segment("Lapsed Donor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test: Active

# MARKDOWN ********************

# has transaction in last 12 months 

# CELL ********************

show_donations_by_segment("Active")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test: LYBNT T12M 

# CELL ********************

show_donations_by_segment("LYBNT T12M")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test: LYBNT CY

# CELL ********************

show_donations_by_segment("LYBNT CY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test: LYBNT FY

# CELL ********************

show_donations_by_segment("LYBNT FY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Test: Prospect

# MARKDOWN ********************

# no donations

# CELL ********************

show_donations_by_segment("Prospect")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
