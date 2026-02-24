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

# MARKDOWN ********************

# # Config

# CELL ********************

%run Fund1_Fundraising_Config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "editable": false
# META }

# MARKDOWN ********************

# # Enrichment

# MARKDOWN ********************

# ## Generic tables

# MARKDOWN ********************

# ### Build: Configuration

# CELL ********************

configurationTable = CdfTable(
    source_table_name="Configuration",
    source_primary_key="ConfigurationId",
    target_table_name="Configuration",
    columns=[
        "ConfigurationId", "Name", "Value", "CreatedDate", "ModifiedDate", "SourceSystemId", "SourceId"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.Configuration AS target
    USING (
        SELECT 
            ConfigurationId,
            Name,
            Value
        FROM latestSnapshot_Configuration
    ) AS source
    ON target.ConfigurationId = source.ConfigurationId
    WHEN MATCHED THEN UPDATE SET
        Name = source.Name,
        Value = source.Value
    WHEN NOT MATCHED THEN INSERT (
        ConfigurationId, Name, Value
    ) VALUES (
        source.ConfigurationId, source.Name, source.Value
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=None
)

ProcessCdfTable(configurationTable)

logging.info(f"✅ Configuration processed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimDate

# CELL ********************

from pyspark.sql.functions import (
    sequence, explode, to_date, year, month, date_format, quarter,
    dayofweek, dayofmonth, weekofyear, when, lit, coalesce, expr, col
)
from pyspark.sql.types import LongType
import holidays

# Define target table using your gold lakehouse variable
target_table = f"{gold_lakehouse_name}.DimDate"

if table_exists(target_table) and spark.table(target_table).count() > 0:
    logging.info("✅ DimDate already populated – skipping rebuild.")
else:
    logging.info("⏳ Building DimDate table...")

    # Create US holidays list for years 1900–2050
    us_holidays = list(holidays.US(years=range(1900, 2051)).keys())
    holiday_df = spark.createDataFrame([(d,) for d in us_holidays], ["Date"]) \
                      .withColumn("IsHoliday", lit(True))

    # Generate and enrich date range
    date_df = (
        spark.range(1)  # must produce at least one row
            .select(explode(sequence(
                to_date(lit("1900-01-01")),
                to_date(lit("2050-12-31")),
                expr("interval 1 day")
            )).alias("Date"))
            .withColumn("DateKey", date_format("Date", "yyyyMMdd").cast(LongType()))
            .withColumn("Year", year("Date"))
            .withColumn("Month", month("Date"))
            .withColumn("MonthName", date_format("Date", "MMMM"))
            .withColumn("MonthNameShort", date_format("Date", "MMM"))
            .withColumn("Day", dayofmonth("Date"))
            .withColumn("DayOfWeek", dayofweek("Date"))
            .withColumn("DayName", date_format("Date", "EEEE"))
            .withColumn("WeekOfYear", weekofyear("Date"))
            .withColumn("Quarter", quarter("Date"))
            .withColumn("FiscalYear", year("Date"))  # Adjust if you use a fiscal calendar
            .withColumn("FiscalQuarter", quarter("Date"))
            .withColumn("IsWeekend", dayofweek("Date").isin(1, 7).cast("boolean"))
    )

    # Join with holiday list (US)
    final_df = (
        date_df.join(holiday_df.hint("broadcast"), on="Date", how="left")
               .withColumn("IsHoliday", coalesce("IsHoliday", lit(False)))
               .select(
                   "DateKey", "Date", "Year", "Month", "MonthName", "MonthNameShort",
                   "Day", "DayOfWeek", "DayName", "WeekOfYear", "Quarter",
                   "FiscalYear", "FiscalQuarter", "IsWeekend", "IsHoliday"
               )
    )

    # Write to Delta table (no schema overwrite)
    final_df.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable(target_table)

    logging.info(f"✅ DimDate written with {final_df.count()} rows.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimSource

# CELL ********************

from pyspark.sql.functions import col, xxhash64
from pyspark.sql import DataFrame

def EnrichDimSource(df: DataFrame) -> DataFrame:

    new_df = (
        df
        .select("SourceId", "Name")
        .withColumn("SourceKey", xxhash64(col("SourceId")).cast("bigint"))
        .select(
            "SourceKey",
            "SourceId",
            col("Name").alias("SourceName")
        )
    )

    logging.info(f"✅ DimSource processing {new_df.count()} rows.")
    return new_df

dimSourceTable = CdfTable(
    source_table_name="Source",
    source_primary_key="SourceId",
    target_table_name="DimSource",
    columns=["SourceId", "Name", "CreatedDate", "ModifiedDate"],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimSource AS target
    USING latestSnapshot_Source AS source
    ON target.SourceId = source.SourceId
    WHEN MATCHED THEN UPDATE SET
        SourceName = source.SourceName
    WHEN NOT MATCHED THEN INSERT (
        SourceKey, SourceId, SourceName
    ) VALUES (
        source.SourceKey, source.SourceId, source.SourceName
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimSource
)

ProcessCdfTable(dimSourceTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimChannel

# CELL ********************

from pyspark.sql.functions import col, to_date, xxhash64

def EnrichDimChannel(df: DataFrame) -> DataFrame:
    dim_date = get_gold_table("DimDate").select(
        col("Date").alias("DimDate"),
        col("DateKey")
    )

    new_df = (
        df.select("ChannelId", "Name", "Weight", "CreatedDate", "ModifiedDate")
          .withColumn("ChannelKey", xxhash64(col("ChannelId")).cast("bigint"))
          .join(dim_date, to_date("CreatedDate") == col("DimDate"), "left")
          .withColumnRenamed("DateKey", "CreatedDateKey")
          .drop("DimDate")
          .join(dim_date, to_date("ModifiedDate") == col("DimDate"), "left")
          .withColumnRenamed("DateKey", "ModifiedDateKey")
          .drop("DimDate")
          .select(
              "ChannelKey",
              "ChannelId",
              col("Name").alias("ChannelName"),
              "Weight",
              "CreatedDateKey",
              "ModifiedDateKey"
          )
    )

    logging.info(f"✅ DimChannel processing {new_df.count()} rows.")

    return new_df

dimChannelTable = CdfTable(
    source_table_name="Channel",
    source_primary_key="ChannelId",
    target_table_name="DimChannel",
    columns=["ChannelId", "Name", "Weight", "CreatedDate", "ModifiedDate"],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimChannel AS target
    USING latestSnapshot_Channel AS source
    ON target.ChannelId = source.ChannelId
    WHEN MATCHED THEN UPDATE SET
        ChannelName = source.ChannelName,
        Weight = source.Weight,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey
    WHEN NOT MATCHED THEN INSERT (
        ChannelKey, ChannelId, ChannelName, Weight, CreatedDateKey, ModifiedDateKey
    ) VALUES (
        source.ChannelKey, source.ChannelId, source.ChannelName, source.Weight,
        source.CreatedDateKey, source.ModifiedDateKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimChannel
)

ProcessCdfTable(dimChannelTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: CampaignType

# CELL ********************

from pyspark.sql.functions import col, xxhash64

def EnrichDimCampaignType(df: DataFrame) -> DataFrame:

    new_df = (
        df
        .select("CampaignTypeId", col("Name").alias("CampaignType"))
        .dropna(subset=["CampaignTypeId"])
        .withColumn(
            "CampaignTypeKey",
            xxhash64(col("CampaignTypeId")).cast("bigint")
        )
        .select("CampaignTypeKey", "CampaignTypeId", "CampaignType")
    )

    logging.info(f"✅ DimCampaignType processing {new_df.count()} rows.")
    return new_df

dimCampaignTypeTable = CdfTable(
    source_table_name="CampaignType",
    source_primary_key="CampaignTypeId",
    target_table_name="DimCampaignType",
    columns=[
        "CampaignTypeId", "Name", "CreatedDate", "ModifiedDate",
        "SourceId", "SourceSystemId"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimCampaignType AS target
    USING latestSnapshot_CampaignType AS source
    ON target.CampaignTypeId = source.CampaignTypeId
    WHEN MATCHED THEN UPDATE SET
        CampaignType = source.CampaignType
    WHEN NOT MATCHED THEN INSERT (
        CampaignTypeKey, CampaignTypeId, CampaignType
    ) VALUES (
        source.CampaignTypeKey, source.CampaignTypeId, source.CampaignType
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimCampaignType
)

ProcessCdfTable(dimCampaignTypeTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimCampaign

# CELL ********************

from pyspark.sql.functions import col, to_date, xxhash64

def EnrichDimCampaign(df: DataFrame) -> DataFrame:
    # Reference to DimCampaignType in Gold
    campaign_type_df = get_gold_table("DimCampaignType") \
        .select("CampaignTypeId", "CampaignTypeKey") \
        .alias("ct")

    # Reference to DimDate
    dim_date_df = get_gold_table("DimDate") \
        .select(col("Date").alias("DimDate"), col("DateKey"))

    # Alias for base Campaign table
    df = df.alias("c")

    new_df = (
        df.join(campaign_type_df, col("c.CampaignTypeId") == col("ct.CampaignTypeId"), "left")
          .join(dim_date_df.alias("start"), to_date(col("c.StartDate")) == col("start.DimDate"), "left")
          .join(dim_date_df.alias("end"), to_date(col("c.EndDate")) == col("end.DimDate"), "left")
          .join(dim_date_df.alias("created"), to_date(col("c.CreatedDate")) == col("created.DimDate"), "left")
          .join(dim_date_df.alias("modified"), to_date(col("c.ModifiedDate")) == col("modified.DimDate"), "left")
          .select(
              col("c.CampaignId"),
              col("c.Name").alias("CampaignName"),
              col("ct.CampaignTypeKey"),
              col("c.Cost").cast("decimal(18,2)"),
              col("created.DateKey").alias("CreatedDateKey"),
              col("end.DateKey").alias("EndDateKey"),
              col("modified.DateKey").alias("ModifiedDateKey"),
              col("c.SourceSystemId").alias("SourceSysCampaignId"),
              col("start.DateKey").alias("StartDateKey"),
              col("c.Timezone")
          )
          .withColumn(
              "CampaignKey",
              xxhash64(
                  col("CampaignId"), 
                  col("SourceSysCampaignId")
              ).cast("bigint")
          )
          .select(
              "CampaignKey",
              "CampaignId",
              "CampaignName",
              "CampaignTypeKey",
              "Cost",
              "CreatedDateKey",
              "EndDateKey",
              "ModifiedDateKey",
              "SourceSysCampaignId",
              "StartDateKey",
              "Timezone"
          )
    )

    logging.info(f"✅ DimCampaign processing {new_df.count()} rows.")
    return new_df

dimCampaignTable = CdfTable(
    source_table_name="Campaign",
    source_primary_key="CampaignId",
    target_table_name="DimCampaign",
    columns=[
        "CampaignId", "CampaignTypeId", "Cost", "CreatedDate", "EndDate",
        "ModifiedDate", "Name", "SourceId", "SourceSystemId", "StartDate", "Timezone"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimCampaign AS target
    USING latestSnapshot_Campaign AS source
    ON target.CampaignId = source.CampaignId
    WHEN MATCHED THEN UPDATE SET
        CampaignName = source.CampaignName,
        CampaignTypeKey = source.CampaignTypeKey,
        Cost = source.Cost,
        CreatedDateKey = source.CreatedDateKey,
        EndDateKey = source.EndDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        SourceSysCampaignId = source.SourceSysCampaignId,
        StartDateKey = source.StartDateKey,
        Timezone = source.Timezone
    WHEN NOT MATCHED THEN INSERT (
        CampaignKey, CampaignId, CampaignName, CampaignTypeKey, Cost,
        CreatedDateKey, EndDateKey, ModifiedDateKey,
        SourceSysCampaignId, StartDateKey, Timezone
    ) VALUES (
        source.CampaignKey, source.CampaignId, source.CampaignName, source.CampaignTypeKey,
        source.Cost, source.CreatedDateKey, source.EndDateKey, source.ModifiedDateKey,
        source.SourceSysCampaignId, source.StartDateKey, source.Timezone
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimCampaign,
    hard_delete=True
)

ProcessCdfTable(dimCampaignTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimAddress

# CELL ********************

from pyspark.sql.functions import col, xxhash64

def EnrichDimAddress(df: DataFrame) -> DataFrame:
    df = df.alias("a")
    dim_source = get_gold_table("DimSource").select("SourceId", "SourceKey").alias("ds")
    country_df = get_silver_table("Country").select(
        "CountryId", col("Name").alias("CountryName"), "CountryCode"
    ).alias("c")

    new_df = (
        df.join(dim_source, col("a.SourceId") == col("ds.SourceId"), "left")
          .join(country_df, col("a.CountryId") == col("c.CountryId"), "left")
          .withColumn(
              "AddressKey",
              xxhash64(
                  col("a.AddressId"),
                  col("a.SourceSystemId")
              ).cast("bigint")
          )
          .select(
              "AddressKey",
              col("a.AddressId"),
              col("a.CountryId"),
              col("a.SourceSystemId").alias("SourceSysAddressId"),
              col("a.City"),
              col("a.State"),
              col("a.StateCode"),
              col("c.CountryName"),
              col("c.CountryCode"),
              col("a.Region"),
              col("a.ZipCode"),
              col("a.Latitude"),
              col("a.Longitude"),
              col("ds.SourceKey")
          )
    )

    logging.info(f"✅ DimAddress processing {new_df.count()} rows.")

    return new_df

dimAddressTable = CdfTable(
    source_table_name="Address",
    source_primary_key="AddressId",
    target_table_name="DimAddress",
    columns=[
        "AddressId", "CountryId", "SourceId", "SourceSystemId",
        "City", "State", "StateCode", "Region", "ZipCode",
        "Latitude", "Longitude", "CreatedDate", "ModifiedDate"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimAddress AS target
    USING latestSnapshot_Address AS source
    ON target.AddressId = source.AddressId
    WHEN MATCHED THEN UPDATE SET
        CountryId          = source.CountryId,
        SourceSysAddressId = source.SourceSysAddressId,
        City               = source.City,
        State              = source.State,
        StateCode          = source.StateCode,
        CountryName        = source.CountryName,
        CountryCode        = source.CountryCode,
        Region             = source.Region,
        ZipCode            = source.ZipCode,
        Latitude           = source.Latitude,
        Longitude          = source.Longitude,
        SourceKey          = source.SourceKey
    WHEN NOT MATCHED THEN INSERT (
        AddressKey, AddressId, CountryId, SourceSysAddressId, City, State, StateCode,
        CountryName, CountryCode, Region, ZipCode, Latitude, Longitude, SourceKey
    ) VALUES (
        source.AddressKey, source.AddressId, source.CountryId, source.SourceSysAddressId,
        source.City, source.State, source.StateCode, source.CountryName, source.CountryCode,
        source.Region, source.ZipCode, source.Latitude, source.Longitude, source.SourceKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimAddress,
    hard_delete=True
)

ProcessCdfTable(dimAddressTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimCampaignChannelBridge

# CELL ********************

from pyspark.sql.functions import col, xxhash64

def EnrichDimCampaignChannelBridge(df: DataFrame) -> DataFrame:
    dimCampaignDf = get_gold_table("DimCampaign").select("CampaignId", "CampaignKey")
    dimChannelDf = get_gold_table("DimChannel").select("ChannelId", "ChannelKey")
    dimExistingBridge = get_gold_table("DimCampaignChannelBridge").select("CampaignChannelId")

    new_df = (
        df
        .join(dimCampaignDf, on="CampaignId", how="left")
        .join(dimChannelDf, on="ChannelId", how="left")
        .withColumn("CampaignChannelBridgeKey", xxhash64("CampaignId", "ChannelId").cast("bigint"))
        .select(
            "CampaignChannelBridgeKey",
            "CampaignChannelId",
            "CampaignKey",
            "ChannelKey"
        )
    )

    logging.info(f"✅ DimCampaignChannelBridge processing {new_df.count()} rows.")
    return new_df

campaignChannelBridgeTable = CdfTable(
    source_table_name="CampaignChannel",
    source_primary_key="CampaignChannelId",
    target_table_name="DimCampaignChannelBridge",
    columns=[
        "CampaignChannelId",
        "CampaignId",
        "ChannelId"
        # "ModifiedDate"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimCampaignChannelBridge AS target
    USING latestSnapshot_CampaignChannel AS source
    ON target.CampaignChannelId = source.CampaignChannelId
    WHEN MATCHED THEN UPDATE SET
        CampaignKey = source.CampaignKey,
        ChannelKey = source.ChannelKey
    WHEN NOT MATCHED THEN INSERT (
        CampaignChannelBridgeKey,
        CampaignChannelId,
        CampaignKey,
        ChannelKey
    ) VALUES (
        source.CampaignChannelBridgeKey,
        source.CampaignChannelId,
        source.CampaignKey,
        source.ChannelKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimCampaignChannelBridge,
    hard_delete=True
)

ProcessCdfTable(campaignChannelBridgeTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Program

# MARKDOWN ********************

# ### Build: DimProgram

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64

def EnrichDimProgram(df: DataFrame) -> DataFrame:
    dimSourceDf = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimDateDf = get_gold_table("DimDate").select("Date", "DateKey")

    dimDateCreated = dimDateDf.select(
        col("Date").alias("CreatedDate_lookup"),
        col("DateKey").alias("CreatedDateKey")
    )

    dimDateModified = dimDateDf.select(
        col("Date").alias("ModifiedDate_lookup"),
        col("DateKey").alias("ModifiedDateKey")
    )

    # enrich
    new_df = (
        df
        .join(dimSourceDf, on="SourceId", how="left")
        .join(dimDateCreated, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
        .join(dimDateModified, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
        .withColumn("ProgramKey", xxhash64("ProgramId", "SourceSystemId").cast("bigint"))
        .select(
            "ProgramKey",
            "ProgramId",
            col("Name").alias("ProgramName"),
            col("SourceSystemId").alias("SourceSysProgramId"),
            "CreatedDateKey",
            "ModifiedDateKey",
            "SourceKey"
        )
    )

    logging.info(f"✅ DimProgram processing {new_df.count()} rows.")

    return new_df

programTable = CdfTable(
    source_table_name="Program",
    source_primary_key="ProgramId",
    target_table_name="DimProgram",
    columns=["ProgramId", "Name", "SourceSystemId", "SourceId", "CreatedDate", "ModifiedDate"],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimProgram AS target
    USING latestSnapshot_Program AS source
    ON target.ProgramId = source.ProgramId
    WHEN MATCHED THEN UPDATE SET
        ProgramName = source.ProgramName,
        SourceSysProgramId = source.SourceSysProgramId,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        SourceKey = source.SourceKey
    WHEN NOT MATCHED THEN INSERT (
        ProgramKey, ProgramId, ProgramName, SourceSysProgramId, CreatedDateKey, ModifiedDateKey, SourceKey
    ) VALUES (
        source.ProgramKey, source.ProgramId, source.ProgramName, source.SourceSysProgramId, source.CreatedDateKey, source.ModifiedDateKey, source.SourceKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimProgram,
    hard_delete=True
)

ProcessCdfTable(programTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Activities

# MARKDOWN ********************

# ### Build: DimLetter

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64

def EnrichDimLetter(df: DataFrame) -> DataFrame:
    dimSourceDf = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimDateDf = get_gold_table("DimDate")
    dimChannelDf = get_gold_table("DimChannel").select("ChannelId", "ChannelKey")

    date_created = dimDateDf.select(
        col("Date").alias("CreatedDate_lookup"),
        col("DateKey").alias("CreatedDateKey")
    )

    date_modified = dimDateDf.select(
        col("Date").alias("ModifiedDate_lookup"),
        col("DateKey").alias("ModifiedDateKey")
    )

    date_sent = dimDateDf.select(
        col("Date").alias("SentDate_lookup"),
        col("DateKey").alias("SentDateKey")
    )

    new_df = (
        df
        .join(dimSourceDf, on="SourceId", how="left")
        .join(dimChannelDf, on="ChannelId", how="left")
        .join(date_created, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
        .join(date_modified, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
        .join(date_sent, expr("cast(SentDate as date) = SentDate_lookup"), "left")
        .withColumn("LetterKey", xxhash64("LetterId", "SourceSystemId").cast("bigint"))
        .select(
            "LetterKey",
            "LetterId",
            col("Subject").alias("LetterSubject"),
            col("SourceSystemId").alias("SourceSysLetterId"),
            "CreatedDateKey",
            "ModifiedDateKey",
            "SentDateKey",
            "SourceKey",
            "ChannelKey"
        )
    )

    logging.info(f"✅ DimLetter processing {new_df.count()} rows.")
    return new_df


letterTable = CdfTable(
    source_table_name="Letter",
    source_primary_key="LetterId",
    target_table_name="DimLetter",
    columns=["LetterId", "Subject", "SourceSystemId", "CreatedDate", "ModifiedDate", "SentDate", "SourceId", "ChannelId"],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimLetter AS target
    USING latestSnapshot_Letter AS source
    ON target.LetterId = source.LetterId
    WHEN MATCHED THEN UPDATE SET
        LetterSubject = source.LetterSubject,
        SourceSysLetterId = source.SourceSysLetterId,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        SentDateKey = source.SentDateKey,
        SourceKey = source.SourceKey,
        ChannelKey = source.ChannelKey
    WHEN NOT MATCHED THEN INSERT (
        LetterKey, LetterId, LetterSubject, SourceSysLetterId,
        CreatedDateKey, ModifiedDateKey, SentDateKey, SourceKey, ChannelKey
    ) VALUES (
        source.LetterKey, source.LetterId, source.LetterSubject, source.SourceSysLetterId,
        source.CreatedDateKey, source.ModifiedDateKey, source.SentDateKey, source.SourceKey, source.ChannelKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimLetter,
    hard_delete=True
)

ProcessCdfTable(letterTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimPhonecall

# CELL ********************

from pyspark.sql.functions import col, to_date, xxhash64

def EnrichDimPhonecall(df: DataFrame) -> DataFrame:
    dimDateDf = get_gold_table("DimDate").select("Date", "DateKey")   

    dimDateCreated = dimDateDf.select(
        col("Date").alias("CreatedDate_lookup"),
        col("DateKey").alias("CreatedDateKey")
    )

    dimDateModified = dimDateDf.select(
        col("Date").alias("ModifiedDate_lookup"),
        col("DateKey").alias("ModifiedDateKey")
    )

    dim_source = get_gold_table("DimSource").select("SourceId", "SourceKey")

    dim_channel = get_gold_table("DimChannel").select("ChannelId", "ChannelKey")

    new_df = (
        df.select("PhonecallId", "Description", "CreatedDate", "ModifiedDate", "SourceId", "SourceSystemId", "ChannelId")
          .withColumn("PhonecallKey", xxhash64("PhonecallId", "SourceSystemId").cast("bigint"))
          .join(dimDateCreated, to_date("CreatedDate") == col("CreatedDate_lookup"), "left")
          .join(dimDateModified, to_date("ModifiedDate") == col("ModifiedDate_lookup"), "left")
          .join(dim_source, "SourceId", "left")
          .join(dim_channel, "ChannelId", "left")
          .select(
              "PhonecallKey",
              "PhonecallId",
              col("SourceSystemId").alias("SourceSysPhonecallId"),
              col("Description").alias("PhonecallDescription"),
              "CreatedDateKey",
              "ModifiedDateKey",
              "SourceKey",
              "ChannelKey"
          )
    )

    logging.info(f"✅ DimPhonecall processing {new_df.count()} rows.")
    return new_df


dimPhonecallTable = CdfTable(
    source_table_name="Phonecall",
    source_primary_key="PhonecallId",
    target_table_name="DimPhonecall",
    columns=["PhonecallId", "Description", "CreatedDate", "ModifiedDate", "SourceId", "SourceSystemId", "ChannelId"], 
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimPhonecall AS target
    USING latestSnapshot_Phonecall AS source
    ON target.PhonecallId = source.PhonecallId
    WHEN MATCHED THEN UPDATE SET
        PhonecallDescription = source.PhonecallDescription,
        SourceSysPhonecallId = source.SourceSysPhonecallId,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        SourceKey = source.SourceKey,
        ChannelKey = source.ChannelKey
    WHEN NOT MATCHED THEN INSERT (
        PhonecallKey, PhonecallId, PhonecallDescription, SourceSysPhonecallId, CreatedDateKey, ModifiedDateKey, SourceKey, ChannelKey
    ) VALUES (
        source.PhonecallKey, source.PhonecallId, source.PhonecallDescription, source.SourceSysPhonecallId,
        source.CreatedDateKey, source.ModifiedDateKey, source.SourceKey, source.ChannelKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimPhonecall,
    hard_delete=True
)

ProcessCdfTable(dimPhonecallTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimEmail

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, xxhash64

def EnrichDimEmail(df: DataFrame) -> DataFrame:
    # Load dimension tables
    dimDate = get_gold_table("DimDate").select(col("Date").alias("DimDate"), col("DateKey"))
    dimSource = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimChannel = get_gold_table("DimChannel").select("ChannelId", "ChannelKey")

    # Prepare date joins
    dateCreated = dimDate.withColumnRenamed("DimDate", "CreatedDate_lookup").withColumnRenamed("DateKey", "CreatedDateKey")
    dateModified = dimDate.withColumnRenamed("DimDate", "ModifiedDate_lookup").withColumnRenamed("DateKey", "ModifiedDateKey")

    # Enrichment
    enriched = (
        df
        .join(dimSource, "SourceId", "left")
        .join(dimChannel, "ChannelId", "left")
        .join(dateCreated, to_date("CreatedDate") == col("CreatedDate_lookup"), "left")
        .join(dateModified, to_date("ModifiedDate") == col("ModifiedDate_lookup"), "left")
        .withColumn("EmailKey", xxhash64("EmailId", "SourceSystemId").cast("bigint"))
        .withColumnRenamed("SourceSystemId", "SourceSystemEmailId")
        .select(
            "EmailEngagementId",
            "EmailId",
            "EmailKey",
            col("Subject").alias("EmailSubject"),
            "SourceSystemEmailId",
            "ChannelKey",
            "SourceKey",
            "CreatedDateKey",
            "ModifiedDateKey",
            "VariantType"
        )
    )

    logging.info(f"✅ DimEmail processing {enriched.count()} rows.")
    return enriched



dimEmailTable = CdfTable(
    source_table_name="EmailEngagement",
    source_primary_key="EmailEngagementId",
    target_table_name="DimEmail",
    columns=[
        "EmailEngagementId", "EmailId", "Subject", "VariantType", "CreatedDate", "ModifiedDate", "ChannelId",
        "SourceId", "SourceSystemId"
    ],
    merge_sql_template=f"""
MERGE INTO {gold_lakehouse_name}.DimEmail AS target
USING latestSnapshot_EmailEngagement AS source
ON target.EmailEngagementId = source.EmailEngagementId
WHEN MATCHED THEN UPDATE SET
    EmailSubject = source.EmailSubject,
    SourceSystemEmailId = source.SourceSystemEmailId,
    ChannelKey = source.ChannelKey,
    SourceKey = source.SourceKey,
    ModifiedDateKey = source.ModifiedDateKey,
    CreatedDateKey = source.CreatedDateKey,
    VariantType = source.VariantType
WHEN NOT MATCHED THEN INSERT (
    EmailEngagementId, EmailId, EmailKey, EmailSubject, SourceSystemEmailId,
    ChannelKey, SourceKey, ModifiedDateKey, CreatedDateKey, VariantType
) VALUES (
    source.EmailEngagementId, source.EmailId, source.EmailKey, source.EmailSubject, source.SourceSystemEmailId,
    source.ChannelKey, source.SourceKey, source.ModifiedDateKey, source.CreatedDateKey, source.VariantType
)

    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimEmail,
    hard_delete=True
)

ProcessCdfTable(dimEmailTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constituent

# MARKDOWN ********************

# ### Build: DimConstituent

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, concat_ws, coalesce, lit, xxhash64, to_date, row_number
)
from pyspark.sql.window import Window


def EnrichDimConstituent(df: DataFrame) -> DataFrame:
    # 🗂️ Load reference tables
    dimDateDf = get_gold_table("DimDate").select("Date", "DateKey")
    dimAddressDf = get_gold_table("DimAddress").select("AddressId", "AddressKey")
    dimSourceDf = get_gold_table("DimSource").select("SourceId", "SourceKey")

    engagementStageDf = get_table_from_lakehouse(silver_lakehouse_name, "EngagementStage") \
        .select("EngagementStageId", col("Name").alias("StageName"))

    constituentTypeDf = get_table_from_lakehouse(silver_lakehouse_name, "ConstituentType") \
        .select("ConstituentTypeId", col("Name").alias("ConstituentTypeName"))

    genderDf = get_table_from_lakehouse(silver_lakehouse_name, "Gender") \
        .select("GenderId", col("Name").alias("GenderName"))

    contactDf = get_table_from_lakehouse(silver_lakehouse_name, "Contact").alias("contact")
    accountDf = get_table_from_lakehouse(silver_lakehouse_name, "Account").alias("account")
    constituentAllDf = get_table_from_lakehouse(silver_lakehouse_name, "Constituent").alias("constituent")

    # ✅ Join source
    if "ConstituentId" in df.columns:
        constituentDf = constituentAllDf.join(df, "ConstituentId", "inner")
    elif "AccountId" in df.columns:
        constituentDf = constituentAllDf.join(df, "AccountId", "inner")
    elif "ContactId" in df.columns:
        constituentDf = constituentAllDf.join(df, "ContactId", "inner")
    else:
        raise ValueError("Input DataFrame must contain one of: ConstituentId, AccountId, ContactId")

    # 🔗 Join enrichment
    df_joined = (
        constituentDf
        .join(accountDf, "AccountId", "left")
        .join(contactDf, "ContactId", "left")
        .join(dimAddressDf, coalesce(col("contact.AddressId"), col("account.AddressId")) == dimAddressDf.AddressId, "left")
        .withColumn("RegistrationDateCoalesced", coalesce(col("contact.RegistrationDate"), col("account.RegistrationDate")))
        .withColumn("CreatedDateCoalesced", coalesce(col("contact.CreatedDate"), col("account.CreatedDate")))
        .withColumn("ModifiedDateCoalesced", coalesce(col("contact.ModifiedDate"), col("account.ModifiedDate")))
        .join(
            dimDateDf.withColumnRenamed("DateKey", "CreatedDateKey").withColumnRenamed("Date", "CreatedDate_lookup"),
            to_date(col("CreatedDateCoalesced")) == col("CreatedDate_lookup"), "left"
        )
        .join(
            dimDateDf.withColumnRenamed("DateKey", "ModifiedDateKey").withColumnRenamed("Date", "ModifiedDate_lookup"),
            to_date(col("ModifiedDateCoalesced")) == col("ModifiedDate_lookup"), "left"
        )
        .join(
            dimDateDf.withColumnRenamed("DateKey", "RegistrationDateKey").withColumnRenamed("Date", "RegDate_lookup"),
            to_date(col("RegistrationDateCoalesced")) == col("RegDate_lookup"), "left"
        )
        .withColumn("SourceIdCoalesced", coalesce(col("contact.SourceId"), col("account.SourceId")))
        .join(dimSourceDf, col("SourceIdCoalesced") == dimSourceDf.SourceId, "left")
        .withColumn("EngagementStageIdCoalesced", coalesce(col("contact.EngagementStageId"), col("account.EngagementStageId")))
        .join(engagementStageDf, col("EngagementStageIdCoalesced") == engagementStageDf.EngagementStageId, "left")
        .join(constituentTypeDf, col("constituent.ConstituentTypeId") == constituentTypeDf.ConstituentTypeId, "left")
        .join(genderDf, col("contact.GenderId") == genderDf.GenderId, "left")
        .withColumn("ConstituentName",
            when(col("contact.FirstName").isNotNull(),
                concat_ws(" ", col("contact.FirstName"), col("contact.LastName")))
            .otherwise(col("account.Name"))
        )
        .withColumn("FinalEmail", when(col("contact.Email").isNotNull(), col("contact.Email"))
                    .otherwise(col("account.Email")))
    )

    df_joined = df_joined.withColumn(
        "ConstituentKey",
        xxhash64(
            col("ConstituentId"),
            when(col("contact.SourceSystemId").isNotNull(), col("contact.SourceSystemId"))
            .otherwise(col("account.SourceSystemId"))
        ).cast("bigint")
    )

    df_joined = df_joined.withColumn("is_contact_preferred", col("contact.ContactId").isNotNull().cast("int"))
    window_spec = Window.partitionBy("ConstituentId").orderBy(col("is_contact_preferred").desc())

    df_joined_deduped = df_joined.withColumn("rownum", row_number().over(window_spec)) \
                                 .filter(col("rownum") == 1) \
                                 .drop("rownum", "is_contact_preferred")

    # ✅ Final select
    df_joined_deduped = df_joined_deduped.select(
        "ConstituentKey",
        "ConstituentId",
        when(col("contact.SourceSystemId").isNotNull(), col("contact.SourceSystemId"))
            .otherwise(col("account.SourceSystemId")).alias("SourceSysConstituentId"),
        "ConstituentName",
        col("FinalEmail").alias("Email"),
        col("StageName").alias("EngagementStage"),
        col("ConstituentTypeName").alias("ConstituentType"),
        col("GenderName").alias("Gender"),
        "AddressKey",
        "CreatedDateKey",
        "ModifiedDateKey",
        "RegistrationDateKey",
        "SourceKey"
    )

    count = df_joined_deduped.count()
    print(f"✅ DimConstituent: {count} new rows will be inserted/updated.")
    if count > 0:
        print("📄 Preview of inserted/updated rows:")
        df_joined_deduped.show(10, truncate=False)

    return df_joined_deduped


def generate_merge_sql(target_table: str, snapshot_name: str) -> str:
    return f"""
    MERGE INTO {gold_lakehouse_name}.{target_table} AS target
    USING {snapshot_name} AS source
    ON target.ConstituentId = source.ConstituentId
    WHEN MATCHED THEN UPDATE SET
        target.ConstituentKey = source.ConstituentKey,
        target.SourceSysConstituentId = source.SourceSysConstituentId,
        target.ConstituentName = source.ConstituentName,
        target.Email = source.Email,
        target.EngagementStage = source.EngagementStage,
        target.ConstituentType = source.ConstituentType,
        target.Gender = source.Gender,
        target.AddressKey = source.AddressKey,
        target.CreatedDateKey = source.CreatedDateKey,
        target.ModifiedDateKey = source.ModifiedDateKey,
        target.RegistrationDateKey = source.RegistrationDateKey,
        target.SourceKey = source.SourceKey
    WHEN NOT MATCHED THEN INSERT (
        ConstituentKey,
        ConstituentId,
        SourceSysConstituentId,
        ConstituentName,
        Email,
        EngagementStage,
        ConstituentType,
        Gender,
        AddressKey,
        CreatedDateKey,
        ModifiedDateKey,
        RegistrationDateKey,
        SourceKey
    ) VALUES (
        source.ConstituentKey,
        source.ConstituentId,
        source.SourceSysConstituentId,
        source.ConstituentName,
        source.Email,
        source.EngagementStage,
        source.ConstituentType,
        source.Gender,
        source.AddressKey,
        source.CreatedDateKey,
        source.ModifiedDateKey,
        source.RegistrationDateKey,
        source.SourceKey
    )
    """

def map_contact_to_constituent(keys_df, table):
    # keys_df has column ContactId (built from the CDF PK of the Contact source)
    cons = get_silver_table("Constituent").select("ConstituentId", "ContactId")
    return (keys_df.alias("k")
            .join(cons.alias("c"), col("k.ContactId") == col("c.ContactId"), "inner")
            .select(col("c.ConstituentId").alias("ConstituentId"))
            .dropDuplicates())

def map_account_to_constituent(keys_df, table):
    # keys_df has column AccountId (built from the CDF PK of the Account source)
    cons = get_silver_table("Constituent").select("ConstituentId", "AccountId")
    return (keys_df.alias("k")
            .join(cons.alias("c"), col("k.AccountId") == col("c.AccountId"), "inner")
            .select(col("c.ConstituentId").alias("ConstituentId"))
            .dropDuplicates())


constituentTable = CdfTable(
    source_table_name="Constituent",
    source_primary_key="ConstituentId",
    target_table_name="DimConstituent",
    columns=[
        "ConstituentId",
        "AccountId",
        "ContactId",
        "ConstituentTypeId"
    ],
    merge_sql_template = generate_merge_sql("DimConstituent", "latestSnapshot_Constituent"),
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimConstituent,
    hard_delete=True,
    delete_on="t.ConstituentId = k.ConstituentId" 
)

accountTable = CdfTable(
    source_table_name="Account",
    source_primary_key="AccountId",
    target_table_name="DimConstituent",
    columns=[
        "AccountId",
        "Name",
        "Email",
        "EngagementStageId",
        "AddressId",
        "RegistrationDate",
        "CreatedDate",
        "ModifiedDate",
        "SourceId",
        "SourceSystemId"
    ],
    merge_sql_template = generate_merge_sql("DimConstituent", "latestSnapshot_Account"),
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimConstituent,
    hard_delete=True,
    delete_on="t.ConstituentId = k.ConstituentId",
    delete_key_mapper=map_account_to_constituent   
)

contactTable = CdfTable(
    source_table_name="Contact",
    source_primary_key="ContactId",
    target_table_name="DimConstituent",
    columns=[
        "ContactId",
        "FirstName",
        "LastName",
        "Email",
        "EngagementStageId",
        "GenderId",
        "AddressId",
        "RegistrationDate",
        "CreatedDate",
        "ModifiedDate",
        "SourceId",
        "SourceSystemId"
    ],
    merge_sql_template = generate_merge_sql("DimConstituent", "latestSnapshot_Contact"),
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimConstituent,
    hard_delete=True,
    delete_on="t.ConstituentId = k.ConstituentId",
    delete_key_mapper=map_contact_to_constituent      
)

ProcessCdfTable(constituentTable)
ProcessCdfTable(accountTable)
ProcessCdfTable(contactTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimConstituentSegmentType

# CELL ********************

from pyspark.sql.functions import col, xxhash64

def EnrichDimConstituentSegmentType(df: DataFrame) -> DataFrame:
    dimDf = get_gold_table("DimConstituentSegmentType").select("ConstituentSegmentTypeId")

    new_df = (
        df
        .withColumn(
            "ConstituentSegmentTypeKey",
            xxhash64(col("ConstituentSegmentTypeId"), col("Name")).cast("bigint")
        )
        .select(
            "ConstituentSegmentTypeKey",
            "ConstituentSegmentTypeId",
            col("Name").alias("ConstituentSegmentType")
        )
    )

    logging.info(f"✅ DimConstituentSegmentType processing {new_df.count()} rows.")

    return new_df

constituentSegmentTypeTable = CdfTable(
    source_table_name="ConstituentSegmentType",
    source_primary_key="ConstituentSegmentTypeId",
    target_table_name="DimConstituentSegmentType",
    columns=[
        "ConstituentSegmentTypeId",
        "Name"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimConstituentSegmentType AS target
    USING latestSnapshot_ConstituentSegmentType AS source
    ON target.ConstituentSegmentTypeId = source.ConstituentSegmentTypeId
    WHEN MATCHED THEN UPDATE SET
        ConstituentSegmentType = source.ConstituentSegmentType
    WHEN NOT MATCHED THEN INSERT (
        ConstituentSegmentTypeKey,
        ConstituentSegmentTypeId,
        ConstituentSegmentType
    ) VALUES (
        source.ConstituentSegmentTypeKey,
        source.ConstituentSegmentTypeId,
        source.ConstituentSegmentType
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimConstituentSegmentType
)

ProcessCdfTable(constituentSegmentTypeTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimConstituentSegment

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64

def EnrichDimConstituentSegment(df: DataFrame) -> DataFrame:
    
    # Lookup tables for keys
    dimTypeDf = (
        get_gold_table("DimConstituentSegmentType")
        .select(
            "ConstituentSegmentTypeId",
            col("ConstituentSegmentTypeKey").alias("TypeKey")
        )
    )
    dimSourceDf = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimDateDf = get_gold_table("DimDate").select("Date", "DateKey")
    
    dimDateCreated = dimDateDf.select(
        col("Date").alias("CreatedDate_lookup"),
        col("DateKey").alias("CreatedDateKey")
    )
    dimDateModified = dimDateDf.select(
        col("Date").alias("ModifiedDate_lookup"),
        col("DateKey").alias("ModifiedDateKey")
    )

    new_df = (
        df
        .join(dimTypeDf, on="ConstituentSegmentTypeId", how="left")
        .join(dimSourceDf, on="SourceId", how="left")
        .join(
            dimDateCreated,
            expr("cast(CreatedDate as date) = CreatedDate_lookup"),
            how="left"
        )
        .join(
            dimDateModified,
            expr("cast(ModifiedDate as date) = ModifiedDate_lookup"),
            how="left"
        )
        .withColumn(
            "ConstituentSegmentKey",
            xxhash64(
                col("ConstituentSegmentId"),
                col("Name"),
                col("SourceSystemId")
            ).cast("bigint")
        )
        .select(
            "ConstituentSegmentKey",
            "ConstituentSegmentId",
            col("Name").alias("ConstituentSegmentName"),
            "TypeKey",
            col("SourceSystemId").alias("SourceSysConstituentSegmentId"),
            "CreatedDateKey",
            "ModifiedDateKey",
            "SourceKey",
            "Order"
        )
    )

    logging.info(f"✅ DimConstituentSegment processing {new_df.count()} rows.")

    return new_df

constituentSegmentTable = CdfTable(
    source_table_name="ConstituentSegment",
    primary_key="ConstituentSegmentId",
    target_table_name="DimConstituentSegment",
    columns=[
        "ConstituentSegmentId",
        "ConstituentSegmentTypeId",
        "Name",
        "SourceSystemId",
        "SourceId",
        "CreatedDate",
        "ModifiedDate",
        "Order"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimConstituentSegment AS target
    USING latestSnapshot_ConstituentSegment AS source
    ON target.ConstituentSegmentId = source.ConstituentSegmentId
    WHEN MATCHED THEN UPDATE SET
        ConstituentSegmentName = source.ConstituentSegmentName,
        TypeKey = source.TypeKey,
        SourceSysConstituentSegmentId = source.SourceSysConstituentSegmentId,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        SourceKey = source.SourceKey,
        Order = source.Order
    WHEN NOT MATCHED THEN INSERT (
        ConstituentSegmentKey,
        ConstituentSegmentId,
        ConstituentSegmentName,
        TypeKey,
        SourceSysConstituentSegmentId,
        CreatedDateKey,
        ModifiedDateKey,
        SourceKey,
        Order
    ) VALUES (
        source.ConstituentSegmentKey,
        source.ConstituentSegmentId,
        source.ConstituentSegmentName,
        source.TypeKey,
        source.SourceSysConstituentSegmentId,
        source.CreatedDateKey,
        source.ModifiedDateKey,
        source.SourceKey,
        source.Order
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimConstituentSegment,
    hard_delete=True
)

ProcessCdfTable(constituentSegmentTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimConstituentSegmentBridge 

# CELL ********************

def EnrichDimConstituentSegmentBridge(df: DataFrame) -> DataFrame:
    dim_constituent = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey")
    dim_segment = get_gold_table("DimConstituentSegment").select("ConstituentSegmentId", "ConstituentSegmentKey")

    df_new = (
        df.join(dim_constituent, on="ConstituentId", how="left")
          .join(dim_segment, on="ConstituentSegmentId", how="left")
          .withColumn(
              "ConstituentSegmentBridgeKey",
              xxhash64(
                  col("ConstituentSegmentMappingId"),
                  col("ConstituentId")
              ).cast("bigint")
          )
          .select(
              "ConstituentSegmentBridgeKey",
              "ConstituentSegmentMappingId",
              "ConstituentKey",
              "ConstituentSegmentKey"
          )
    )

    logging.info(f"✅ DimConstituentSegmentBridge processing {df_new.count()} rows.")

    return df_new

constituentSegmentBridgeTable = CdfTable(
    source_table_name="ConstituentSegmentMapping",
    target_table_name="DimConstituentSegmentBridge",
    source_primary_key="ConstituentSegmentMappingId",
    columns=[
        "ConstituentSegmentMappingId", "ConstituentId", "ConstituentSegmentId"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimConstituentSegmentBridge AS target
    USING latestSnapshot_ConstituentSegmentMapping AS source
    ON target.ConstituentSegmentMappingId = source.ConstituentSegmentMappingId
    WHEN MATCHED THEN UPDATE SET
        ConstituentKey = source.ConstituentKey,
        ConstituentSegmentKey = source.ConstituentSegmentKey
    WHEN NOT MATCHED THEN INSERT (
        ConstituentSegmentBridgeKey,
        ConstituentSegmentMappingId,
        ConstituentKey,
        ConstituentSegmentKey
    ) VALUES (
        source.ConstituentSegmentBridgeKey,
        source.ConstituentSegmentMappingId,
        source.ConstituentKey,
        source.ConstituentSegmentKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimConstituentSegmentBridge,
    hard_delete=True
)

ProcessCdfTable(constituentSegmentBridgeTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimEngagementPlatform

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64

def EnrichDimEngagementPlatform(df: DataFrame) -> DataFrame:
    dimSourceDf = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimDateDf = get_gold_table("DimDate")

    date_created = dimDateDf.select(
        col("Date").alias("CreatedDate_lookup"),
        col("DateKey").alias("CreatedDateKey")
    )

    date_modified = dimDateDf.select(
        col("Date").alias("ModifiedDate_lookup"),
        col("DateKey").alias("ModifiedDateKey")
    )

    new_df = (
        df
        .join(dimSourceDf, on="SourceId", how="left")
        .join(date_created, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
        .join(date_modified, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
        .withColumn(
            "EngagementPlatformKey",
            xxhash64(
                col("EngagementPlatformId"),
                col("SourceSystemId"),
                col("Name")
            ).cast("bigint")
        )
        .select(
            "EngagementPlatformKey",
            "EngagementPlatformId",
            col("Name").alias("EngagementPlatformName"),
            col("SourceSystemId").alias("SourceSysEngagementPlatformId"),
            "CreatedDateKey",
            "ModifiedDateKey",
            "SourceKey"
        )
    )

    logging.info(f"✅ DimEngagementPlatform processing {new_df.count()} rows.")
    return new_df

engagementPlatformTable = CdfTable(
    source_table_name="EngagementPlatform",
    primary_key="EngagementPlatformId",
    target_table_name="DimEngagementPlatform",
    columns=["EngagementPlatformId", "Name", "SourceSystemId", "CreatedDate", "ModifiedDate", "SourceId"],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimEngagementPlatform AS target
    USING latestSnapshot_EngagementPlatform AS source
    ON target.EngagementPlatformId = source.EngagementPlatformId
    WHEN MATCHED THEN UPDATE SET
        Name = source.EngagementPlatformName,
        SourceSysEngagementPlatformId = source.SourceSysEngagementPlatformId,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        SourceKey = source.SourceKey
    WHEN NOT MATCHED THEN INSERT (
        EngagementPlatformKey, EngagementPlatformId, Name, SourceSysEngagementPlatformId,
        CreatedDateKey, ModifiedDateKey, SourceKey
    ) VALUES (
        source.EngagementPlatformKey, source.EngagementPlatformId, source.EngagementPlatformName, source.SourceSysEngagementPlatformId,
        source.CreatedDateKey, source.ModifiedDateKey, source.SourceKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimEngagementPlatform
)

ProcessCdfTable(engagementPlatformTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimEvent

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64

def EnrichDimEvent(df: DataFrame) -> DataFrame:
    dimSourceDf = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimAddressDf = get_gold_table("DimAddress").select("AddressId", "AddressKey")
    dimChannelDf = get_gold_table("DimChannel").select("ChannelId", "ChannelKey")
    dimDateDf = get_gold_table("DimDate")

    date_start = dimDateDf.select(
        col("Date").alias("StartDate_lookup"),
        col("DateKey").alias("EventDateKey")
    )
    date_created = dimDateDf.select(
        col("Date").alias("CreatedDate_lookup"),
        col("DateKey").alias("CreatedDateKey")
    )
    date_modified = dimDateDf.select(
        col("Date").alias("ModifiedDate_lookup"),
        col("DateKey").alias("ModifiedDateKey")
    )

    new_df = (
        df
        .join(dimSourceDf, on="SourceId", how="left")
        .join(dimAddressDf, on="AddressId", how="left")
        .join(dimChannelDf, on="ChannelId", how="left")  
        .join(date_start, expr("cast(StartDate as date) = StartDate_lookup"), "left")
        .join(date_created, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
        .join(date_modified, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
        .withColumn(
            "EventKey",
            xxhash64(col("EventId"), col("SourceSystemId")).cast("bigint")
        )
        .select(
            "EventKey",
            "EventId",
            col("SourceSystemId").alias("SourceSysEventId"),
            "EventDateKey",
            col("Name").alias("EventName"),
            "Cost",
            "AddressKey",
            "CreatedDateKey",
            "ModifiedDateKey",
            "ChannelKey", 
            "SourceKey"
        )
    )

    logging.info(f"✅ DimEvent processing {new_df.count()} rows.")
    return new_df


eventTable = CdfTable(
    source_table_name="Event",
    target_table_name="DimEvent",
    primary_key="EventId",
    columns=["EventId", "SourceSystemId", "StartDate", "Name", "Cost",
             "AddressId", "CreatedDate", "ModifiedDate", "ChannelId", "SourceId"],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimEvent AS target
    USING latestSnapshot_Event AS source
    ON target.EventId = source.EventId
    WHEN MATCHED THEN UPDATE SET
        SourceSysEventId = source.SourceSysEventId,
        EventDateKey = source.EventDateKey,
        EventName = source.EventName,
        Cost = source.Cost,
        AddressKey = source.AddressKey,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        ChannelKey = source.ChannelKey,
        SourceKey = source.SourceKey
    WHEN NOT MATCHED THEN INSERT (
        EventKey, EventId, SourceSysEventId, EventDateKey, EventName,
        Cost, AddressKey, CreatedDateKey, ModifiedDateKey, ChannelKey, SourceKey
    ) VALUES (
        source.EventKey, source.EventId, source.SourceSysEventId, source.EventDateKey,
        source.EventName, source.Cost, source.AddressKey,
        source.CreatedDateKey, source.ModifiedDateKey, source.ChannelKey, source.SourceKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimEvent,
    hard_delete=True
)

ProcessCdfTable(eventTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Opportunity

# MARKDOWN ********************

# ### Build: DimOpportunityType

# CELL ********************

from pyspark.sql.functions import col, xxhash64

def EnrichDimOpportunityType(df: DataFrame) -> DataFrame:

    new_df = (
        df
        .select("OpportunityTypeId", col("Name").alias("OpportunityTypeName"))
        .dropna(subset=["OpportunityTypeId"])
        .withColumn(
            "OpportunityTypeKey",
            xxhash64(
                col("OpportunityTypeId"),
                col("OpportunityTypeName")
            ).cast("bigint")
        )
        .select("OpportunityTypeKey", "OpportunityTypeId", "OpportunityTypeName")
    )

    logging.info(f"✅ DimOpportunityType processing {new_df.count()} rows.")

    return new_df

dimOpportunityTypeTable = CdfTable(
    source_table_name="OpportunityType",
    source_primary_key="OpportunityTypeId",
    target_table_name="DimOpportunityType",
    columns=[
        "OpportunityTypeId", "Name", "CreatedDate", "ModifiedDate",
        "SourceId", "SourceSystemId"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimOpportunityType AS target
    USING latestSnapshot_OpportunityType AS source
    ON target.OpportunityTypeId = source.OpportunityTypeId
    WHEN MATCHED THEN UPDATE SET
        OpportunityTypeName = source.OpportunityTypeName
    WHEN NOT MATCHED THEN INSERT (
        OpportunityTypeKey, OpportunityTypeId, OpportunityTypeName
    ) VALUES (
        source.OpportunityTypeKey, source.OpportunityTypeId, source.OpportunityTypeName
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimOpportunityType,
    hard_delete=True
)

ProcessCdfTable(dimOpportunityTypeTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimOpportunityStage

# CELL ********************

from pyspark.sql.functions import col, xxhash64

def EnrichDimOpportunityStage(df: DataFrame) -> DataFrame:
    dim_type = get_gold_table("DimOpportunityType").select(
        "OpportunityTypeId", "OpportunityTypeKey"
    ).alias("dot")

    df = df.alias("s")

    new_df = (
        df
        .join(dim_type, col("s.OpportunityTypeId") == col("dot.OpportunityTypeId"), "left")
        .withColumn(
            "OpportunityStageKey",
            xxhash64(
                col("s.OpportunityStageId"),
                col("s.OpportunityTypeId"),
                col("s.Name")
            ).cast("bigint")
        )
        .select(
            "OpportunityStageKey",
            col("s.OpportunityStageId"),
            col("s.OpportunityTypeId"),
            col("dot.OpportunityTypeKey"),
            col("s.Name").alias("OpportunityStage")
        )
    )

    logging.info(f"✅ DimOpportunityStage processing {new_df.count()} rows.")

    return new_df

opportunityStageTable = CdfTable(
    source_table_name="OpportunityStage",
    primary_key="OpportunityStageId",
    target_table_name="DimOpportunityStage",
    columns=[
        "OpportunityStageId",
        "OpportunityTypeId",
        "Name"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimOpportunityStage AS target
    USING latestSnapshot_OpportunityStage AS source
    ON target.OpportunityStageId = source.OpportunityStageId
    WHEN MATCHED THEN UPDATE SET
        OpportunityTypeId = source.OpportunityTypeId,
        OpportunityTypeKey = source.OpportunityTypeKey,
        OpportunityStage = source.OpportunityStage
    WHEN NOT MATCHED THEN INSERT (
        OpportunityStageKey,
        OpportunityStageId,
        OpportunityTypeId,
        OpportunityTypeKey,
        OpportunityStage
    ) VALUES (
        source.OpportunityStageKey,
        source.OpportunityStageId,
        source.OpportunityTypeId,
        source.OpportunityTypeKey,
        source.OpportunityStage
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimOpportunityStage
)

ProcessCdfTable(opportunityStageTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: FactOpportunity

# CELL ********************

from pyspark.sql.functions import col, xxhash64

def EnrichFactOpportunity(df: DataFrame) -> DataFrame:
    import pyspark.sql.functions as F

    df = df.alias("o")

    # Load dimension tables with aliases
    dim_date = get_gold_table("DimDate").select(F.col("Date").alias("DimDate"), F.col("DateKey"))
    dim_campaign = get_gold_table("DimCampaign").select("CampaignId", "CampaignKey").alias("dc")
    dim_channel = get_gold_table("DimChannel").select("ChannelId", "ChannelKey").alias("dch")
    dim_constituent = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey").alias("dcon")
    dim_stage = get_gold_table("DimOpportunityStage").select("OpportunityStageId", "OpportunityStageKey", "OpportunityStage").alias("dos")
    dim_type = get_gold_table("DimOpportunityType").select("OpportunityTypeId", "OpportunityTypeKey").alias("dot")
    dim_source = get_gold_table("DimSource").select("SourceId", "SourceKey").alias("ds")

    new_df = (
        df.join(dim_campaign, F.col("o.CampaignId") == F.col("dc.CampaignId"), "left")
          .join(dim_channel, F.col("o.ChannelId") == F.col("dch.ChannelId"), "left")
          .join(dim_constituent, F.col("o.ConstituentId") == F.col("dcon.ConstituentId"), "left")
          .join(dim_stage, F.col("o.OpportunityStageId") == F.col("dos.OpportunityStageId"), "left")
          .join(dim_type, F.col("o.OpportunityTypeId") == F.col("dot.OpportunityTypeId"), "left")
          .join(dim_source, F.col("o.SourceId") == F.col("ds.SourceId"), "left")
          .join(dim_date.alias("created"), F.to_date(F.col("o.CreatedDate")) == F.col("created.DimDate"), "left")
          .join(dim_date.alias("modified"), F.to_date(F.col("o.ModifiedDate")) == F.col("modified.DimDate"), "left")
          .join(dim_date.alias("close"), F.to_date(F.col("o.CloseDate")) == F.col("close.DimDate"), "left")
          .withColumn(
              "OpportunityKey",
              xxhash64(
                  F.col("o.OpportunityId"),
                  F.col("o.SourceSystemId")
              ).cast("bigint")
          )
          .withColumn(
              "IsClosed",
              F.when(F.col("dos.OpportunityStage").isin("Closed Won", "Closed Lost"), F.lit(True))
               .otherwise(F.lit(False))
          )
          .select(
              F.col("OpportunityKey"),
              F.col("o.OpportunityId"),
              F.col("o.SourceSystemId").alias("SourceSysOpportunityId"),
              F.col("o.ExpectedRevenue").cast("decimal(18,2)"),
              F.col("o.Timezone"),
              F.col("IsClosed"),
              F.col("dc.CampaignKey"),
              F.col("dch.ChannelKey"),
              F.col("dcon.ConstituentKey"),
              F.col("ds.SourceKey"),
              F.col("dos.OpportunityStageKey"),
              F.col("dot.OpportunityTypeKey"),
              F.col("created.DateKey").alias("CreatedDateKey"),
              F.col("modified.DateKey").alias("ModifiedDateKey"),
              F.col("close.DateKey").alias("CloseDateKey"),
              F.col("o.OpportunityName").alias("OpportunityName")
          )
    )

    logging.info(f"✅ FactOpportunity processing {new_df.count()} rows.")

    return new_df

factOpportunityTable = CdfTable(
    source_table_name="Opportunity",
    target_table_name= "FactOpportunity",
    source_primary_key="OpportunityId",
    columns=[
        "OpportunityId", "CampaignId", "ChannelId", "CloseDate", "ConstituentId",
        "CreatedDate", "ExpectedRevenue", "ModifiedDate", "OpportunityStageId",
        "SourceId", "SourceSystemId", "Timezone", "OpportunityTypeId", "OpportunityName"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.FactOpportunity AS target
    USING latestSnapshot_Opportunity AS source
    ON target.OpportunityId = source.OpportunityId
    WHEN MATCHED THEN UPDATE SET
        CampaignKey = source.CampaignKey,
        ChannelKey = source.ChannelKey,
        CloseDateKey = source.CloseDateKey,
        ConstituentKey = source.ConstituentKey,
        CreatedDateKey = source.CreatedDateKey,
        ExpectedRevenue = source.ExpectedRevenue,
        IsClosed = source.IsClosed,
        ModifiedDateKey = source.ModifiedDateKey,
        OpportunityName = source.OpportunityName,
        OpportunityStageKey = source.OpportunityStageKey,
        OpportunityTypeKey = source.OpportunityTypeKey,
        SourceKey = source.SourceKey,
        SourceSysOpportunityId = source.SourceSysOpportunityId,
        Timezone = source.Timezone
    WHEN NOT MATCHED THEN INSERT (
        OpportunityKey, OpportunityId, CampaignKey, ChannelKey, CloseDateKey,
        ConstituentKey, CreatedDateKey, ExpectedRevenue, IsClosed,
        ModifiedDateKey, OpportunityName, OpportunityStageKey, OpportunityTypeKey,
        SourceKey, SourceSysOpportunityId, Timezone
    ) VALUES (
        source.OpportunityKey, source.OpportunityId, source.CampaignKey, source.ChannelKey,
        source.CloseDateKey, source.ConstituentKey, source.CreatedDateKey,
        source.ExpectedRevenue, source.IsClosed, source.ModifiedDateKey, source.OpportunityName,
        source.OpportunityStageKey, source.OpportunityTypeKey, source.SourceKey,
        source.SourceSysOpportunityId, source.Timezone
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichFactOpportunity,
    hard_delete=True
)

ProcessCdfTable(factOpportunityTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: FactEventAttendance

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64

def EnrichFactEventAttendance(df: DataFrame) -> DataFrame:
    # Dimension lookups
    dimConstituentDf = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey")
    dimEventDf = get_gold_table("DimEvent").select("EventId", "EventKey", "ChannelKey")
    dimSourceDf = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimDateDf = get_gold_table("DimDate").select("Date", "DateKey")

    # DateKey lookups
    dimDateAccepted = dimDateDf.select(col("Date").alias("AcceptedDate_lookup"), col("DateKey").alias("AcceptedDateKey"))
    dimDateCreated = dimDateDf.select(col("Date").alias("CreatedDate_lookup"), col("DateKey").alias("CreatedDateKey"))
    dimDateModified = dimDateDf.select(col("Date").alias("ModifiedDate_lookup"), col("DateKey").alias("ModifiedDateKey"))
    dimDateInvitation = dimDateDf.select(col("Date").alias("InvitationDate_lookup"), col("DateKey").alias("InvitationDateKey"))

    # Enrich
    new_df = (
        df
        .join(dimConstituentDf, on="ConstituentId", how="left")
        .join(dimEventDf, on="EventId", how="left")  # brings both EventKey and ChannelKey
        .join(dimSourceDf, on="SourceId", how="left")
        .join(dimDateAccepted, expr("cast(AcceptedDate as date) = AcceptedDate_lookup"), "left")
        .join(dimDateCreated, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
        .join(dimDateModified, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
        .join(dimDateInvitation, expr("cast(InvitationDate as date) = InvitationDate_lookup"), "left")
        .withColumn(
            "EventAttendanceKey",
            xxhash64(
                col("ParticipationId"),
                col("SourceSystemId")
            ).cast("bigint")
        )
        .select(
            "EventAttendanceKey",
            col("ParticipationId").alias("EventAttendanceId"),
            col("SourceSystemId").alias("SourceSysEventAttendanceId"),
            "AttendedEvent",
            "ConstituentKey",
            "EventKey",
            "ChannelKey",  # from DimEvent
            "SourceKey",
            "AcceptedDateKey",
            "CreatedDateKey",
            "ModifiedDateKey",
            "InvitationDateKey"
        )
    )

    logging.info(f"✅ FactEventAttendance processing {new_df.count()} rows.")
    return new_df



eventAttendanceTable = CdfTable(    
    source_table_name="Participation",
    source_primary_key="ParticipationId",
    target_table_name="FactEventAttendance",
    columns=[
        "ParticipationId", "SourceSystemId", "AttendedEvent", "ConstituentId", "EventId", "SourceId",
        "AcceptedDate", "CreatedDate", "ModifiedDate", "InvitationDate"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.FactEventAttendance AS target
    USING latestSnapshot_Participation AS source
    ON target.EventAttendanceId = source.EventAttendanceId
    WHEN MATCHED THEN UPDATE SET
        SourceSysEventAttendanceId = source.SourceSysEventAttendanceId,
        AttendedEvent = source.AttendedEvent,
        ConstituentKey = source.ConstituentKey,
        EventKey = source.EventKey,
        ChannelKey = source.ChannelKey,
        SourceKey = source.SourceKey,
        AcceptedDateKey = source.AcceptedDateKey,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        InvitationDateKey = source.InvitationDateKey
    WHEN NOT MATCHED THEN INSERT (
        EventAttendanceKey, EventAttendanceId, SourceSysEventAttendanceId, AttendedEvent,
        ConstituentKey, EventKey, ChannelKey, SourceKey,
        AcceptedDateKey, CreatedDateKey, ModifiedDateKey, InvitationDateKey
    ) VALUES (
        source.EventAttendanceKey, source.EventAttendanceId, source.SourceSysEventAttendanceId, source.AttendedEvent,
        source.ConstituentKey, source.EventKey, source.ChannelKey, source.SourceKey,
        source.AcceptedDateKey, source.CreatedDateKey, source.ModifiedDateKey, source.InvitationDateKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichFactEventAttendance,
    hard_delete=True,
    delete_on="t.EventAttendanceId = k.ParticipationId"
)

ProcessCdfTable(eventAttendanceTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Donation

# MARKDOWN ********************

# ### Build: DimDonationSource

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64
from functools import reduce

def EnrichDimDonationSource(df: DataFrame) -> DataFrame:
    # Load common dimension tables
    dimSourceDf = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimDateDf = get_gold_table("DimDate").select("Date", "DateKey")

    dimDateCreated = dimDateDf.select(
        col("Date").alias("CreatedDate_lookup"),
        col("DateKey").alias("CreatedDateKey")
    )
    dimDateModified = dimDateDf.select(
        col("Date").alias("ModifiedDate_lookup"),
        col("DateKey").alias("ModifiedDateKey")
    )

    # Define mapping: TypeName → (gold table, id column, surrogate key column)
    type_to_gold = {
        "EmailEngagement": ("DimEmail", "EmailId", "EmailKey"),
        "SocialEngagement": ("FactConstituentSocialEngagement", "ConstituentSocialEngagementId", "ConstituentSocialEngagementKey"),
        "Event": ("DimEvent", "EventId", "EventKey"),
        "Letter": ("DimLetter", "LetterId", "LetterKey"),
        "PhoneCall": ("DimPhonecall", "PhonecallId", "PhonecallKey"),
    }

    enriched_subsets = []

    for type_name, (gold_table, id_col, key_col) in type_to_gold.items():
        gold_df = get_gold_table(gold_table).select(
            col(id_col).alias("RecordId"),
            col(key_col).alias("ResolvedKey")
        )

        subset = (
            df.filter(col("TypeName") == type_name)
            .join(gold_df, on="RecordId", how="left")
            .join(dimSourceDf, on="SourceId", how="left")
            .join(dimDateCreated, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
            .join(dimDateModified, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
            .withColumn(
                "DonationSourceKey",
                xxhash64(
                    col("TransactionSourceId"),
                    col("RecordId"),
                    col("TypeName")
                ).cast("bigint")
            )
            .select(
                "DonationSourceKey",
                col("TransactionSourceId").alias("DonationSourceId"),
                col("TypeName").alias("DonationSourceTypeName"),
                col("ResolvedKey").alias("DonationSourceRecordKey"),
                "ResolvedKey",
                "SourceKey",
                "CreatedDateKey",
                "ModifiedDateKey"
            )
        )

        enriched_subsets.append(subset)

    if enriched_subsets:
        df_final = reduce(lambda a, b: a.unionByName(b), enriched_subsets)
    else:
        df_final = spark.createDataFrame([], StructType([]))  # fallback

    logging.info(f"✅ DimDonationSource processing {df_final.count()} rows.")
    return df_final

donationSourceTable = CdfTable(
    source_table_name="TransactionSource",
    source_primary_key="TransactionSourceId",
    target_table_name="DimDonationSource",
    columns=["TransactionSourceId", "RecordId", "TypeName", "SourceSystemId", "SourceId", "CreatedDate", "ModifiedDate"],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimDonationSource AS target
    USING latestSnapshot_TransactionSource AS source
    ON target.DonationSourceId = source.DonationSourceId
    WHEN MATCHED THEN UPDATE SET
        DonationSourceTypeName = source.DonationSourceTypeName,
        DonationSourceRecordKey = source.DonationSourceRecordKey,
        SourceKey = source.SourceKey,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey
    WHEN NOT MATCHED THEN INSERT (
        DonationSourceKey, DonationSourceId, DonationSourceTypeName, SourceKey, CreatedDateKey, ModifiedDateKey
    ) VALUES (
        source.DonationSourceKey, source.DonationSourceId, source.DonationSourceTypeName, source.SourceKey, source.CreatedDateKey, source.ModifiedDateKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimDonationSource
)

ProcessCdfTable(donationSourceTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: FactDonation

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_date, xxhash64, date_sub, current_date, max as max_, min as min_
)

def EnrichFactDonation(df: DataFrame) -> DataFrame:
    df = df.alias("t")

    dim_date = get_gold_table("DimDate").select(col("Date").alias("DimDate"), col("DateKey"))
    dim_campaign = get_gold_table("DimCampaign").select("CampaignId", "CampaignKey").alias("dc")
    dim_channel = get_gold_table("DimChannel").select("ChannelId", "ChannelKey").alias("dch")
    dim_const = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey").alias("dcon")
    dim_source = get_gold_table("DimSource").select("SourceId", "SourceKey").alias("ds")
    dim_don_source = get_gold_table("DimDonationSource").select("DonationSourceId", "DonationSourceKey").alias("dds")
    fact_opp = get_gold_table("FactOpportunity").select("OpportunityId", "OpportunityKey").alias("fo")

    one_year_ago = date_sub(current_date(), 365)

    donation_flags = (
        df.withColumn("TxDate", to_date("TransactionDate"))
          .groupBy("ConstituentId")
          .agg(
              (max_(col("TxDate")) > one_year_ago).alias("HasRecent"),
              (min_(col("TxDate")) <= one_year_ago).alias("HasOld")
          )
          .withColumn("IsNewConstituent", (col("HasRecent") & (~col("HasOld"))).cast("boolean"))
          .select("ConstituentId", "IsNewConstituent")
    )

    new_df = (
        df.join(donation_flags, "ConstituentId", "left")
          .join(dim_campaign, col("t.CampaignId") == col("dc.CampaignId"), "left")
          .join(dim_channel, col("t.ChannelId") == col("dch.ChannelId"), "left")
          .join(dim_const.hint("merge"), col("t.ConstituentId") == col("dcon.ConstituentId"), "left")
          .join(dim_source, col("t.SourceId") == col("ds.SourceId"), "left")
          .join(dim_don_source, col("t.TransactionSourceId").cast("string") == col("dds.DonationSourceId").cast("string"), "left")
          .join(fact_opp, col("t.OpportunityId") == col("fo.OpportunityId"), "left")
          .join(dim_date.alias("created"), to_date(col("t.CreatedDate")) == col("created.DimDate"), "left")
          .join(dim_date.alias("modified"), to_date(col("t.ModifiedDate")) == col("modified.DimDate"), "left")
          .join(dim_date.alias("don"), to_date(col("t.TransactionDate")) == col("don.DimDate"), "left")
          .withColumn(
              "DonationKey",
              xxhash64(
                  col("t.TransactionId"),
                  col("t.TransactionSourceId")
              ).cast("bigint")
          )
          .select(
              col("DonationKey"),
              col("t.TransactionId").alias("DonationId"),
              col("t.SourceSystemId").alias("SourceSysDonationId"),
              col("t.Name").alias("DonationName"),
              col("t.Amount"),
              col("t.IsRecurring").alias("IsReccuring"),
              col("IsNewConstituent"),
              col("t.Timezone"),
              col("dc.CampaignKey"),
              col("dch.ChannelKey"),
              col("dcon.ConstituentKey"),
              col("created.DateKey").alias("CreatedDateKey"),
              col("don.DateKey").alias("DonationDateKey"),
              col("modified.DateKey").alias("ModifiedDateKey"),
              col("fo.OpportunityKey"),
              col("ds.SourceKey"),
              col("dds.DonationSourceKey").alias("DonationSourceKey")
          )
    )

    logging.info(f"✅ FactDonation processing {new_df.count()} rows.")
    return new_df

factDonationTable = CdfTable(
    source_table_name="Transaction",
    target_table_name="FactDonation",
    source_primary_key="TransactionId",
    columns=[
        "TransactionId", "CampaignId", "ChannelId", "ConstituentId",
        "CreatedDate", "TransactionDate", "IsRecurring", "ModifiedDate",
        "Name", "OpportunityId", "SourceId", "SourceSystemId", "Timezone",
        "Amount", "TransactionSourceId"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.FactDonation AS target
    USING latestSnapshot_Transaction AS source
    ON target.DonationId = source.DonationId
    WHEN MATCHED THEN UPDATE SET
        Amount = source.Amount,
        CampaignKey = source.CampaignKey,
        ChannelKey = source.ChannelKey,
        ConstituentKey = source.ConstituentKey,
        CreatedDateKey = source.CreatedDateKey,
        DonationDateKey = source.DonationDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        OpportunityKey = source.OpportunityKey,
        DonationSourceKey = source.DonationSourceKey,
        SourceKey = source.SourceKey,
        SourceSysDonationId = source.SourceSysDonationId,
        DonationName = source.DonationName,
        IsReccuring = source.IsReccuring,
        IsNewConstituent = source.IsNewConstituent,
        Timezone = source.Timezone
    WHEN NOT MATCHED THEN INSERT (
        DonationKey, DonationId, Amount, CampaignKey, ChannelKey, ConstituentKey,
        CreatedDateKey, DonationDateKey, ModifiedDateKey, OpportunityKey,
        DonationSourceKey, SourceKey, SourceSysDonationId, DonationName,
        IsReccuring, IsNewConstituent, Timezone
    ) VALUES (
        source.DonationKey, source.DonationId, source.Amount, source.CampaignKey,
        source.ChannelKey, source.ConstituentKey, source.CreatedDateKey,
        source.DonationDateKey, source.ModifiedDateKey, source.OpportunityKey,
        source.DonationSourceKey, source.SourceKey, source.SourceSysDonationId,
        source.DonationName, source.IsReccuring, source.IsNewConstituent,
        source.Timezone
    );
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichFactDonation,
    hard_delete=True,
    delete_on="t.DonationId = k.TransactionId"
)

ProcessCdfTable(factDonationTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimVolunteeringType

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64

def EnrichDimVolunteeringType(df: DataFrame) -> DataFrame:

    # Lookup tables
    dim_date = get_gold_table("DimDate").select("Date", "DateKey")
    dim_source = get_gold_table("DimSource").select("SourceId", "SourceKey")

    # Date key lookups
    date_created = dim_date.select(
        col("Date").alias("CreatedDate_lookup"),
        col("DateKey").alias("CreatedDateKey")
    )
    date_modified = dim_date.select(
        col("Date").alias("ModifiedDate_lookup"),
        col("DateKey").alias("ModifiedDateKey")
    )

    # Enrichment logic
    new_df = (
        df
        .join(dim_source, on="SourceId", how="left")
        .join(date_created, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
        .join(date_modified, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
        .withColumn(
            "VolunteeringTypeKey",
            xxhash64(
                col("ParticipationTypeId"),
                col("SourceId"),
                col("Name")
            ).cast("bigint")
        )
        .select(
            "VolunteeringTypeKey",
            col("ParticipationTypeId").alias("VolunteeringTypeId"),
            col("Name").alias("VolunteeringTypeName"),
            "CreatedDateKey",
            "ModifiedDateKey",
            "ModifiedDate",
            "SourceKey"
        )
    )

    logging.info(f"✅ DimVolunteeringType processing {new_df.count()} rows.")

    return new_df

volunteeringTypeTable = CdfTable(
    source_table_name="ParticipationType",
    source_primary_key="ParticipationTypeId",
    target_table_name="DimVolunteeringType",
    columns=[
        "ParticipationTypeId",
        "Name",
        "CreatedDate",
        "ModifiedDate",
        "SourceId"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimVolunteeringType AS target
    USING latestSnapshot_ParticipationType AS source
    ON target.VolunteeringTypeId = source.VolunteeringTypeId
    WHEN MATCHED THEN UPDATE SET
        VolunteeringTypeName = source.VolunteeringTypeName,
        CreatedDateKey       = source.CreatedDateKey,
        ModifiedDateKey      = source.ModifiedDateKey,
        SourceKey            = source.SourceKey
    WHEN NOT MATCHED THEN INSERT (
        VolunteeringTypeKey,
        VolunteeringTypeId,
        VolunteeringTypeName,
        CreatedDateKey,
        ModifiedDateKey,
        SourceKey
    ) VALUES (
        source.VolunteeringTypeKey,
        source.VolunteeringTypeId,
        source.VolunteeringTypeName,
        source.CreatedDateKey,
        source.ModifiedDateKey,
        source.SourceKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimVolunteeringType
)

ProcessCdfTable(volunteeringTypeTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: FactVolunteerHours

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64
from pyspark.sql import DataFrame

def EnrichFactVolunteerHours(df: DataFrame) -> DataFrame:
    dim_constituent = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey")
    dim_event = get_gold_table("DimEvent").select("EventId", "EventKey")
    dim_vol_type = get_gold_table("DimVolunteeringType").select("VolunteeringTypeId", "VolunteeringTypeKey")
    dim_source = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dim_date = get_gold_table("DimDate").select("Date", "DateKey")

    volunteered_date_lkp = dim_date.select(col("Date").alias("VolunteeredDate_lookup"), col("DateKey").alias("VolunteeredDateKey"))
    created_date_lkp = dim_date.select(col("Date").alias("CreatedDate_lookup"), col("DateKey").alias("CreatedDateKey"))
    modified_date_lkp = dim_date.select(col("Date").alias("ModifiedDate_lookup"), col("DateKey").alias("ModifiedDateKey"))

    new_df = (
        df
        .join(dim_constituent.hint("merge"), "ConstituentId", "left")
        .join(dim_event, "EventId", "left")
        .join(dim_vol_type, df["ParticipationTypeId"] == dim_vol_type["VolunteeringTypeId"], "left")
        .join(dim_source, "SourceId", "left")
        .join(volunteered_date_lkp, expr("cast(StartDate as date) = VolunteeredDate_lookup"), "left")
        .join(created_date_lkp, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
        .join(modified_date_lkp, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
        .withColumn(
            "VolunteerHoursKey",
            xxhash64(
                col("ParticipationId"),
                col("SourceSystemId")
            ).cast("bigint")
        )
        .select(
            "VolunteerHoursKey",
            col("ParticipationId").alias("VolunteerHoursId"),
            col("SourceSystemId").alias("SourceSysVolunteerHoursId"),
            col("Hours").alias("HoursVolunteered"),
            col("ConstituentKey").alias("VolunteerKey"),
            "EventKey",
            "VolunteeringTypeKey",
            "VolunteeredDateKey",
            "CreatedDateKey",
            "ModifiedDateKey",
            "SourceKey",
            "Timezone"
        )
    )

    logging.info(f"✅ FactVolunteerHours processing {new_df.count()} rows.")

    return new_df

volunteerHoursTable = CdfTable(
    source_table_name = "Participation",
    target_table_name = "FactVolunteerHours",
    source_primary_key = "ParticipationId",
    columns = [
        "ParticipationId", "SourceSystemId", "ConstituentId", "EventId",
        "ParticipationTypeId", "Hours", "Timezone",
        "StartDate", "CreatedDate", "ModifiedDate", "SourceId"
    ],
    merge_sql_template = f"""
    MERGE INTO {gold_lakehouse_name}.FactVolunteerHours AS target
    USING latestSnapshot_Participation AS source
    ON target.VolunteerHoursId = source.VolunteerHoursId
    WHEN MATCHED THEN UPDATE SET
        SourceSysVolunteerHoursId = source.SourceSysVolunteerHoursId,
        HoursVolunteered = source.HoursVolunteered,
        VolunteerKey = source.VolunteerKey,
        EventKey = source.EventKey,
        VolunteeringTypeKey = source.VolunteeringTypeKey,
        VolunteeredDateKey = source.VolunteeredDateKey,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        SourceKey = source.SourceKey,
        Timezone = source.Timezone
    WHEN NOT MATCHED THEN INSERT (
        VolunteerHoursKey,
        VolunteerHoursId,
        SourceSysVolunteerHoursId,
        HoursVolunteered,
        VolunteerKey,
        EventKey,
        VolunteeringTypeKey,
        VolunteeredDateKey,
        CreatedDateKey,
        ModifiedDateKey,
        SourceKey,
        Timezone
    ) VALUES (
        source.VolunteerHoursKey,
        source.VolunteerHoursId,
        source.SourceSysVolunteerHoursId,
        source.HoursVolunteered,
        source.VolunteerKey,
        source.EventKey,
        source.VolunteeringTypeKey,
        source.VolunteeredDateKey,
        source.CreatedDateKey,
        source.ModifiedDateKey,
        source.SourceKey,
        source.Timezone
    )
    """,
    source_lakehouse = silver_lakehouse_name,
    target_lakehouse = gold_lakehouse_name,
    enrich_func      = EnrichFactVolunteerHours,
    hard_delete=True,
    delete_on="t.VolunteerHoursId = k.ParticipationId"
)

ProcessCdfTable(volunteerHoursTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: DimConstituentProgramBridge

# CELL ********************

from pyspark.sql.functions import col, xxhash64

def EnrichDimConstituentProgramBridge(df: DataFrame) -> DataFrame:
    dimConstituentDf = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey")
    dimProgramDf = get_gold_table("DimProgram").select("ProgramId", "ProgramKey")

    new_df = (
        df
        .join(dimConstituentDf, on="ConstituentId", how="left")
        .join(dimProgramDf, on="ProgramId", how="left")
        .withColumn(
            "ConstituentProgramBridgeKey",
            xxhash64(
                col("ConstituentId"),
                col("ProgramId")
            ).cast("bigint")
        )
        .select(
            "ConstituentProgramBridgeKey",
            col("ConstituentProgramId").alias("ConstituentProgramBridgeId"),
            "ConstituentKey",
            "ProgramKey"
        )
    )

    logging.info(f"✅ DimConstituentProgramBridge processing {new_df.count()} rows.")
    return new_df

constituentProgramBridgeTable = CdfTable(
    source_table_name="ConstituentProgram",
    target_table_name="DimConstituentProgramBridge",
    source_primary_key="ConstituentProgramId",
    columns=["ConstituentProgramId", "ConstituentId", "ProgramId", "SourceId", "CreatedDate", "ModifiedDate"],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.DimConstituentProgramBridge AS target
    USING latestSnapshot_ConstituentProgram AS source
    ON target.ConstituentProgramBridgeId = source.ConstituentProgramBridgeId
    WHEN MATCHED THEN UPDATE SET
        ConstituentKey = source.ConstituentKey,
        ProgramKey = source.ProgramKey
    WHEN NOT MATCHED THEN INSERT (
        ConstituentProgramBridgeKey, ConstituentProgramBridgeId, ConstituentKey, ProgramKey
    ) VALUES (
        source.ConstituentProgramBridgeKey, source.ConstituentProgramBridgeId, source.ConstituentKey, source.ProgramKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichDimConstituentProgramBridge,
    hard_delete=True,
    delete_on="t.ConstituentProgramBridgeId = k.ConstituentProgramId"
)

ProcessCdfTable(constituentProgramBridgeTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: FactSoftCredit

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64
from pyspark.sql import DataFrame

def EnrichFactSoftCredit(df: DataFrame) -> DataFrame:
    dimConstituentDf = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey")
    dimSourceDf = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimDateDf = get_gold_table("DimDate").select("Date", "DateKey")
    factDonationDf = get_gold_table("FactDonation").select("DonationId", "DonationKey")

    dimDateCreated = dimDateDf.select(
        col("Date").alias("CreatedDate_lookup"),
        col("DateKey").alias("CreatedDateKey")
    )
    dimDateModified = dimDateDf.select(
        col("Date").alias("ModifiedDate_lookup"),
        col("DateKey").alias("ModifiedDateKey")
    )

    new_df = (
        df
        .join(dimConstituentDf, on="ConstituentId", how="left")
        .join(dimSourceDf, on="SourceId", how="left")
        .join(factDonationDf, df["TransactionId"] == factDonationDf["DonationId"], how="left")
        .join(dimDateCreated, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
        .join(dimDateModified, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
        .withColumn(
            "SoftCreditKey",
            xxhash64(
                col("SoftCreditId"),
                col("SourceSystemId")
            ).cast("bigint")
        )
        .select(
            "SoftCreditKey",
            col("SoftCreditId"),
            col("SourceSystemId").alias("SourceSysSoftCreditId"),
            col("Amount").alias("SoftCreditAmount"),
            "ConstituentKey",
            "SourceKey",
            "DonationKey",
            "CreatedDateKey",
            "ModifiedDateKey"
        )
    )

    logging.info(f"✅ FactSoftCredit processing {new_df.count()} rows.")
    return new_df


softCreditTable = CdfTable(
    source_table_name="SoftCredit",
    target_table_name="FactSoftCredit",
    source_primary_key="SoftCreditId",
    columns=[
        "SoftCreditId", "SourceSystemId", "Amount",
        "ConstituentId", "SourceId", "TransactionId",
        "CreatedDate", "ModifiedDate"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.FactSoftCredit AS target
    USING latestSnapshot_SoftCredit AS source
    ON target.SoftCreditId = source.SoftCreditId
    WHEN MATCHED THEN UPDATE SET
        SourceSysSoftCreditId = source.SourceSysSoftCreditId,
        SoftCreditAmount = source.SoftCreditAmount,
        ConstituentKey = source.ConstituentKey,
        SourceKey = source.SourceKey,
        DonationKey = source.DonationKey,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey
    WHEN NOT MATCHED THEN INSERT (
        SoftCreditKey, SoftCreditId, SourceSysSoftCreditId,
        SoftCreditAmount, ConstituentKey, SourceKey,
        DonationKey, CreatedDateKey, ModifiedDateKey
    ) VALUES (
        source.SoftCreditKey, source.SoftCreditId, source.SourceSysSoftCreditId,
        source.SoftCreditAmount, source.ConstituentKey, source.SourceKey,
        source.DonationKey, source.CreatedDateKey, source.ModifiedDateKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichFactSoftCredit,
    hard_delete=True
)

ProcessCdfTable(softCreditTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Wealth screeening

# MARKDOWN ********************

# ### Build: FactWealthScreening

# CELL ********************

from pyspark.sql.functions import col, to_date, xxhash64

def EnrichFactWealthScreening(df: DataFrame) -> DataFrame:
    dimDateDf = get_gold_table("DimDate").select("Date", "DateKey")

    dimDateCreated = dimDateDf.select(
        col("Date").alias("CreatedDate_lookup"),
        col("DateKey").alias("CreatedDateKey")
    )

    dimDateModified = dimDateDf.select(
        col("Date").alias("ModifiedDate_lookup"),
        col("DateKey").alias("ModifiedDateKey")
    )

    dimDateLastScreening = dimDateDf.select(
        col("Date").alias("LastScreeningDate_lookup"),
        col("DateKey").alias("LastScreeningDateKey")
    )

    dim_source = get_gold_table("DimSource").select(
        col("SourceId"),
        col("SourceKey")
    )

    capacityrangeDf = get_table_from_lakehouse(silver_lakehouse_name, "WealthScreeningCapacityRange")
    constituentratingDf = get_table_from_lakehouse(silver_lakehouse_name, "WealthScreeningConstituentRating")
    dimConstituent = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey")

    wealthscreeningDf = df.alias("ws")  # ✅ alias on WealthScreening

    new_df = (
        wealthscreeningDf
        .join(capacityrangeDf, wealthscreeningDf.CapacityRangeId == capacityrangeDf.WealthScreeningCapacityRangeId, how="left")
        .join(constituentratingDf, wealthscreeningDf.ConstituentRatingId == constituentratingDf.WealthScreeningConstituentRatingId, how="left")
        .join(dim_source, "SourceId", "left")
        .join(dimConstituent, "ConstituentId", "left")
        .join(dimDateCreated, to_date(wealthscreeningDf["CreatedDate"]) == col("CreatedDate_lookup"), "left")
        .join(dimDateModified, to_date(wealthscreeningDf["ModifiedDate"]) == col("ModifiedDate_lookup"), "left")
        .join(dimDateLastScreening, to_date(wealthscreeningDf["LastScreeningDate"]) == col("LastScreeningDate_lookup"), "left")
        .withColumn(
            "WealthScreeningKey",
            xxhash64(
                col("ws.WealthScreeningId"),
                col("ws.SourceId")
            ).cast("bigint")
        )
        .select(
            "WealthScreeningKey",
            "WealthScreeningId",
            "ConstituentKey",
            "LastScreeningDateKey",
            constituentratingDf.Name.alias("ConstituentRating"),
            capacityrangeDf.Name.alias("CapacityRange"),
            "CreatedDateKey",
            "ModifiedDateKey",
            "SourceKey"
        )
    )

    logging.info(f"✅ FactWealthScreening processing {new_df.count()} rows.")
    return new_df


factWealthScreeningTable = CdfTable(
    source_table_name="WealthScreening",
    target_table_name="FactWealthScreening",
    source_primary_key="WealthScreeningId",
    columns=[
        "WealthScreeningId", "ConstituentId", "ConstituentRatingId",
        "CapacityRangeId", "LastScreeningDate",
        "CreatedDate", "ModifiedDate", "SourceId"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.FactWealthScreening AS target
    USING latestSnapshot_WealthScreening AS source
    ON target.WealthScreeningId = source.WealthScreeningId
    WHEN MATCHED THEN UPDATE SET
        ConstituentKey = source.ConstituentKey,
        ConstituentRating = source.ConstituentRating,
        CapacityRange = source.CapacityRange,
        LastScreeningDateKey = source.LastScreeningDateKey,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        SourceKey = source.SourceKey
    WHEN NOT MATCHED THEN INSERT (
        WealthScreeningKey, WealthScreeningId, ConstituentKey, ConstituentRating,
        CapacityRange, LastScreeningDateKey, CreatedDateKey, ModifiedDateKey, SourceKey
    ) VALUES (
        source.WealthScreeningKey, source.WealthScreeningId, source.ConstituentKey, source.ConstituentRating,
        source.CapacityRange, source.LastScreeningDateKey, source.CreatedDateKey, source.ModifiedDateKey, source.SourceKey
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichFactWealthScreening,
    hard_delete=True
)

ProcessCdfTable(factWealthScreeningTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Activities engagement

# MARKDOWN ********************

# ### Build: FactConstituentEmailEngagement

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64, to_date

def EnrichFactConstituentEmailEngagement(df: DataFrame) -> DataFrame:
    df = df.alias("cee")

    dim_constituent = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey").alias("dcon")
    
    # 🆕 Pull ChannelKey from DimEmail
    dim_email = get_gold_table("DimEmail").select("EmailEngagementId", "EmailId", "EmailKey", "ChannelKey")

    dim_campaign = get_gold_table("DimCampaign").select("CampaignId", "CampaignKey").alias("dc")
    dim_source = get_gold_table("DimSource").select("SourceId", "SourceKey").alias("ds")
    dim_date = get_gold_table("DimDate").select("Date", "DateKey").alias("dd")
    email_engagement = get_silver_table("EmailEngagement").select("EmailEngagementId", "CampaignId").alias("ee")

    # Date joins
    click_lkp = dim_date.select(col("Date").alias("ClickThroughDate_lookup"), col("DateKey").alias("ClickThroughDateKey"))
    open_lkp  = dim_date.select(col("Date").alias("OpenedDate_lookup"), col("DateKey").alias("OpenedDateKey"))
    sent_lkp  = dim_date.select(col("Date").alias("SendDate_lookup"), col("DateKey").alias("SentDateKey"))
    creat_lkp = dim_date.select(col("Date").alias("CreatedDate_lookup"), col("DateKey").alias("CreatedDateKey"))
    mod_lkp   = dim_date.select(col("Date").alias("ModifiedDate_lookup"), col("DateKey").alias("ModifiedDateKey"))

    new_df = (
        df
        .join(dim_constituent.hint("merge"), "ConstituentId", "left")
        .join(dim_email, "EmailEngagementId", "left")                         
        .join(email_engagement, "EmailEngagementId", "left")
        .join(dim_campaign, "CampaignId", "left")
        .join(dim_source, "SourceId", "left")
        .join(click_lkp, expr("cast(ClickThroughDate as date) = ClickThroughDate_lookup"), "left")
        .join(open_lkp, expr("cast(OpenedDate as date) = OpenedDate_lookup"), "left")
        .join(sent_lkp, expr("cast(SendDate as date) = SendDate_lookup"), "left")
        .join(creat_lkp, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
        .join(mod_lkp, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
        .withColumn(
            "ConstituentEmailEngagementKey",
            xxhash64(
                col("cee.ConstituentEmailEngagementId"),
                col("cee.SourceSystemId")
            ).cast("bigint")
        )
        .select(
            "ConstituentEmailEngagementKey",
            col("cee.ConstituentEmailEngagementId"),
            col("cee.EmailEngagementId"),
            col("cee.SourceSystemId").alias("SourceSysConstituentEmailEngagementId"),
            col("cee.ConstituentEmail"),
            "CampaignKey",
            "ChannelKey",     # ✅ from DimEmail
            "WasOpened",
            "ClickThrough",
            "Timezone",
            "ConstituentKey",
            "EmailKey",
            "SourceKey",
            "ClickThroughDateKey",
            "OpenedDateKey",
            "SentDateKey",
            "CreatedDateKey",
            "ModifiedDateKey"
        )
    )

    logging.info(f"✅ FactConstituentEmailEngagement processing {new_df.count()} rows.")
    return new_df



factCEE_Table = CdfTable(
    source_table_name="ConstituentEmailEngagement",
    target_table_name="FactConstituentEmailEngagement",
    source_primary_key="ConstituentEmailEngagementId",
    columns=[
        "ConstituentEmailEngagementId",
        "ConstituentEmail",
        "ConstituentId",
        "EmailId",
        "EmailEngagementId",
        "ClickThrough",
        "ClickThroughDate",
        "OpenedDate",
        "SendDate",
        "CreatedDate",
        "ModifiedDate",
        "WasOpened",
        "SourceSystemId",
        "SourceId",
        "Timezone"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.FactConstituentEmailEngagement AS target
    USING latestSnapshot_ConstituentEmailEngagement AS source
    ON target.ConstituentEmailEngagementId = source.ConstituentEmailEngagementId
    WHEN MATCHED THEN UPDATE SET
        ConstituentEmail = source.ConstituentEmail,
        WasOpened = source.WasOpened,
        ClickThrough = source.ClickThrough,
        SourceSysConstituentEmailEngagementId = source.SourceSysConstituentEmailEngagementId,
        ConstituentKey = source.ConstituentKey,
        CampaignKey = source.CampaignKey,   
        ChannelKey = source.ChannelKey,
        EmailKey = source.EmailKey,
        EmailEngagementId = source.EmailEngagementId,
        SourceKey = source.SourceKey,
        ClickThroughDateKey = source.ClickThroughDateKey,
        OpenedDateKey = source.OpenedDateKey,
        SentDateKey = source.SentDateKey,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        Timezone = source.Timezone
    WHEN NOT MATCHED THEN INSERT (
        ConstituentEmailEngagementKey,
        ConstituentEmailEngagementId,
        ConstituentEmail,
        WasOpened,
        ClickThrough,
        SourceSysConstituentEmailEngagementId,
        ConstituentKey,
        CampaignKey,
        ChannelKey,
        EmailKey,
        EmailEngagementId,
        SourceKey,
        ClickThroughDateKey,
        OpenedDateKey,
        SentDateKey,
        CreatedDateKey,
        ModifiedDateKey,
        Timezone
    ) VALUES (
        source.ConstituentEmailEngagementKey,
        source.ConstituentEmailEngagementId,
        source.ConstituentEmail,
        source.WasOpened,
        source.ClickThrough,
        source.SourceSysConstituentEmailEngagementId,
        source.ConstituentKey,
        source.CampaignKey,
        source.ChannelKey,
        source.EmailKey,
        source.EmailEngagementId,
        source.SourceKey,
        source.ClickThroughDateKey,
        source.OpenedDateKey,
        source.SentDateKey,
        source.CreatedDateKey,
        source.ModifiedDateKey,
        source.Timezone
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichFactConstituentEmailEngagement,
    hard_delete=True
)

ProcessCdfTable(factCEE_Table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: FactConstituentLetterEngagement

# CELL ********************

from pyspark.sql.functions import to_date, col, xxhash64

def EnrichFactConstituentLetterEngagement(df: DataFrame) -> DataFrame:
    dimDateDf = get_gold_table("DimDate").select("Date", "DateKey")
    dimConstituentDf = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey")
    dimCampaignDf = get_gold_table("DimCampaign").select("CampaignId", "CampaignKey")
    dimChannelDf = get_gold_table("DimChannel").select("ChannelId", "ChannelKey")  # NEW
    dimLetterDf = get_gold_table("DimLetter").select("LetterId", "LetterKey")
    
    date_sent_df = dimDateDf.select(
        col("Date").alias("SentDate_lookup"),
        col("DateKey").alias("DateSent")
    )

    df = df.withColumn("SentDate_casted", to_date("SentDate"))

    new_df = (
        df
        .join(dimConstituentDf, on="ConstituentId", how="left")
        .join(dimCampaignDf, on="CampaignId", how="left")
        .join(dimChannelDf, on="ChannelId", how="left")  # NEW
        .join(dimLetterDf, on="LetterId", how="left")
        .join(date_sent_df, col("SentDate_casted") == col("SentDate_lookup"), "left")
        .withColumn(
            "ConstituentLetterEngagementKey",
            xxhash64(col("LetterId"), col("ConstituentId"), col("CampaignId")).cast("bigint")
        )
        .withColumn("ConstituentLetterEngagementId", col("LetterId"))
        .withColumn("SourceSysConstituentLetterEngagementId", col("LetterId"))
        .select(
            "ConstituentLetterEngagementKey",
            "ConstituentLetterEngagementId",
            "SourceSysConstituentLetterEngagementId",
            "DateSent",
            "ConstituentKey",
            "CampaignKey",
            "ChannelKey",  
            "LetterKey",
            "Timezone"
        )
    )

    logging.info(f"✅ FactConstituentLetterEngagement processing {new_df.count()} rows.")
    return new_df


factEngagementTable = CdfTable(
    source_table_name="Letter",
    target_table_name="FactConstituentLetterEngagement",
    source_primary_key="LetterId",
    columns=["LetterId", "SentDate", "ConstituentId", "CampaignId", "ChannelId", "Timezone"],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.FactConstituentLetterEngagement AS target
    USING latestSnapshot_Letter AS source
    ON target.ConstituentLetterEngagementId = source.ConstituentLetterEngagementId
    WHEN MATCHED THEN UPDATE SET
        SourceSysConstituentLetterEngagementId = source.SourceSysConstituentLetterEngagementId,
        DateSent = source.DateSent,
        ConstituentKey = source.ConstituentKey,
        CampaignKey = source.CampaignKey,
        ChannelKey = source.ChannelKey,
        LetterKey = source.LetterKey,
        Timezone = source.Timezone
    WHEN NOT MATCHED THEN INSERT (
        ConstituentLetterEngagementKey, ConstituentLetterEngagementId,
        SourceSysConstituentLetterEngagementId, DateSent, ConstituentKey,
        CampaignKey, ChannelKey, LetterKey, Timezone
    ) VALUES (
        source.ConstituentLetterEngagementKey, source.ConstituentLetterEngagementId,
        source.SourceSysConstituentLetterEngagementId, source.DateSent,
        source.ConstituentKey, source.CampaignKey, source.ChannelKey, source.LetterKey, source.Timezone
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichFactConstituentLetterEngagement,
    hard_delete=True,
    delete_on="t.ConstituentLetterEngagementId = k.LetterId"
)

ProcessCdfTable(factEngagementTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: FactConstituentPhonecallEngagement

# CELL ********************

from pyspark.sql.functions import col, xxhash64, expr
from pyspark.sql import DataFrame

def EnrichFactConstituentPhonecallEngagement(df: DataFrame) -> DataFrame:
    # Load dimension tables
    dimConstituentDf = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey")
    dimCampaignDf = get_gold_table("DimCampaign").select("CampaignId", "CampaignKey")
    dimPhonecallDf = get_gold_table("DimPhonecall").select("PhonecallId", "PhonecallKey")
    dimChannelDf = get_gold_table("DimChannel").select("ChannelId", "ChannelKey")
    dimSourceDf = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimDateDf = get_gold_table("DimDate").select("Date", "DateKey")

    # Date lookups
    dimCallDate = dimDateDf.select(col("Date").alias("CallDate_lookup"), col("DateKey").alias("PhonecallDate"))
    dimCreatedDate = dimDateDf.select(col("Date").alias("CreatedDate_lookup"), col("DateKey").alias("CreatedDateKey"))
    dimModifiedDate = dimDateDf.select(col("Date").alias("ModifiedDate_lookup"), col("DateKey").alias("ModifiedDateKey"))

    # Enrichment
    new_df = (
        df
        .join(dimConstituentDf, on="ConstituentId", how="left")
        .join(dimCampaignDf, on="CampaignId", how="left")
        .join(dimChannelDf, on="ChannelId", how="left")
        .join(dimPhonecallDf, on="PhonecallId", how="left")
        .join(dimSourceDf, on="SourceId", how="left")
        .join(dimCallDate, expr("cast(CallDate as date) = CallDate_lookup"), "left")
        .join(dimCreatedDate, expr("cast(CreatedDate as date) = CreatedDate_lookup"), "left")
        .join(dimModifiedDate, expr("cast(ModifiedDate as date) = ModifiedDate_lookup"), "left")
        .withColumn(
            "ConstituentPhonecallEngagementKey",
            xxhash64(col("PhonecallId"), col("ConstituentId"), col("CampaignId")).cast("bigint")
        )
        .select(
            "ConstituentPhonecallEngagementKey",
            col("PhonecallId").alias("ConstituentPhonecallEngagementId"),
            col("SourceSystemId").alias("SourceSysConstituentPhonecallEngagementId"),
            "ConstituentKey",
            "CampaignKey",
            "ChannelKey",
            "PhonecallKey",
            "PhonecallDate",
            "CreatedDateKey",
            "ModifiedDateKey",
            "SourceKey",
            "Timezone"
        )
    )

    logging.info(f"✅ FactConstituentPhonecallEngagement processing {new_df.count()} rows.")

    return new_df



constituentPhonecallEngagementTable = CdfTable(
    source_table_name="Phonecall",
    source_primary_key="PhonecallId",
    target_table_name="FactConstituentPhonecallEngagement",
    columns=[
        "PhonecallId", "SourceSystemId", "ConstituentId", "CampaignId", "ChannelId", "CallDate",
        "CreatedDate", "ModifiedDate", "SourceId", "Timezone"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.FactConstituentPhonecallEngagement AS target
    USING latestSnapshot_Phonecall AS source
    ON target.ConstituentPhonecallEngagementId = source.ConstituentPhonecallEngagementId
    WHEN MATCHED THEN UPDATE SET
        SourceSysConstituentPhonecallEngagementId = source.SourceSysConstituentPhonecallEngagementId,
        ConstituentKey = source.ConstituentKey,
        CampaignKey = source.CampaignKey,
        ChannelKey = source.ChannelKey,
        PhonecallKey = source.PhonecallKey,
        PhonecallDate = source.PhonecallDate,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        SourceKey = source.SourceKey,
        Timezone = source.Timezone
    WHEN NOT MATCHED THEN INSERT (
        ConstituentPhonecallEngagementKey, ConstituentPhonecallEngagementId,
        SourceSysConstituentPhonecallEngagementId, ConstituentKey, CampaignKey, ChannelKey,
        PhonecallKey, PhonecallDate, CreatedDateKey, ModifiedDateKey, SourceKey, Timezone
    ) VALUES (
        source.ConstituentPhonecallEngagementKey, source.ConstituentPhonecallEngagementId,
        source.SourceSysConstituentPhonecallEngagementId, source.ConstituentKey, source.CampaignKey, source.ChannelKey,
        source.PhonecallKey, source.PhonecallDate, source.CreatedDateKey, source.ModifiedDateKey, source.SourceKey, source.Timezone
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichFactConstituentPhonecallEngagement,
    hard_delete=True,
    delete_on="t.ConstituentPhonecallEngagementId = k.PhonecallId"
)

ProcessCdfTable(constituentPhonecallEngagementTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: FactSocialEngagement

# CELL ********************

from pyspark.sql.functions import col, to_date, xxhash64
from pyspark.sql import DataFrame

def EnrichFactSocialEngagement(df: DataFrame) -> DataFrame:
    # Load dimension tables
    dimDate = get_gold_table("DimDate").select(col("Date").alias("DimDate"), col("DateKey"))
    dimCampaign = get_gold_table("DimCampaign").select("CampaignId", "CampaignKey")
    dimPlatform = get_gold_table("DimEngagementPlatform").select("EngagementPlatformId", "EngagementPlatformKey")
    dimSource = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dimChannel = get_gold_table("DimChannel").select("ChannelId", "ChannelKey")  

    # Date lookups
    dateCreated = dimDate.withColumnRenamed("DimDate", "CreatedDate_lookup").withColumnRenamed("DateKey", "CreatedDateKey")
    dateModified = dimDate.withColumnRenamed("DimDate", "ModifiedDate_lookup").withColumnRenamed("DateKey", "ModifiedDateKey")
    dateInteraction = dimDate.withColumnRenamed("DimDate", "InteractionDate_lookup").withColumnRenamed("DateKey", "InteractionDateKey")
    dateReport = dimDate.withColumnRenamed("DimDate", "ReportDate_lookup").withColumnRenamed("DateKey", "ReportDateKey")

    # Join and enrich
    new_df = (
        df
        .withColumnRenamed("Platform", "EngagementPlatformId")
        .join(dimCampaign, "CampaignId", "left")
        .join(dimPlatform, "EngagementPlatformId", "left")
        .join(dimSource, "SourceId", "left")
        .join(dimChannel, "ChannelId", "left") 
        .join(dateCreated, to_date("CreatedDate") == col("CreatedDate_lookup"), "left")
        .join(dateModified, to_date("ModifiedDate") == col("ModifiedDate_lookup"), "left")
        .join(dateInteraction, to_date("InteractionDate") == col("InteractionDate_lookup"), "left")
        .join(dateReport, to_date("ReportDate") == col("ReportDate_lookup"), "left")
        .withColumn(
            "FactSocialEngagementKey",
            xxhash64(col("SocialEngagementId"), col("SourceSystemId")).cast("bigint")
        )
        .select(
            "FactSocialEngagementKey",
            "SocialEngagementId",
            col("SourceSystemId").alias("SourceSocialEngagementId"),
            "CampaignKey",
            "EngagementPlatformKey",
            "ChannelKey",
            "SourceKey",
            "CreatedDateKey",
            "ModifiedDateKey",
            "InteractionDateKey",
            "ReportDateKey",
            "PostId",
            "Clicks",
            "Shares",
            "Comments",
            "Likes",
            "Impressions",
            "Reach",
            "Engagements"
        )
    )

    logging.info(f"✅ FactSocialEngagement processing {new_df.count()} rows.")
    return new_df


factSocialEngagementTable = CdfTable(
    source_table_name="SocialEngagement",
    source_primary_key="SocialEngagementId",
    target_table_name="FactSocialEngagement",
    columns=[
        "SocialEngagementId", "CampaignId", "SourceSystemId", "SourceId", "Platform", "PostId", "ChannelId",
        "Clicks", "Shares", "Comments", "Likes", "Impressions", "Reach", "Engagements",
        "CreatedDate", "ModifiedDate", "InteractionDate", "ReportDate"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.FactSocialEngagement AS target
    USING latestSnapshot_SocialEngagement AS source
    ON target.SocialEngagementId = source.SocialEngagementId
    WHEN MATCHED THEN UPDATE SET
        CampaignKey = source.CampaignKey,
        EngagementPlatformKey = source.EngagementPlatformKey,
        ChannelKey = source.ChannelKey,
        SourceKey = source.SourceKey,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        InteractionDateKey = source.InteractionDateKey,
        ReportDateKey = source.ReportDateKey,
        PostId = source.PostId,
        Clicks = source.Clicks,
        Shares = source.Shares,
        Comments = source.Comments,
        Likes = source.Likes,
        Impressions = source.Impressions,
        Reach = source.Reach,
        Engagements = source.Engagements
    WHEN NOT MATCHED THEN INSERT (
        FactSocialEngagementKey, SocialEngagementId, SourceSocialEngagementId,
        CampaignKey, EngagementPlatformKey, ChannelKey, SourceKey,
        CreatedDateKey, ModifiedDateKey, InteractionDateKey, ReportDateKey,
        PostId, Clicks, Shares, Comments, Likes, Impressions, Reach, Engagements
    ) VALUES (
        source.FactSocialEngagementKey, source.SocialEngagementId, source.SourceSocialEngagementId,
        source.CampaignKey, source.EngagementPlatformKey, source.ChannelKey, source.SourceKey,
        source.CreatedDateKey, source.ModifiedDateKey, source.InteractionDateKey, source.ReportDateKey,
        source.PostId, source.Clicks, source.Shares, source.Comments, source.Likes,
        source.Impressions, source.Reach, source.Engagements
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichFactSocialEngagement,
    hard_delete=True
)

ProcessCdfTable(factSocialEngagementTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: FactConstituentSocialEngagement

# CELL ********************

from pyspark.sql.functions import col, expr, xxhash64

def EnrichConstituentSocialEngagement(df: DataFrame) -> DataFrame:
    # Gold lookups
    dim_const = get_gold_table("DimConstituent").select("ConstituentId", "ConstituentKey")
    dim_source = get_gold_table("DimSource").select("SourceId", "SourceKey")
    dim_date = get_gold_table("DimDate").select("Date", "DateKey")

    # 🆕 Join to FactSocialEngagement to get ChannelKey
    fact_social = get_gold_table("FactSocialEngagement").select(
        "SocialEngagementId", "ChannelKey"
    )

    # Date lookups
    date_lkp     = dim_date.select(col("Date").alias("Date_lookup"),     col("DateKey").alias("DateKey"))
    created_lkp  = dim_date.select(col("Date").alias("Created_lookup"),  col("DateKey").alias("CreatedDateKey"))
    modified_lkp = dim_date.select(col("Date").alias("Modified_lookup"), col("DateKey").alias("ModifiedDateKey"))

    # Join and enrich
    new_df = (
        df.join(dim_const, "ConstituentId", "left")
          .join(dim_source, "SourceId", "left")
          .join(fact_social, "SocialEngagementId", "left")  
          .join(date_lkp,     expr("cast(Date as date) = Date_lookup"), "left")
          .join(created_lkp,  expr("cast(CreatedDate as date) = Created_lookup"), "left")
          .join(modified_lkp, expr("cast(ModifiedDate as date) = Modified_lookup"), "left")
          .withColumn(
              "ConstituentSocialEngagementKey",
              xxhash64(
                  col("ConstituentSocialEngagementId"),
                  col("SourceSystemId")
              ).cast("bigint")
          )
          .withColumn("SourceSysConstituentSocialEngagementId", col("SourceSystemId"))
          .select(
              "ConstituentSocialEngagementKey",
              "ConstituentSocialEngagementId",
              "SocialEngagementId",
              "SourceSysConstituentSocialEngagementId",
              "ConstituentKey",
              "ChannelKey",  
              "DateKey",
              "Impression",
              "Share",
              "Comment",
              "Like",
              "CreatedDateKey",
              "ModifiedDateKey",
              "SourceKey",
              "Timezone"
          )
    )

    logging.info(f"✅ FactConstituentSocialEngagement processing {new_df.count()} rows.")
    return new_df



constituentSocialEngagementTable = CdfTable(
    source_table_name="ConstituentSocialEngagement",
    source_primary_key="ConstituentSocialEngagementId",
    target_table_name="FactConstituentSocialEngagement",
    columns=[
        "ConstituentSocialEngagementId","SocialEngagementId","ConstituentId",
        "Date","Impression","Share","Comment","Like",
        "CreatedDate","ModifiedDate","SourceId","SourceSystemId","Timezone"
    ],
    merge_sql_template=f"""
    MERGE INTO {gold_lakehouse_name}.FactConstituentSocialEngagement AS target
    USING latestSnapshot_ConstituentSocialEngagement AS source
    ON target.ConstituentSocialEngagementId = source.ConstituentSocialEngagementId
    WHEN MATCHED THEN UPDATE SET
        SourceSysConstituentSocialEngagementId = source.SourceSysConstituentSocialEngagementId,
        ConstituentKey = source.ConstituentKey,
        ChannelKey = source.ChannelKey,
        DateKey = source.DateKey,
        Impression = source.Impression,
        Share = source.Share,
        Comment = source.Comment,
        Like = source.Like,
        CreatedDateKey = source.CreatedDateKey,
        ModifiedDateKey = source.ModifiedDateKey,
        SourceKey = source.SourceKey,
        Timezone = source.Timezone
    WHEN NOT MATCHED THEN INSERT (
        ConstituentSocialEngagementKey,
        ConstituentSocialEngagementId,
        SocialEngagementId,
        SourceSysConstituentSocialEngagementId,
        ConstituentKey,
        ChannelKey,
        DateKey,
        Impression,
        Share,
        Comment,
        Like,
        CreatedDateKey,
        ModifiedDateKey,
        SourceKey,
        Timezone
    ) VALUES (
        source.ConstituentSocialEngagementKey,
        source.ConstituentSocialEngagementId,
        source.SocialEngagementId,
        source.SourceSysConstituentSocialEngagementId,
        source.ConstituentKey,
        source.ChannelKey,
        source.DateKey,
        source.Impression,
        source.Share,
        source.Comment,
        source.Like,
        source.CreatedDateKey,
        source.ModifiedDateKey,
        source.SourceKey,
        source.Timezone
    )
    """,
    source_lakehouse=silver_lakehouse_name,
    target_lakehouse=gold_lakehouse_name,
    enrich_func=EnrichConstituentSocialEngagement,
    hard_delete=True
)

ProcessCdfTable(constituentSocialEngagementTable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Datamart tables

# MARKDOWN ********************

# ### Build: dm_Constituent

# CELL ********************

df_constituent = spark.sql(f"""
    WITH DonationStats AS (
        SELECT 
            ConstituentKey,
            CAST(SUM(Amount) AS DECIMAL(18,4)) AS LifetimeDonationAmount,
            MIN(DonationDateKey) AS FirstDonationDateKey,
            MAX(DonationDateKey) AS LastDonationDateKey
        FROM {gold_lakehouse_name}.FactDonation
        GROUP BY ConstituentKey
    ),
    EventStats AS (
        SELECT 
            ConstituentKey,
            COUNT(*) AS AttendedEventsCount
        FROM {gold_lakehouse_name}.FactEventAttendance
        WHERE AttendedEvent = true
        GROUP BY ConstituentKey
    ),
    EngagementStats AS (
        SELECT ConstituentKey, MIN(EngagementDate) AS FirstEngagementDateKey
        FROM (
            SELECT ea.ConstituentKey, MIN(e.EventDateKey) AS EngagementDate
            FROM {gold_lakehouse_name}.FactEventAttendance ea
            JOIN {gold_lakehouse_name}.DimEvent e ON e.EventKey = ea.EventKey
            GROUP BY ea.ConstituentKey

            UNION ALL

            SELECT VolunteerKey AS ConstituentKey, MIN(VolunteeredDateKey)
            FROM {gold_lakehouse_name}.FactVolunteerHours
            GROUP BY VolunteerKey

            UNION ALL

            SELECT ConstituentKey, MIN(DonationDateKey)
            FROM {gold_lakehouse_name}.FactDonation
            GROUP BY ConstituentKey

            UNION ALL

            SELECT ConstituentKey, MIN(DateKey)
            FROM {gold_lakehouse_name}.FactConstituentSocialEngagement
            GROUP BY ConstituentKey

            UNION ALL

            SELECT ConstituentKey, MIN(SentDateKey)
            FROM {gold_lakehouse_name}.FactConstituentEmailEngagement
            GROUP BY ConstituentKey
        ) AS EngagementUnion
        GROUP BY ConstituentKey
    ),
    OpportunityStats AS (
        SELECT ConstituentKey
        FROM {gold_lakehouse_name}.FactOpportunity fo
        LEFT JOIN {gold_lakehouse_name}.DimOpportunityType do
            ON fo.OpportunityTypeKey = do.OpportunityTypeKey
        WHERE OpportunityTypeName = 'Pledge' AND CloseDateKey IS NOT NULL
        GROUP BY ConstituentKey
    ),
    ContactInfo AS (
        SELECT
            g.ConstituentKey,
            c.BirthDate,
            CAST(FLOOR(DATEDIFF(CURRENT_DATE(), c.BirthDate) / 365.25) AS BIGINT) AS Age
        FROM {gold_lakehouse_name}.DimConstituent g
        JOIN {silver_lakehouse_name}.Constituent s ON g.ConstituentId = s.ConstituentId
        JOIN {silver_lakehouse_name}.Contact c ON s.ContactId = c.ContactId
    ),
    FirstEngagementChannel AS (
    SELECT ConstituentKey, ChannelName AS AcquisitionChannel
    FROM (
        SELECT 
            ce.ConstituentKey,
            ce.ChannelName,
            dd.Date,
            ROW_NUMBER() OVER (PARTITION BY ce.ConstituentKey ORDER BY dd.Date ASC) AS rn
        FROM (
            SELECT ConstituentKey, SentDateKey AS DateKey, 'Email' AS ChannelName FROM {gold_lakehouse_name}.FactConstituentEmailEngagement
            UNION ALL
            SELECT ConstituentKey, DateKey, 'Social Media' AS ChannelName FROM {gold_lakehouse_name}.FactConstituentSocialEngagement
            UNION ALL
            SELECT ConstituentKey, DateSentKey AS DateKey, 'Direct Mail' AS ChannelName FROM {gold_lakehouse_name}.FactConstituentLetterEngagement
            UNION ALL
            SELECT ConstituentKey, PhonecallDate AS DateKey, 'Phone Call' AS ChannelName FROM {gold_lakehouse_name}.FactConstituentPhonecallEngagement
            UNION ALL
            SELECT fa.ConstituentKey, e.EventDateKey AS DateKey, 'Events' AS ChannelName
            FROM {gold_lakehouse_name}.FactEventAttendance fa
            JOIN {gold_lakehouse_name}.DimEvent e ON fa.EventKey = e.EventKey
        ) ce
        JOIN {gold_lakehouse_name}.DimDate dd ON ce.DateKey = dd.DateKey
    ) ranked
    WHERE rn = 1
)

    SELECT
        dc.ConstituentKey,
        dc.ConstituentId,
        dc.ConstituentName,
        dc.Email,
        ci.Age,
        fc.AcquisitionChannel,

        CAST(ds.LifetimeDonationAmount AS DECIMAL(18,4)) AS LifetimeDonationAmount,
        ds.FirstDonationDateKey,
        ds.LastDonationDateKey,

        es.AttendedEventsCount,
        eg.FirstEngagementDateKey,

        CAST(
            CASE 
                WHEN dd_fd.Date >= DATEADD(month, -12, CURRENT_DATE())
                    AND dd_fd.Date IS NOT NULL THEN 1 
                ELSE 0 
            END AS BOOLEAN
        ) AS IsNewDonor,

        CASE 
            WHEN ds.LifetimeDonationAmount IS NOT NULL OR o.ConstituentKey IS NOT NULL THEN 'Conversion'
            WHEN eg.FirstEngagementDateKey IS NOT NULL THEN 'Engagement'
            WHEN EXISTS (
                SELECT 1 FROM {gold_lakehouse_name}.FactConstituentEmailEngagement e 
                WHERE e.ConstituentKey = dc.ConstituentKey
            ) OR EXISTS (
                SELECT 1 FROM {gold_lakehouse_name}.FactConstituentSocialEngagement s 
                WHERE s.ConstituentKey = dc.ConstituentKey
            ) THEN 'Awareness'
            ELSE 'Unengaged'
        END AS EngagementStage

    FROM {gold_lakehouse_name}.DimConstituent dc
    LEFT JOIN DonationStats ds ON ds.ConstituentKey = dc.ConstituentKey
    LEFT JOIN EventStats es ON es.ConstituentKey = dc.ConstituentKey
    LEFT JOIN EngagementStats eg ON eg.ConstituentKey = dc.ConstituentKey
    LEFT JOIN OpportunityStats o ON o.ConstituentKey = dc.ConstituentKey
    LEFT JOIN {gold_lakehouse_name}.DimDate dd_fd ON dd_fd.DateKey = ds.FirstDonationDateKey
    LEFT JOIN ContactInfo ci ON dc.ConstituentKey = ci.ConstituentKey
    LEFT JOIN FirstEngagementChannel fc ON dc.ConstituentKey = fc.ConstituentKey
""")

df_constituent.write.mode("overwrite").format("delta").saveAsTable(f"{gold_lakehouse_name}.dm_Constituent")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: dm_EngagementTimeline

# CELL ********************

df_engagementtimeline = spark.sql(f"""
WITH engagement_union AS (
    SELECT
        cee.ConstituentKey,
        cee.SentDateKey AS EngagementDate,
        ch.ChannelKey,
        cee.CampaignKey,
        CASE 
            WHEN cee.ClickThrough = 1 THEN 'Email Click'
            WHEN cee.WasOpened = 1 THEN 'Email Open'
            ELSE 'Email Sent'
        END AS EngagementType,
        dc.EngagementStage,
        de.EmailId AS InteractionId,
        CASE
            WHEN dcc.FirstDonationDateKey IS NOT NULL
             AND dcc.FirstDonationDateKey >= cee.SentDateKey THEN TRUE
            ELSE FALSE
        END AS WasConverted
    FROM {gold_lakehouse_name}.FactConstituentEmailEngagement cee
    JOIN {gold_lakehouse_name}.DimChannel ch ON ch.ChannelName = 'Email'
    JOIN {gold_lakehouse_name}.DimConstituent dc ON cee.ConstituentKey = dc.ConstituentKey
    JOIN {gold_lakehouse_name}.DimEmail de ON cee.EmailKey = de.EmailKey
    JOIN {gold_lakehouse_name}.dm_Constituent dcc ON cee.ConstituentKey = dcc.ConstituentKey

   UNION ALL

   SELECT
       cse.ConstituentKey,
       cse.DateKey AS EngagementDate,
       ch.ChannelKey,
       NULL AS CampaignKey,
       CASE 
           WHEN cse.`Like` THEN 'Social Like'
           WHEN cse.Share THEN 'Social Share'
           WHEN cse.`Comment` THEN 'Social Comment'
           WHEN cse.Impression THEN 'Social Impression'
           ELSE 'Social View'
       END AS EngagementType,
       dc.EngagementStage,
       cse.SocialEngagementId AS InteractionId,
       CASE
           WHEN dcc.FirstDonationDateKey IS NOT NULL
            AND dcc.FirstDonationDateKey >= cse.DateKey THEN TRUE
           ELSE FALSE
       END AS WasConverted
   FROM {gold_lakehouse_name}.FactConstituentSocialEngagement cse
   JOIN {gold_lakehouse_name}.DimChannel ch ON ch.ChannelName = 'Social Media'
   JOIN {gold_lakehouse_name}.DimConstituent dc ON cse.ConstituentKey = dc.ConstituentKey
   JOIN {gold_lakehouse_name}.dm_Constituent dcc ON cse.ConstituentKey = dcc.ConstituentKey
    UNION ALL

    SELECT
        ea.ConstituentKey,
        e.EventDateKey AS EngagementDate,
        ch.ChannelKey,
        NULL AS CampaignKey,
        CASE 
            WHEN ea.AttendedEvent = 1 THEN 'Event Attended'
            WHEN ea.AcceptedDateKey IS NOT NULL THEN 'Event Accepted'
            WHEN ea.InvitationDateKey IS NOT NULL THEN 'Event Invited'
            ELSE 'Event Registered'
        END AS EngagementType,
        dc.EngagementStage,
        e.EventId AS InteractionId,
        CASE
            WHEN dcc.FirstDonationDateKey IS NOT NULL
             AND dcc.FirstDonationDateKey >= e.EventDateKey THEN TRUE
            ELSE FALSE
        END AS WasConverted
    FROM {gold_lakehouse_name}.FactEventAttendance ea
    JOIN {gold_lakehouse_name}.DimEvent e ON ea.EventKey = e.EventKey
    JOIN {gold_lakehouse_name}.DimChannel ch ON ch.ChannelName = 'Events'
    JOIN {gold_lakehouse_name}.DimConstituent dc ON ea.ConstituentKey = dc.ConstituentKey
    JOIN {gold_lakehouse_name}.dm_Constituent dcc ON ea.ConstituentKey = dcc.ConstituentKey

    UNION ALL

    SELECT
        cle.ConstituentKey,
        cle.DateSent AS EngagementDate,
        ch.ChannelKey,
        cle.CampaignKey,
        'Letter Sent' AS EngagementType,
        dc.EngagementStage,
        dl.LetterId AS InteractionId,
        CASE
            WHEN dcc.FirstDonationDateKey IS NOT NULL
             AND dcc.FirstDonationDateKey >= cle.DateSent THEN TRUE
            ELSE FALSE
        END AS WasConverted
    FROM {gold_lakehouse_name}.FactConstituentLetterEngagement cle
    JOIN {gold_lakehouse_name}.DimChannel ch ON ch.ChannelName = 'Direct Mail'
    JOIN {gold_lakehouse_name}.DimConstituent dc ON cle.ConstituentKey = dc.ConstituentKey
    JOIN {gold_lakehouse_name}.DimLetter dl ON cle.LetterKey = dl.LetterKey
    JOIN {gold_lakehouse_name}.dm_Constituent dcc ON cle.ConstituentKey = dcc.ConstituentKey

    UNION ALL

    SELECT
        pc.ConstituentKey,
        pc.PhonecallDate AS EngagementDate,
        ch.ChannelKey,
        pc.CampaignKey,
        'Phone Call' AS EngagementType,
        dc.EngagementStage,
        dp.PhonecallId AS InteractionId,
        CASE
            WHEN dcc.FirstDonationDateKey IS NOT NULL
             AND dcc.FirstDonationDateKey >= pc.PhonecallDate THEN TRUE
            ELSE FALSE
        END AS WasConverted
    FROM {gold_lakehouse_name}.FactConstituentPhonecallEngagement pc
    JOIN {gold_lakehouse_name}.DimPhonecall dp ON pc.PhonecallKey = dp.PhonecallKey
    JOIN {gold_lakehouse_name}.DimChannel ch ON ch.ChannelName = 'Phone Call'
    JOIN {gold_lakehouse_name}.DimConstituent dc ON pc.ConstituentKey = dc.ConstituentKey
    JOIN {gold_lakehouse_name}.dm_Constituent dcc ON pc.ConstituentKey = dcc.ConstituentKey
),
counts AS (
    SELECT ConstituentKey, COUNT(*) AS EngagementsBeforeDonation
    FROM engagement_union
    WHERE WasConverted = TRUE
    GROUP BY ConstituentKey
)
SELECT e.*, c.EngagementsBeforeDonation
FROM engagement_union e
LEFT JOIN counts c
    ON e.ConstituentKey = c.ConstituentKey
""")

df_engagementtimeline.write.mode("overwrite").format("delta").saveAsTable(f"{gold_lakehouse_name}.dm_EngagementTimeline")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build: dm_CampaignAttribution

# CELL ********************

df_campaignattribution = spark.sql(f"""
    -- EMAIL ENGAGEMENTS
    SELECT /*+ MERGE(d, e, dd_d, dd_e, ch) */
        d.DonationKey,
        e.ConstituentKey,
        d.DonationDateKey,
        e.SentDateKey AS EngagementDateKey,
        CASE 
            WHEN e.WasOpened = 1 THEN 'Email Open'
            WHEN e.ClickThroughDateKey IS NOT NULL THEN 'Email Click'
            ELSE 'Email Sent'
        END AS EngagementType,
        ch.ChannelKey,
        e.CampaignKey
    FROM {gold_lakehouse_name}.FactDonation d
    JOIN {gold_lakehouse_name}.FactConstituentEmailEngagement e ON d.ConstituentKey = e.ConstituentKey
    JOIN {gold_lakehouse_name}.DimDate dd_d ON dd_d.DateKey = d.DonationDateKey
    JOIN {gold_lakehouse_name}.DimDate dd_e ON dd_e.DateKey = e.SentDateKey
    JOIN {gold_lakehouse_name}.DimChannel ch ON ch.ChannelName = 'Email'
    WHERE dd_e.Date <= dd_d.Date

    UNION ALL

    -- SOCIAL ENGAGEMENTS
    SELECT /*+ MERGE(d, s, dd_d, dd_s, ch) */
        d.DonationKey,
        s.ConstituentKey,
        d.DonationDateKey,
        s.DateKey AS EngagementDateKey,
        CASE 
            WHEN s.`Like` THEN 'Social Like'
            WHEN s.Share THEN 'Social Share'
            WHEN s.`Comment` THEN 'Social Comment'
            WHEN s.Impression THEN 'Social Impression'
            ELSE 'Social View'
        END AS EngagementType,
        ch.ChannelKey,
        NULL AS CampaignKey
    FROM {gold_lakehouse_name}.FactDonation d
    JOIN {gold_lakehouse_name}.FactConstituentSocialEngagement s ON d.ConstituentKey = s.ConstituentKey
    JOIN {gold_lakehouse_name}.DimDate dd_d ON dd_d.DateKey = d.DonationDateKey
    JOIN {gold_lakehouse_name}.DimDate dd_s ON dd_s.DateKey = s.DateKey
    JOIN {gold_lakehouse_name}.DimChannel ch ON ch.ChannelName = 'Social Media'
    WHERE dd_s.Date <= dd_d.Date

    UNION ALL

    -- LETTER ENGAGEMENTS
    SELECT /*+ MERGE(d, l, dd_d, dd_l, ch) */
        d.DonationKey,
        l.ConstituentKey,
        d.DonationDateKey,
        l.DateSent AS EngagementDateKey,
        'Letter Sent' AS EngagementType,
        ch.ChannelKey,
        l.CampaignKey
    FROM {gold_lakehouse_name}.FactDonation d
    JOIN {gold_lakehouse_name}.FactConstituentLetterEngagement l ON d.ConstituentKey = l.ConstituentKey
    JOIN {gold_lakehouse_name}.DimDate dd_d ON dd_d.DateKey = d.DonationDateKey
    JOIN {gold_lakehouse_name}.DimDate dd_l ON dd_l.DateKey = l.DateSent
    JOIN {gold_lakehouse_name}.DimChannel ch ON ch.ChannelName = 'Direct Mail'
    WHERE dd_l.Date <= dd_d.Date

    UNION ALL

    -- PHONE CALL ENGAGEMENTS
    SELECT /*+ MERGE(d, p, dd_d, dd_p, ch) */
        d.DonationKey,
        p.ConstituentKey,
        d.DonationDateKey,
        p.PhonecallDate AS EngagementDateKey,
        'Phone Call' AS EngagementType,
        ch.ChannelKey,
        p.CampaignKey
    FROM {gold_lakehouse_name}.FactDonation d
    JOIN {gold_lakehouse_name}.FactConstituentPhonecallEngagement p ON d.ConstituentKey = p.ConstituentKey
    JOIN {gold_lakehouse_name}.DimDate dd_d ON dd_d.DateKey = d.DonationDateKey
    JOIN {gold_lakehouse_name}.DimDate dd_p ON dd_p.DateKey = p.PhonecallDate
    JOIN {gold_lakehouse_name}.DimChannel ch ON ch.ChannelName = 'Phone Call'
    WHERE dd_p.Date <= dd_d.Date

    UNION ALL

    -- EVENT ATTENDANCE ENGAGEMENTS
    SELECT /*+ MERGE(d, ea, e, dd_d, dd_e, ch) */
        d.DonationKey,
        ea.ConstituentKey,
        d.DonationDateKey,
        e.EventDateKey AS EngagementDateKey,
        CASE 
            WHEN ea.AttendedEvent = 1 THEN 'Event Attended'
            WHEN ea.AcceptedDateKey IS NOT NULL THEN 'Event Accepted'
            WHEN ea.InvitationDateKey IS NOT NULL THEN 'Event Invited'
            ELSE 'Event Registered'
        END AS EngagementType,
        ch.ChannelKey,
        NULL AS CampaignKey
    FROM {gold_lakehouse_name}.FactDonation d
    JOIN {gold_lakehouse_name}.FactEventAttendance ea ON d.ConstituentKey = ea.ConstituentKey
    JOIN {gold_lakehouse_name}.DimEvent e ON ea.EventKey = e.EventKey
    JOIN {gold_lakehouse_name}.DimDate dd_d ON dd_d.DateKey = d.DonationDateKey
    JOIN {gold_lakehouse_name}.DimDate dd_e ON dd_e.DateKey = e.EventDateKey
    JOIN {gold_lakehouse_name}.DimChannel ch ON ch.ChannelName = 'Events'
    WHERE dd_e.Date <= dd_d.Date
""")

df_campaignattribution.write.mode("overwrite").format("delta").saveAsTable(f"{gold_lakehouse_name}.dm_CampaignAttribution")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validation

# CELL ********************

tables = [
    "Configuration",
    "DimAddress",
    "DimCampaign",
    "DimCampaignChannelBridge",
    "DimCampaignType",
    "DimChannel",
    "DimConstituent",
    "DimConstituentProgramBridge",
    "DimConstituentSegment",
    "DimConstituentSegmentBridge",
    "DimConstituentSegmentType",
    "DimDate",
    "DimDonationSource",
    "DimEmail",
    "DimEngagementPlatform",
    "DimEvent",
    "DimLetter",
    "DimOpportunityStage",
    "DimOpportunityType",
    "DimPhonecall",
    "DimProgram",
    "DimSource",
    "DimVolunteeringType",
    "FactConstituentEmailEngagement",
    "FactConstituentLetterEngagement",
    "FactConstituentPhonecallEngagement",
    "FactConstituentSocialEngagement",
    "FactDonation",
    "FactEventAttendance",
    "FactOpportunity",
    "FactSocialEngagement",
    "FactSoftCredit",
    "FactVolunteerHours",
    "FactWealthScreening",
    "dm_CampaignAttribution",
    "dm_Constituent",
    "dm_EngagementTimeline",
]

df = get_lakehouse_table_counts(gold_lakehouse_name, tables)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
