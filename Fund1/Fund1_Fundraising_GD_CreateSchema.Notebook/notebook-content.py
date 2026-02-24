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
# META   "language_group": "synapse_pyspark",
# META   "editable": false
# META }

# PARAMETERS CELL ********************

# Target lakehouse
target_lakehouse = f"{gold_lakehouse_name}"
logging.info(f"Target lakehouse: {target_lakehouse}")

# Feature flags
enable_delete_tables = False
enable_create_tables = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "editable": true
# META }

# MARKDOWN ********************

# # Drop tables

# CELL ********************

if enable_delete_tables:
    tables_to_delete = [
        CDF_STATE_TABLE_NAME,
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
    for table_name in tables_to_delete:
        full_table_name = get_full_table_name(target_lakehouse, table_name)
        if spark.catalog.tableExists(full_table_name):
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            logging.info(f"Dropped table: {full_table_name}")
        else:
            logging.info(f"Table {full_table_name} does not exist.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create tables
# The provided code snippet defines a nonprofit_model class that creates tables in a Spark environment. Existing tables will be appended.

# CELL ********************

# Keep information about Silver -> Gold synchronization
EnsureChangeDataFeedStateTable(gold_lakehouse_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType, IntegerType, BooleanType, LongType
from pyspark.sql import SparkSession, DataFrame
import concurrent.futures
from datetime import datetime, timezone
import logging

spark = SparkSession.builder.getOrCreate()

class NonprofitGoldModel(BaseDataModel):
    def get_entities(self) -> list[tuple[StructType, str, bool]]:
        return [
            (NonprofitGoldModel.get_configuration_schema(), "Configuration", True),
            (NonprofitGoldModel.get_dimaddress_schema(), "DimAddress", True),
            (NonprofitGoldModel.get_dimcampaign_schema(), "DimCampaign", True),
            (NonprofitGoldModel.get_dimcampaignchannelbridge_schema(), "DimCampaignChannelBridge", True),
            (NonprofitGoldModel.get_dimcampaigntype_schema(), "DimCampaignType", True),
            (NonprofitGoldModel.get_dimchannel_schema(), "DimChannel", True),
            (NonprofitGoldModel.get_dimconstituent_schema(), "DimConstituent", True),
            (NonprofitGoldModel.get_dimconstituentprogrambridge_schema(), "DimConstituentProgramBridge", True),
            (NonprofitGoldModel.get_dimconstituentsegment_schema(), "DimConstituentSegment", True),
            (NonprofitGoldModel.get_dimconstituentsegmentbridge_schema(), "DimConstituentSegmentBridge", True),
            (NonprofitGoldModel.get_dimconstituentsegmenttype_schema(), "DimConstituentSegmentType", True),
            (NonprofitGoldModel.get_dimdate_schema(), "DimDate", True),
            (NonprofitGoldModel.get_dimdonationsource_schema(), "DimDonationSource", True),
            (NonprofitGoldModel.get_dimemail_schema(), "DimEmail", True),
            (NonprofitGoldModel.get_dimengagementplatform_schema(), "DimEngagementPlatform", True),
            (NonprofitGoldModel.get_dimevent_schema(), "DimEvent", True),
            (NonprofitGoldModel.get_dimletter_schema(), "DimLetter", True),
            (NonprofitGoldModel.get_dimopportunitystage_schema(), "DimOpportunityStage", True),
            (NonprofitGoldModel.get_dimopportunitytype_schema(), "DimOpportunityType", True),
            (NonprofitGoldModel.get_dimphonecall_schema(), "DimPhonecall", True),
            (NonprofitGoldModel.get_dimprogram_schema(), "DimProgram", True),
            (NonprofitGoldModel.get_dimsource_schema(), "DimSource", True),
            (NonprofitGoldModel.get_dimvolunteeringtype_schema(), "DimVolunteeringType", True),
            (NonprofitGoldModel.get_factconstituentemailengagement_schema(), "FactConstituentEmailEngagement", True),
            (NonprofitGoldModel.get_factconstituentletterengagement_schema(), "FactConstituentLetterEngagement", True),
            (NonprofitGoldModel.get_factconstituentphonecallengagement_schema(), "FactConstituentPhonecallEngagement", True),
            (NonprofitGoldModel.get_factconstituentsocialengagement_schema(), "FactConstituentSocialEngagement", True),
            (NonprofitGoldModel.get_factdonation_schema(), "FactDonation", True),
            (NonprofitGoldModel.get_facteventattendance_schema(), "FactEventAttendance", True),
            (NonprofitGoldModel.get_factopportunity_schema(), "FactOpportunity", True),
            (NonprofitGoldModel.get_factsocialengagement_schema(), "FactSocialEngagement", True),
            (NonprofitGoldModel.get_factsoftcredit_schema(), "FactSoftCredit", True),
            (NonprofitGoldModel.get_factvolunteerhours_schema(), "FactVolunteerHours", True),
            (NonprofitGoldModel.get_factwealthscreening_schema(), "FactWealthScreening", True),
            (NonprofitGoldModel.get_dm_campaignattribution_schema(), "dm_CampaignAttribution", True),
            (NonprofitGoldModel.get_dm_constituent_schema(), "dm_Constituent", True),
            (NonprofitGoldModel.get_dm_engagementtimeline_schema(), "dm_EngagementTimeline", True),
        ]

    @staticmethod
    def get_configuration_schema() -> StructType:
        return StructType([
            StructField("ConfigurationId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("Name", StringType()),
            StructField("Value", StringType())
        ])

    @staticmethod
    def get_dimaddress_schema() -> StructType:
        return StructType([
            StructField("AddressId", StringType()),
            StructField("AddressKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("City", StringType()),
            StructField("CountryCode", StringType(), nullable=False),
            StructField("CountryId", StringType()),
            StructField("CountryName", StringType()),
            StructField("Latitude", DecimalType(18, 4)),
            StructField("Longitude", DecimalType(18, 4)),
            StructField("Region", StringType()),
            StructField("SourceKey", LongType()),
            StructField("SourceSysAddressId", StringType()),
            StructField("State", StringType()),
            StructField("StateCode", StringType()),
            StructField("ZipCode", StringType())
        ])

    @staticmethod
    def get_dimcampaign_schema() -> StructType:
        return StructType([
            StructField("CampaignId", StringType()),
            StructField("CampaignKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("CampaignName", StringType()),
            StructField("CampaignTypeKey", LongType()),
            StructField("Cost", DecimalType(18, 4)),
            StructField("CreatedDateKey", LongType()),
            StructField("EndDateKey", LongType(), nullable=False),
            StructField("ModifiedDateKey", LongType()),
            StructField("SourceSysCampaignId", StringType()),
            StructField("StartDateKey", LongType()),
            StructField("Timezone", StringType())
        ])

    @staticmethod
    def get_dimcampaignchannelbridge_schema() -> StructType:
        return StructType([
            StructField("CampaignChannelBridgeKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("CampaignChannelId", StringType()),
            StructField("CampaignKey", LongType(), nullable=False),
            StructField("ChannelKey", LongType())
        ])

    @staticmethod
    def get_dimcampaigntype_schema() -> StructType:
        return StructType([
            StructField("CampaignType", StringType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("CampaignTypeKey", LongType(), nullable=False),
            StructField("CampaignTypeId", StringType(), nullable=False)
        ])

    @staticmethod
    def get_dimchannel_schema() -> StructType:
        return StructType([
            StructField("ChannelId", StringType()),
            StructField("ChannelKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("ChannelName", StringType()),
            StructField("CreatedDateKey", LongType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("Weight", DecimalType(18, 4))
        ])

    @staticmethod
    def get_dimconstituent_schema() -> StructType:
        return StructType([
            StructField("AddressKey", LongType()),
            StructField("ConstituentId", StringType()),
            StructField("ConstituentKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("ConstituentName", StringType()),
            StructField("ConstituentType", StringType()),
            StructField("CreatedDateKey", LongType()),
            StructField("Email", StringType()),
            StructField("EngagementStage", StringType()),
            StructField("Gender", StringType(), nullable=False),
            StructField("ModifiedDateKey", LongType()),
            StructField("RegistrationDateKey", LongType()),
            StructField("SourceKey", LongType()),
            StructField("SourceSysConstituentId", StringType())
        ])

    @staticmethod
    def get_dimconstituentprogrambridge_schema() -> StructType:
        return StructType([
            StructField("ConstituentKey", LongType()),
            StructField("ConstituentProgramBridgeId", StringType()),
            StructField("ConstituentProgramBridgeKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("ProgramKey", LongType())
        ])

    @staticmethod
    def get_dimconstituentsegment_schema() -> StructType:
        return StructType([
            StructField("ConstituentSegmentId", StringType()),
            StructField("ConstituentSegmentKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("ConstituentSegmentName", StringType()),
            StructField("Order", IntegerType()),
            StructField("TypeKey", LongType())
        ])

    @staticmethod
    def get_dimconstituentsegmentbridge_schema() -> StructType:
        return StructType([
            StructField("ConstituentKey", LongType()),
            StructField("ConstituentSegmentKey", LongType()),
            StructField("ConstituentSegmentMappingId", StringType()),
            StructField("ConstituentSegmentBridgeKey", LongType(), nullable=False, metadata={ "primaryKey": True })
        ])

    @staticmethod
    def get_dimconstituentsegmenttype_schema() -> StructType:
        return StructType([
            StructField("ConstituentSegmentType", StringType(), nullable=False),
            StructField("ConstituentSegmentTypeId", StringType()),
            StructField("ConstituentSegmentTypeKey", LongType(), nullable=False, metadata={ "primaryKey": True })
        ])

    @staticmethod
    def get_dimdate_schema() -> StructType:
        return StructType([
            StructField("Date", DateType()),
            StructField("DateKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("Day", IntegerType()),
            StructField("DayName", StringType()),
            StructField("DayOfWeek", IntegerType()),
            StructField("FiscalQuarter", IntegerType()),
            StructField("FiscalYear", IntegerType()),
            StructField("IsHoliday", BooleanType()),
            StructField("IsWeekend", BooleanType()),
            StructField("Month", IntegerType()),
            StructField("MonthName", StringType()),
            StructField("MonthNameShort", StringType()),
            StructField("Quarter", IntegerType()),
            StructField("WeekOfYear", IntegerType()),
            StructField("Year", IntegerType())
        ])

    @staticmethod
    def get_dimdonationsource_schema() -> StructType:
        return StructType([
            StructField("CreatedDateKey", LongType()),
            StructField("DonationSourceId", StringType()),
            StructField("DonationSourceKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("DonationSourceRecordKey", LongType()),
            StructField("DonationSourceTypeName", StringType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("SourceKey", LongType())
        ])

    @staticmethod
    def get_dimemail_schema() -> StructType:
        return StructType([
            StructField("EmailId", StringType()),
            StructField("EmailKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("EmailSubject", StringType()),
            StructField("SourceSystemEmailId", StringType()),
            StructField("ChannelKey", LongType()),
            StructField("EmailEngagementId", StringType()),
            StructField("SourceKey", LongType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("CreatedDateKey", LongType()),
            StructField("VariantType", StringType())
        ])

    @staticmethod
    def get_dimengagementplatform_schema() -> StructType:
        return StructType([
            StructField("CreatedDateKey", LongType()),
            StructField("EngagementPlatformId", StringType()),
            StructField("EngagementPlatformKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("ModifiedDateKey", LongType()),
            StructField("Name", StringType()),
            StructField("SourceKey", LongType()),
            StructField("SourceSysEngagementPlatformId", StringType(), nullable=False)
        ])

    @staticmethod
    def get_dimevent_schema() -> StructType:
        return StructType([
            StructField("AddressKey", LongType()),
            StructField("Cost", DecimalType(18, 4)),
            StructField("CreatedDateKey", LongType()),
            StructField("EventDateKey", LongType()),
            StructField("EventId", StringType()),
            StructField("EventKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("EventName", StringType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("SourceKey", LongType()),
            StructField("SourceSysEventId", StringType(), nullable=False),
            StructField("ChannelKey", LongType())
        ])

    @staticmethod
    def get_dimletter_schema() -> StructType:
        return StructType([
            StructField("CreatedDateKey", LongType()),
            StructField("LetterId", StringType(), nullable=False),
            StructField("LetterKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("LetterSubject", StringType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("SentDateKey", LongType()),
            StructField("SourceKey", LongType()),
            StructField("SourceSysLetterId", StringType()),
            StructField("ChannelKey", LongType())
        ])

    @staticmethod
    def get_dimopportunitystage_schema() -> StructType:
        return StructType([
            StructField("OpportunityStage", StringType(), nullable=False),
            StructField("OpportunityStageId", StringType(), nullable=False),
            StructField("OpportunityStageKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("OpportunityTypeKey", LongType())
        ])

    @staticmethod
    def get_dimopportunitytype_schema() -> StructType:
        return StructType([
            StructField("OpportunityTypeName", StringType()),
            StructField("OpportunityTypeId", StringType()),
            StructField("OpportunityTypeKey", LongType(), nullable=False, metadata={ "primaryKey": True })
        ])

    @staticmethod
    def get_dimphonecall_schema() -> StructType:
        return StructType([
            StructField("CreatedDateKey", LongType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("PhonecallDescription", StringType()),
            StructField("PhonecallId", StringType(), nullable=False),
            StructField("PhonecallKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("SourceKey", LongType()),
            StructField("SourceSysPhonecallId", StringType()),
            StructField("ChannelKey", LongType())
        ])

    @staticmethod
    def get_dimprogram_schema() -> StructType:
        return StructType([
            StructField("CreatedDateKey", LongType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("ProgramId", StringType()),
            StructField("ProgramKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("ProgramName", StringType()),
            StructField("SourceKey", LongType()),
            StructField("SourceSysProgramId", StringType())
        ])

    @staticmethod
    def get_dimsource_schema() -> StructType:
        return StructType([
            StructField("SourceId", StringType()),
            StructField("SourceKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("SourceName", StringType())
        ])

    @staticmethod
    def get_dimvolunteeringtype_schema() -> StructType:
        return StructType([
            StructField("CreatedDateKey", LongType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("SourceKey", LongType(), nullable=False),
            StructField("VolunteeringTypeId", StringType()),
            StructField("VolunteeringTypeKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("VolunteeringTypeName", StringType())
        ])

    @staticmethod
    def get_factconstituentemailengagement_schema() -> StructType:
        return StructType([
            StructField("CampaignKey", LongType()),
            StructField("ClickThrough", BooleanType()),
            StructField("ClickThroughDateKey", LongType()),
            StructField("ConstituentEmail", StringType()),
            StructField("ConstituentEmailEngagementId", StringType()),
            StructField("ConstituentEmailEngagementKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("ConstituentKey", LongType()),
            StructField("CreatedDateKey", LongType()),
            StructField("EmailKey", LongType()),
            StructField("ModifiedDateKey", LongType(), nullable=False),
            StructField("OpenedDateKey", LongType()),
            StructField("SentDateKey", LongType()),
            StructField("SourceKey", LongType()),
            StructField("EmailEngagementId", StringType()),
            StructField("SourceSysConstituentEmailEngagementId", StringType()),
            StructField("Timezone", StringType()),
            StructField("WasOpened", BooleanType()),
            StructField("ChannelKey", LongType())
        ])

    @staticmethod
    def get_factconstituentletterengagement_schema() -> StructType:
        return StructType([
            StructField("CampaignKey", LongType()),
            StructField("ConstituentKey", LongType()),
            StructField("ConstituentLetterEngagementId", StringType()),
            StructField("ConstituentLetterEngagementKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("DateSentKey", LongType()),
            StructField("LetterKey", LongType()),
            StructField("SourceSysConstituentLetterEngagementId", StringType()),
            StructField("Timezone", StringType()),
            StructField("ChannelKey", LongType())
        ])

    @staticmethod
    def get_factconstituentphonecallengagement_schema() -> StructType:
        return StructType([
            StructField("CampaignKey", LongType()),
            StructField("ConstituentKey", LongType()),
            StructField("ConstituentPhonecallEngagementId", StringType()),
            StructField("ConstituentPhonecallEngagementKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("PhonecallDate", LongType()),
            StructField("PhonecallKey", LongType()),
            StructField("SourceSysConstituentPhonecallEngagementId", StringType()),
            StructField("Timezone", StringType()),
            StructField("ChannelKey", LongType())
        ])

    @staticmethod
    def get_factconstituentsocialengagement_schema() -> StructType:
        return StructType([
            StructField("Comment", BooleanType()),
            StructField("ConstituentKey", LongType()),
            StructField("ConstituentSocialEngagementId", StringType()),
            StructField("ConstituentSocialEngagementKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("SocialEngagementId", StringType()),
            StructField("CreatedDateKey", LongType()),
            StructField("Impression", BooleanType()),
            StructField("DateKey", LongType()),
            StructField("Like", BooleanType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("Share", BooleanType()),
            StructField("SourceKey", LongType(), nullable=False),
            StructField("SourceSysConstituentSocialEngagementId", StringType()),
            StructField("Timezone", StringType(), nullable=False),
            StructField("ChannelKey", LongType())
        ])

    @staticmethod
    def get_factdonation_schema() -> StructType:
        return StructType([
            StructField("Amount", DecimalType(18, 4)),
            StructField("CampaignKey", LongType()),
            StructField("ChannelKey", LongType()),
            StructField("ConstituentKey", LongType()),
            StructField("CreatedDateKey", LongType()),
            StructField("DonationDateKey", LongType()),
            StructField("DonationId", StringType()),
            StructField("DonationKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("DonationName", StringType()),
            StructField("DonationSourceKey", LongType()),
            StructField("IsNewConstituent", BooleanType()),
            StructField("IsReccuring", BooleanType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("OpportunityKey", LongType(), nullable=False),
            StructField("SourceKey", LongType()),
            StructField("SourceSysDonationId", StringType()),
            StructField("Timezone", StringType())
        ])

    @staticmethod
    def get_facteventattendance_schema() -> StructType:
        return StructType([
            StructField("AcceptedDateKey", LongType()),
            StructField("AttendedEvent", BooleanType()),
            StructField("ConstituentKey", LongType()),
            StructField("CreatedDateKey", LongType(), nullable=False),
            StructField("EventAttendanceId", StringType()),
            StructField("EventAttendanceKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("EventKey", LongType()),
            StructField("InvitationDateKey", LongType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("SourceKey", LongType()),
            StructField("SourceSysEventAttendanceId", StringType()),
            StructField("ChannelKey", LongType())
        ])

    @staticmethod
    def get_factopportunity_schema() -> StructType:
        return StructType([
            StructField("CampaignKey", LongType(), nullable=False),
            StructField("ChannelKey", LongType()),
            StructField("CloseDateKey", LongType()),
            StructField("ConstituentKey", LongType()),
            StructField("CreatedDateKey", LongType()),
            StructField("ExpectedRevenue", DecimalType(18, 4)),
            StructField("IsClosed", BooleanType()),
            StructField("ModifiedDateKey", LongType(), nullable=False),
            StructField("OpportunityId", StringType()),
            StructField("OpportunityKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("OpportunityName", StringType()),
            StructField("OpportunityStageKey", LongType()),
            StructField("OpportunityTypeKey", LongType()),
            StructField("SourceKey", LongType()),
            StructField("SourceSysOpportunityId", StringType()),
            StructField("Timezone", StringType())
        ])

    @staticmethod
    def get_factsocialengagement_schema() -> StructType:
        return StructType([
            StructField("FactSocialEngagementKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("CampaignKey", LongType()),
            StructField("Clicks", LongType()),
            StructField("Comments", LongType()),
            StructField("EngagementPlatformKey", LongType()),
            StructField("ChannelKey", LongType()),
            StructField("SocialEngagementId", StringType()),
            StructField("SourceSocialEngagementId", StringType()),
            StructField("Engagements", LongType()),
            StructField("Impressions", LongType()),
            StructField("InteractionDateKey", LongType(), nullable=False),
            StructField("Likes", LongType()),
            StructField("PostId", StringType()),
            StructField("Reach", LongType()),
            StructField("ReportDateKey", LongType()),
            StructField("Shares", LongType()),
            StructField("CreatedDateKey", LongType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("SourceKey", LongType(), nullable=False)
        ])

    @staticmethod
    def get_factsoftcredit_schema() -> StructType:
        return StructType([
            StructField("ConstituentKey", LongType()),
            StructField("CreatedDateKey", LongType()),
            StructField("DonationKey", LongType()),
            StructField("ModifiedDateKey", LongType()),
            StructField("SoftCreditAmount", DecimalType(18, 4)),
            StructField("SoftCreditId", StringType()),
            StructField("SoftCreditKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("SourceKey", LongType()),
            StructField("SourceSysSoftCreditId", StringType())
        ])

    @staticmethod
    def get_factvolunteerhours_schema() -> StructType:
        return StructType([
            StructField("CreatedDateKey", LongType()),
            StructField("EventKey", LongType()),
            StructField("HoursVolunteered", DecimalType(18, 4)),
            StructField("ModifiedDateKey", LongType()),
            StructField("SourceKey", LongType()),
            StructField("SourceSysVolunteerHoursId", StringType()),
            StructField("Timezone", StringType()),
            StructField("VolunteeredDateKey", LongType()),
            StructField("VolunteerHoursId", StringType()),
            StructField("VolunteerHoursKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("VolunteeringTypeKey", LongType()),
            StructField("VolunteerKey", LongType())
        ])

    @staticmethod
    def get_factwealthscreening_schema() -> StructType:
        return StructType([
            StructField("CapacityRange", StringType()),
            StructField("ConstituentKey", LongType()),
            StructField("ConstituentRating", StringType()),
            StructField("CreatedDateKey", LongType()),
            StructField("LastScreeningDateKey", LongType(), nullable=False),
            StructField("ModifiedDateKey", LongType()),
            StructField("SourceKey", LongType()),
            StructField("WealthScreeningKey", LongType(), nullable=False, metadata={ "primaryKey": True }),
            StructField("WealthScreeningId", StringType(), nullable=False)
        ])

    @staticmethod
    def get_dm_campaignattribution_schema() -> StructType:
        return StructType([
            StructField("CampaignKey", LongType(), nullable=False),
            StructField("ChannelKey", LongType()),
            StructField("ConstituentKey", LongType()),
            StructField("DonationDateKey", LongType()),
            StructField("DonationKey", LongType()),
            StructField("EngagementDateKey", LongType()),
            StructField("EngagementType", StringType())
        ])

    @staticmethod
    def get_dm_constituent_schema() -> StructType:
        return StructType([
            StructField("ConstituentKey", LongType()),
            StructField("ConstituentId", StringType()),
            StructField("ConstituentName", StringType()),
            StructField("Email", StringType()),
            StructField("Age", LongType()),
            StructField("EngagementStage", StringType()),
            StructField("AcquisitionChannel", StringType()),
            StructField("LifetimeDonationAmount", DecimalType(18, 4)),
            StructField("FirstDonationDateKey", LongType()),
            StructField("LastDonationDateKey", LongType()),
            StructField("AttendedEventsCount", LongType()),
            StructField("FirstEngagementDateKey", LongType()),
            StructField("IsNewDonor", BooleanType())
        ])

    @staticmethod
    def get_dm_engagementtimeline_schema() -> StructType:
        return StructType([
            StructField("CampaignKey", LongType()),
            StructField("ChannelKey", LongType()),
            StructField("ConstituentKey", LongType()),
            StructField("EngagementDate", LongType()),
            StructField("EngagementStage", StringType(), nullable=False),
            StructField("EngagementType", StringType()),
            StructField("InteractionId", StringType()),
            StructField("WasConverted", BooleanType())
        ])

# Initialize the model and create the tables
if enable_create_tables:
    logging.info("Creating NonprofitGoldModel entities...")

    model = NonprofitGoldModel(spark=spark, lakehouse=target_lakehouse)
    model.create_entities()

    logging.info("All tables created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
