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
# META   "editable": true
# META }

# PARAMETERS CELL ********************

# Target lakehouse
target_lakehouse = f"{silver_lakehouse_name}"

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
        "Account",
        "Address",
        "Campaign",
        "CampaignChannel",
        "CampaignType",
        "Channel",
        "Configuration",
        "Constituent",
        "ConstituentEmailEngagement",
        "ConstituentOpportunityStage",
        "ConstituentProgram",
        "ConstituentSegment",
        "ConstituentSegmentMapping",
        "ConstituentSegmentType",
        "ConstituentSocialEngagement",
        "ConstituentType",
        "Contact",
        "Country",
        "EmailEngagement",
        "EngagementPlatform",
        "EngagementStage",
        "Event",
        "EventType",
        "Gender",
        "Letter",
        "Opportunity",
        "OpportunityStage",
        "OpportunityType",
        "Participation",
        "ParticipationType",
        "Phonecall",
        "Program",
        "SocialEngagement",
        "SoftCredit",
        "Source",
        "SourceSystemIdMapping",
        "Transaction",
        "TransactionSource",
        "WealthScreening",
        "WealthScreeningCapacityRange",
        "WealthScreeningConstituentRating",
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

# Keep information about Bronze -> Silver synchronization
EnsureChangeDataFeedStateTable(target_lakehouse)

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

class NonprofitSilverModel(BaseDataModel):
    def get_entities(self) -> list[tuple[StructType, str, bool]]:
        return [
            (NonprofitSilverModel.get_account_schema(), "Account", True),
            (NonprofitSilverModel.get_address_schema(), "Address", True),
            (NonprofitSilverModel.get_campaign_schema(), "Campaign", True),
            (NonprofitSilverModel.get_campaignchannel_schema(), "CampaignChannel", True),
            (NonprofitSilverModel.get_campaigntype_schema(), "CampaignType", True),
            (NonprofitSilverModel.get_channel_schema(), "Channel", True),
            (NonprofitSilverModel.get_configuration_schema(), "Configuration", True),
            (NonprofitSilverModel.get_constituent_schema(), "Constituent", True),
            (NonprofitSilverModel.get_constituentemailengagement_schema(), "ConstituentEmailEngagement", True),
            (NonprofitSilverModel.get_constituentopportunitystage_schema(), "ConstituentOpportunityStage", True),
            (NonprofitSilverModel.get_constituentprogram_schema(), "ConstituentProgram", True),
            (NonprofitSilverModel.get_constituentsegment_schema(), "ConstituentSegment", True),
            (NonprofitSilverModel.get_constituentsegmentmapping_schema(), "ConstituentSegmentMapping", True),
            (NonprofitSilverModel.get_constituentsegmenttype_schema(), "ConstituentSegmentType", True),
            (NonprofitSilverModel.get_constituentsocialengagement_schema(), "ConstituentSocialEngagement", True),
            (NonprofitSilverModel.get_constituenttype_schema(), "ConstituentType", True),
            (NonprofitSilverModel.get_contact_schema(), "Contact", True),
            (NonprofitSilverModel.get_country_schema(), "Country", True),
            (NonprofitSilverModel.get_emailengagement_schema(), "EmailEngagement", True),
            (NonprofitSilverModel.get_engagementplatform_schema(), "EngagementPlatform", True),
            (NonprofitSilverModel.get_engagementstage_schema(), "EngagementStage", True),
            (NonprofitSilverModel.get_event_schema(), "Event", True),
            (NonprofitSilverModel.get_eventtype_schema(), "EventType", True),
            (NonprofitSilverModel.get_gender_schema(), "Gender", True),
            (NonprofitSilverModel.get_letter_schema(), "Letter", True),
            (NonprofitSilverModel.get_opportunity_schema(), "Opportunity", True),
            (NonprofitSilverModel.get_opportunitystage_schema(), "OpportunityStage", True),
            (NonprofitSilverModel.get_opportunitytype_schema(), "OpportunityType", True),
            (NonprofitSilverModel.get_participation_schema(), "Participation", True),
            (NonprofitSilverModel.get_participationtype_schema(), "ParticipationType", True),
            (NonprofitSilverModel.get_phonecall_schema(), "Phonecall", True),
            (NonprofitSilverModel.get_program_schema(), "Program", True),
            (NonprofitSilverModel.get_socialengagement_schema(), "SocialEngagement", True),
            (NonprofitSilverModel.get_softcredit_schema(), "SoftCredit", True),
            (NonprofitSilverModel.get_source_schema(), "Source", True),
            (NonprofitSilverModel.get_sourcesystemidmapping_schema(), "SourceSystemIdMapping", True),
            (NonprofitSilverModel.get_transaction_schema(), "Transaction", True),
            (NonprofitSilverModel.get_transactionsource_schema(), "TransactionSource", True),
            (NonprofitSilverModel.get_wealthscreening_schema(), "WealthScreening", True),
            (NonprofitSilverModel.get_wealthscreeningcapacityrange_schema(), "WealthScreeningCapacityRange", True),
            (NonprofitSilverModel.get_wealthscreeningconstituentrating_schema(), "WealthScreeningConstituentRating", True),
        ]

    @staticmethod
    def get_account_schema() -> StructType:
        return StructType([
            StructField("AccountId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("AddressId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("Email", StringType(), nullable=False),
            StructField("EngagementStageId", StringType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType(), nullable=False),
            StructField("RegistrationDate", TimestampType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Timezone", StringType())
        ])

    @staticmethod
    def get_address_schema() -> StructType:
        return StructType([
            StructField("AddressId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("City", StringType()),
            StructField("CountryId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("Latitude", DecimalType(16, 8)),
            StructField("Longitude", DecimalType(16, 8)),
            StructField("ModifiedDate", TimestampType()),
            StructField("Region", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("State", StringType()),
            StructField("StateCode", StringType()),
            StructField("ZipCode", StringType())
        ])

    @staticmethod
    def get_campaign_schema() -> StructType:
        return StructType([
            StructField("CampaignId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("Cost", DecimalType(16, 8)),
            StructField("CreatedDate", TimestampType()),
            StructField("EndDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("StartDate", TimestampType()),
            StructField("Timezone", StringType()),
            StructField("CampaignTypeId", StringType())
        ])

    @staticmethod
    def get_campaignchannel_schema() -> StructType:
        return StructType([
            StructField("CampaignChannelId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("CampaignId", StringType()),
            StructField("ChannelId", StringType())
        ])

    @staticmethod
    def get_campaigntype_schema() -> StructType:
        return StructType([
            StructField("CampaignTypeId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType(), nullable=False),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_channel_schema() -> StructType:
        return StructType([
            StructField("ChannelId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Weight", DecimalType())
        ])

    @staticmethod
    def get_configuration_schema() -> StructType:
        return StructType([
            StructField("ConfigurationId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Value", StringType())
        ])

    @staticmethod
    def get_constituent_schema() -> StructType:
        return StructType([
            StructField("AccountId", StringType()),
            StructField("ConstituentId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("ContactId", StringType(), nullable=False),
            StructField("ConstituentTypeId", StringType())
        ])

    @staticmethod
    def get_constituentemailengagement_schema() -> StructType:
        return StructType([
            StructField("ClickThrough", BooleanType()),
            StructField("ClickThroughDate", TimestampType()),
            StructField("ConstituentEmail", StringType()),
            StructField("ConstituentEmailEngagementId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("ConstituentId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("EmailEngagementId", StringType()),
            StructField("EmailId", StringType()),
            StructField("EmailSubject", StringType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("OpenedDate", TimestampType()),
            StructField("SendDate", TimestampType(), nullable=False),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Timezone", StringType()),
            StructField("WasOpened", BooleanType())
        ])

    @staticmethod
    def get_constituentopportunitystage_schema() -> StructType:
        return StructType([
            StructField("ConstituentId", StringType()),
            StructField("ConstituentOpportunityStageId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("OpportunityStageId", StringType())
        ])

    @staticmethod
    def get_constituentprogram_schema() -> StructType:
        return StructType([
            StructField("ConstituentId", StringType()),
            StructField("ConstituentProgramId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("ProgramId", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_constituentsegment_schema() -> StructType:
        return StructType([
            StructField("ConstituentSegmentId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("ConstituentSegmentTypeId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("Order", IntegerType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_constituentsegmentmapping_schema() -> StructType:
        return StructType([
            StructField("ConstituentId", StringType()),
            StructField("ConstituentSegmentId", StringType(), nullable=False),
            StructField("ConstituentSegmentMappingId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" })
        ])

    @staticmethod
    def get_constituentsegmenttype_schema() -> StructType:
        return StructType([
            StructField("ConstituentSegmentTypeId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_constituentsocialengagement_schema() -> StructType:
        return StructType([
            StructField("Comment", BooleanType()),
            StructField("ConstituentId", StringType(), nullable=False),
            StructField("ConstituentSocialEngagementId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("CreatedDate", TimestampType()),
            StructField("Date", TimestampType()),
            StructField("Impression", BooleanType()),
            StructField("Like", BooleanType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Share", BooleanType()),
            StructField("SocialEngagementId", StringType(), nullable=False),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Timezone", StringType())
        ])

    @staticmethod
    def get_constituenttype_schema() -> StructType:
        return StructType([
            StructField("ConstituentTypeId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_contact_schema() -> StructType:
        return StructType([
            StructField("AddressId", StringType()),
            StructField("BirthDate", DateType()),
            StructField("ContactId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("CreatedDate", TimestampType()),
            StructField("Email", StringType()),
            StructField("EngagementStageId", StringType()),
            StructField("FirstName", StringType(), nullable=False),
            StructField("GenderId", StringType()),
            StructField("LastName", StringType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("RegistrationDate", TimestampType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Timezone", StringType())
        ])

    @staticmethod
    def get_country_schema() -> StructType:
        return StructType([
            StructField("CountryCode", StringType(), nullable=False),
            StructField("CountryId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_emailengagement_schema() -> StructType:
        return StructType([
            StructField("CampaignId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("EmailEngagementId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("EmailId", StringType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("SendDate", TimestampType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Subject", StringType()),
            StructField("Timezone", StringType()),
            StructField("VariantType", StringType()),
            StructField("ChannelId", StringType())
        ])

    @staticmethod
    def get_engagementplatform_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("EngagementPlatformId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_engagementstage_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("EngagementStageId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("ModifiedDate", TimestampType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Name", StringType())
        ])

    @staticmethod
    def get_event_schema() -> StructType:
        return StructType([
            StructField("AddressId", StringType()),
            StructField("Cost", DecimalType(16, 8)),
            StructField("CreatedDate", TimestampType()),
            StructField("EventId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("EventTypeId", StringType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("StartDate", TimestampType()),
            StructField("ChannelId", StringType())
        ])

    @staticmethod
    def get_eventtype_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("EventTypeId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_gender_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("GenderId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_letter_schema() -> StructType:
        return StructType([
            StructField("CampaignId", StringType()),
            StructField("ConstituentId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("LetterId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("ModifiedDate", TimestampType()),
            StructField("SentDate", TimestampType()),
            StructField("ChannelId", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Subject", StringType()),
            StructField("Timezone", StringType())
        ])

    @staticmethod
    def get_opportunity_schema() -> StructType:
        return StructType([
            StructField("CampaignId", StringType()),
            StructField("ChannelId", StringType()),
            StructField("CloseDate", TimestampType()),
            StructField("ConstituentId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("ExpectedRevenue", DecimalType(16, 8)),
            StructField("ModifiedDate", TimestampType()),
            StructField("OpportunityId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("OpportunityStageId", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Timezone", StringType()),
            StructField("OpportunityTypeId", StringType()),
            StructField("OpportunityName", StringType())
        ])

    @staticmethod
    def get_opportunitystage_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("OpportunityStageId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("OpportunityTypeId", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_opportunitytype_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("OpportunityTypeId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Name", StringType())
        ])

    @staticmethod
    def get_participation_schema() -> StructType:
        return StructType([
            StructField("AcceptedDate", TimestampType()),
            StructField("AttendedEvent", BooleanType()),
            StructField("ConstituentId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("EndDate", TimestampType()),
            StructField("EventId", StringType()),
            StructField("Hours", DecimalType()),
            StructField("InvitationDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("ParticipationId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("ParticipationTypeId", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("StartDate", TimestampType()),
            StructField("Timezone", StringType())
        ])

    @staticmethod
    def get_participationtype_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("ParticipationTypeId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_phonecall_schema() -> StructType:
        return StructType([
            StructField("CallDate", TimestampType()),
            StructField("CampaignId", StringType()),
            StructField("ConstituentId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("Description", StringType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("PhonecallId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("ChannelId", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Timezone", StringType())
        ])

    @staticmethod
    def get_program_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("ProgramId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_socialengagement_schema() -> StructType:
        return StructType([
            StructField("CampaignId", StringType()),
            StructField("Clicks", LongType()),
            StructField("Comments", LongType()),
            StructField("CreatedDate", TimestampType()),
            StructField("Engagements", LongType()),
            StructField("Impressions", LongType()),
            StructField("InteractionDate", TimestampType()),
            StructField("Likes", LongType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Platform", StringType()),
            StructField("ChannelId", StringType()),
            StructField("PostId", StringType()),
            StructField("Reach", LongType()),
            StructField("ReportDate", TimestampType()),
            StructField("Shares", LongType()),
            StructField("SocialEngagementId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType())
        ])

    @staticmethod
    def get_softcredit_schema() -> StructType:
        return StructType([
            StructField("Amount", DecimalType(19, 9)),
            StructField("ConstituentId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("SoftCreditId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("TransactionId", StringType())
        ])

    @staticmethod
    def get_source_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" })
        ])

    @staticmethod
    def get_sourcesystemidmapping_schema() -> StructType:
        return StructType([
            StructField("SilverRecordId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("SourceId", StringType()),
            StructField("SourceSystemId", StringType()),
            StructField("SourceTable", StringType())
        ])

    @staticmethod
    def get_transaction_schema() -> StructType:
        return StructType([
            StructField("Amount", DecimalType(16, 8)),
            StructField("CampaignId", StringType(), nullable=False),
            StructField("ChannelId", StringType(), nullable=False),
            StructField("ConstituentId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("IsRecurring", BooleanType(), nullable=False),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("OpportunityId", StringType(), nullable=False),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("Timezone", StringType()),
            StructField("TransactionDate", TimestampType()),
            StructField("TransactionId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("TransactionSourceId", StringType(), nullable=False)
        ])

    @staticmethod
    def get_transactionsource_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("RecordId", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("TransactionSourceId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" }),
            StructField("TypeName", StringType())
        ])

    @staticmethod
    def get_wealthscreening_schema() -> StructType:
        return StructType([
            StructField("CapacityRangeId", StringType()),
            StructField("ConstituentId", StringType(), nullable=False),
            StructField("ConstituentRatingId", StringType()),
            StructField("CreatedDate", TimestampType()),
            StructField("LastScreeningDate", DateType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("WealthScreeningId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" })
        ])

    @staticmethod
    def get_wealthscreeningcapacityrange_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("WealthScreeningCapacityRangeId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" })
        ])

    @staticmethod
    def get_wealthscreeningconstituentrating_schema() -> StructType:
        return StructType([
            StructField("CreatedDate", TimestampType()),
            StructField("ModifiedDate", TimestampType()),
            StructField("Name", StringType()),
            StructField("SourceId", StringType(), nullable=False),
            StructField("SourceSystemId", StringType()),
            StructField("WealthScreeningConstituentRatingId", StringType(), nullable=False, metadata={ "primaryKey": True, "pkType": "guid" })
        ])

# Initialize the model and create the tables
if enable_create_tables:
    logging.info("Creating nonprofit silver data model entities.")

    model = NonprofitSilverModel(spark=spark, lakehouse=target_lakehouse)
    model.create_entities()

    logging.info("All tables created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
