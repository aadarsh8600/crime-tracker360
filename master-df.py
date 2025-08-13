from pyspark.sql.functions import lit

lapd_df1 = spark.read.option("header", "true").csv("s3://final-project-bucket-group-5/raw-data-ny-la-cpd/Crime_Data_from_2010_to_2019.csv").withColumn("source", lit("lapd"))
lapd_df2 = spark.read.option("header", "true").csv("s3://final-project-bucket-group-5/raw-data-ny-la-cpd/Crime_Data_from_2020_to_Present_20250730.csv").withColumn("source", lit("lapd"))

# One NYPD file
nypd_df = spark.read.option("header", "true").csv("s3://final-project-bucket-group-5/raw-data-ny-la-cpd/NYPD_Complaint_Data_Historic.csv").withColumn("source", lit("nypd"))
def map_lapd(df):
    return df.selectExpr(
        "DR_NO as case_id",
        "`Date Rptd` as report_date",
        "`DATE OCC` as occurrence_start_date",
        "`TIME OCC` as occurrence_start_time",
        "null as occurrence_end_date",
        "null as occurrence_end_time",
        "`Crm Cd` as crime_code",
        "`Crm Cd Desc` as crime_description",
        "`Status Desc` as crime_completed",
        "`Part 1-2` as offense_severity",
        "`Weapon Desc` as weapon_description",
        "Mocodes as mocodes",
        "`Premis Desc` as location_description",
        "`Cross Street` as address_block",
        "LOCATION as location_name",
        "cast(LAT as double) as latitude",
        "cast(LON as double) as longitude",
        "null as x_coord",
        "null as y_coord",
        "`Rpt Dist No` as reporting_district",
        "`AREA NAME` as area_name",
        "null as jurisdiction",
        "null as station_name",
        "null as fbi_code",
        "case when Status = 'AR' then 'true' else 'false' end as arrest_made",
        "null as domestic_incident",
        "cast(`Vict Age` as int) as victim_age",
        "`Vict Sex` as victim_sex",
        "`Vict Descent` as victim_race",
        "null as suspect_age",
        "null as suspect_sex",
        "null as suspect_race",
        "null as housing_project_name",
        "null as park_name",
        "null as transit_district",
        "source"
    )

def map_nypd(df):
    return df.selectExpr(
        "CMPLNT_NUM as case_id",
        "RPT_DT as report_date",
        "CMPLNT_FR_DT as occurrence_start_date",
        "CMPLNT_FR_TM as occurrence_start_time",
        "CMPLNT_TO_DT as occurrence_end_date",
        "CMPLNT_TO_TM as occurrence_end_time",
        "KY_CD as crime_code",
        "OFNS_DESC as crime_description",
        "CRM_ATPT_CPTD_CD as crime_completed",
        "LAW_CAT_CD as offense_severity",
        "null as weapon_description",
        "null as mocodes",
        "PREM_TYP_DESC as location_description",
        "null as address_block",
        "Lat_Lon as location_name",
        "cast(Latitude as double) as latitude",
        "cast(Longitude as double) as longitude",
        "cast(X_COORD_CD as double) as x_coord",
        "cast(Y_COORD_CD as double) as y_coord",
        "ADDR_PCT_CD as reporting_district",
        "BORO_NM as area_name",
        "JURIS_DESC as jurisdiction",
        "STATION_NAME as station_name",
        "null as fbi_code",
        "case when CRM_ATPT_CPTD_CD = 'COMPLETED' then 'true' else 'false' end as arrest_made",
        "null as domestic_incident",
        "null as victim_age",
        "VIC_SEX as victim_sex",
        "VIC_RACE as victim_race",
        "SUSP_AGE_GROUP as suspect_age",
        "SUSP_SEX as suspect_sex",
        "SUSP_RACE as suspect_race",
        "HADEVELOPT as housing_project_name",
        "PARKS_NM as park_name",
        "TRANSIT_DISTRICT as transit_district",
        "source"
    )

# Apply mappings
lapd_df1_mapped = map_lapd(lapd_df1)
lapd_df2_mapped = map_lapd(lapd_df2)
nypd_df_mapped  = map_nypd(nypd_df)
from functools import reduce

# All should have the same column order now
all_dfs = [lapd_df1_mapped, lapd_df2_mapped, nypd_df_mapped]
master_df = reduce(lambda df1, df2: df1.unionByName(df2), all_dfs)
