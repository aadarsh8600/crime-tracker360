from pyspark.sql.functions import lit
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()


lapd_df1 = spark.read.option("header", "true").csv("s3://final-project-bucket-group-5/raw-data-ny-la-cpd/Crime_Data_from_2010_to_2019.csv").withColumn("source", lit("lapd"))
lapd_df2 = spark.read.option("header", "true").csv("s3://final-project-bucket-group-5/raw-data-ny-la-cpd/Crime_Data_from_2020_to_Present_20250730.csv").withColumn("source", lit("lapd"))

# One NYPD file
nypd_df = spark.read.option("header", "true").csv("s3://final-project-bucket-group-5/raw-data-ny-la-cpd/NYPD_Complaint_Data_Historic.csv").withColumn("source", lit("nypd"))

# One CPD file
cpd_df = spark.read.option("header", "true").csv("s3://final-project-bucket-group-5/raw-data-ny-la-cpd/Crimes_-_2001_to_Present.csv").withColumn("source", lit("cpd"))
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

def map_cpd(df):
    return df.selectExpr(
        "`Case Number` as case_id",
        "`Updated On` as report_date",
        "Date as occurrence_start_date",
        "null as occurrence_start_time",
        "null as occurrence_end_date",
        "null as occurrence_end_time",
        "IUCR as crime_code",
        "`Primary Type` as crime_description",
        "case when Arrest = 'true' then 'true' else 'false' end as crime_completed",
        "null as offense_severity",
        "null as weapon_description",
        "null as mocodes",
        "`Location Description` as location_description",
        "Block as address_block",
        "Location as location_name",
        "cast(Latitude as double) as latitude",
        "cast(Longitude as double) as longitude",
        "cast(`X Coordinate` as double) as x_coord",
        "cast(`Y Coordinate` as double) as y_coord",
        "District as reporting_district",
        "`Community Area` as area_name",
        "null as jurisdiction",
        "null as station_name",
        "`FBI Code` as fbi_code",
        "Arrest as arrest_made",
        "Domestic as domestic_incident",
        "null as victim_age",
        "null as victim_sex",
        "null as victim_race",
        "null as suspect_age",
        "null as suspect_sex",
        "null as suspect_race",
        "null as housing_project_name",
        "null as park_name",
        "null as transit_district",
        "source"
    )


# Apply mappings
lapd_df1_mapped = map_lapd(lapd_df1)
lapd_df2_mapped = map_lapd(lapd_df2)
nypd_df_mapped  = map_nypd(nypd_df)
cpd_df_mapped   = map_cpd(cpd_df)
from functools import reduce

# All should have the same column order now
all_dfs = [lapd_df1_mapped, lapd_df2_mapped, nypd_df_mapped, cpd_df_mapped]
master_df = reduce(lambda df1, df2: df1.unionByName(df2), all_dfs)
from pyspark.sql.functions import col, when, lpad, split, lit, concat_ws


transform_df = master_df.withColumn(
    "occurrence_start_time",
    when(col("source") == "lapd", 
         concat_ws(":",
            lpad(col("occurrence_start_time"), 4, "0").substr(1, 2),  
            lpad(col("occurrence_start_time"), 4, "0").substr(3, 2),  
            lit("00")                                                 
         )
    )
    .when(col("source") == "cpd", 
          
          split(col("occurrence_start_date"), " ").getItem(1)  
    )
    .otherwise(col("occurrence_start_time"))  
)
from pyspark.sql.functions import split, col

# Remove time part (anything after space) from report_date
transform_df = transform_df.withColumn("report_date", split(col("report_date"), " ").getItem(0))

# Remove time part from occurrence_start_date
transform_df = transform_df.withColumn("occurrence_start_date", split(col("occurrence_start_date"), " ").getItem(0))
from pyspark.sql.functions import col, when

transform_df = transform_df.withColumn(
    "crime_category",
    when(col("crime_description").contains("ASSAULT"), "Assault & Battery")
    .when(col("crime_description").contains("BATTERY"), "Assault & Battery")
    .when(col("crime_description").contains("HOMICIDE"), "Homicide & Attempted Murder")
    .when(col("crime_description").contains("MURDER"), "Homicide & Attempted Murder")
    .when(col("crime_description").contains("THEFT"), "Theft & Burglary")
    .when(col("crime_description").contains("BURGLARY"), "Theft & Burglary")
    .when(col("crime_description").contains("LARCENY"), "Theft & Burglary")
    .when(col("crime_description").contains("ROBBERY"), "Robbery")
    .when(col("crime_description").contains("RAPE"), "Sex Crimes")
    .when(col("crime_description").contains("SEX"), "Sex Crimes")
    .when(col("crime_description").contains("DRUG"), "Drug-Related Offenses")
    .when(col("crime_description").contains("WEAPON"), "Weapons Offenses")
    .when(col("crime_description").contains("FIREARM"), "Weapons Offenses")
    .when(col("crime_description").contains("CHILD"), "Child-Related Crimes")
    .when(col("crime_description").contains("FRAUD"), "Fraud & Forgery")
    .when(col("crime_description").contains("FORGERY"), "Fraud & Forgery")
    .when(col("crime_description").contains("CREDIT"), "Fraud & Forgery")
    .when(col("crime_description").contains("VEHICLE"), "Traffic & Vehicle Offenses")
    .when(col("crime_description").contains("DRIVING"), "Traffic & Vehicle Offenses")
    .when(col("crime_description").contains("LOITERING"), "Public Disturbance & Nuisance")
    .when(col("crime_description").contains("DISORDERLY"), "Public Disturbance & Nuisance")
    .when(col("crime_description").contains("DOMESTIC"), "Domestic & Intimate Partner Violence")
    .when(col("crime_description").contains("PARTNER"), "Domestic & Intimate Partner Violence")
    .when(col("crime_description").contains("PROSTITUTION"), "Sex Work / Human Trafficking")
    .when(col("crime_description").contains("PIMPING"), "Sex Work / Human Trafficking")
    .otherwise("Other / Miscellaneous")
)
from pyspark.sql.functions import col, when

transform_df = transform_df.withColumn(
    "weapon_category",
    when(col("weapon_description").contains("GUN"), "Firearm")
    .when(col("weapon_description").contains("RIFLE"), "Firearm")
    .when(col("weapon_description").contains("REVOLVER"), "Firearm")
    .when(col("weapon_description").contains("PISTOL"), "Firearm")
    .when(col("weapon_description").contains("FIREARM"), "Firearm")
    .when(col("weapon_description").contains("KNIFE"), "Knife/Sharp Object")
    .when(col("weapon_description").contains("BLADE"), "Knife/Sharp Object")
    .when(col("weapon_description").contains("RAZOR"), "Knife/Sharp Object")
    .when(col("weapon_description").contains("SWORD"), "Knife/Sharp Object")
    .when(col("weapon_description").contains("ICE PICK"), "Knife/Sharp Object")
    .when(col("weapon_description").contains("CLEAVER"), "Knife/Sharp Object")
    .when(col("weapon_description").contains("SCREWDRIVER"), "Knife/Sharp Object")
    .when(col("weapon_description").contains("PIPE"), "Blunt Object")
    .when(col("weapon_description").contains("BAT"), "Blunt Object")
    .when(col("weapon_description").contains("HAMMER"), "Blunt Object")
    .when(col("weapon_description").contains("BOARD"), "Blunt Object")
    .when(col("weapon_description").contains("IRON"), "Blunt Object")
    .when(col("weapon_description").contains("BRICK"), "Blunt Object")
    .when(col("weapon_description").contains("CHAIN"), "Blunt Object")
    .when(col("weapon_description").contains("BLUNT INSTRUMENT"), "Blunt Object")
    .when(col("weapon_description").contains("MACE"), "Chemical/Explosive")
    .when(col("weapon_description").contains("PEPPER SPRAY"), "Chemical/Explosive")
    .when(col("weapon_description").contains("CHEMICAL"), "Chemical/Explosive")
    .when(col("weapon_description").contains("EXPLOSIVE"), "Chemical/Explosive")
    .when(col("weapon_description").contains("SCALDING LIQUID"), "Chemical/Explosive")
    .when(col("weapon_description").contains("BOMB"), "Chemical/Explosive")
    .when(col("weapon_description").contains("SIMULATED"), "Simulated Weapon")
    .when(col("weapon_description").contains("TOY"), "Simulated Weapon")
    .when(col("weapon_description").contains("STARTER PISTOL"), "Simulated Weapon")
    .when(col("weapon_description").contains("DEMAND NOTE"), "Verbal/Threat")
    .when(col("weapon_description").contains("VERBAL THREAT"), "Verbal/Threat")
    .when(col("weapon_description").contains("DOG"), "Biological/Animal/Body Force")
    .when(col("weapon_description").contains("PHYSICAL"), "Biological/Animal/Body Force")
    .when(col("weapon_description").contains("STRONG-ARM"), "Biological/Animal/Body Force")
    .when(col("weapon_description").contains("LIQUOR"), "Biological/Animal/Body Force")
    .otherwise("Other/Unknown")
)
from pyspark.sql.functions import when, col

transform_df = transform_df.withColumn(
    "city",
    when(col("source") == "nypd", "New York") \
    .when(col("source") == "lapd", "Los Angeles") \
    .when(col("source") == "cpd", "Chicago") \
    .otherwise("Unknown")
)
transform_df.createOrReplaceTempView("tdf")
transform_df = spark.sql("""
  SELECT *,
    CASE
      WHEN location_description IN (
        'SINGLE FAMILY DWELLING', 'MULTI-UNIT DWELLING', 'APARTMENT',
        'CONDOMINIUM/TOWNHOUSE', 'HOUSE', 'MOBILE HOME/TRAILERS'
      ) THEN 'Residential'

      WHEN location_description IN (
        'GAS STATION', 'JEWELRY STORE', 'GROCERY STORE', 'LIQUOR STORE',
        'CLOTHING STORE', 'SUPERMARKET', 'BAR/COCKTAIL/NIGHTCLUB',
        'NAIL SALON', 'PHOTO/COPY', 'RESTAURANT/FAST FOOD'
      ) THEN 'Commercial'

      WHEN location_description LIKE 'MTA%' OR location_description IN (
        'BUS STOP', 'SUBWAY PLATFORM', 'TRAIN TRACKS', 'PARKING LOT',
        'TAXI', 'AIRPORT TERMINAL'
      ) THEN 'Transit'

      WHEN location_description IN (
        'HOSPITAL', 'DOCTOR/DENTIST OFFICE', 'NURSING HOME',
        'CLINIC', 'VETERINARIAN', 'HOSPICE', 'MEDICAL MARIJUANA'
      ) THEN 'Medical'

      WHEN location_description IN (
        'ELEMENTARY SCHOOL', 'HIGH SCHOOL', 'COLLEGE',
        'UNIVERSITY', 'PRIVATE SCHOOL', 'TRADE SCHOOL'
      ) THEN 'Educational'

      WHEN location_description IN (
        'GOVERNMENT FACILITY', 'FIRE STATION', 'POLICE FACILITY',
        'POST OFFICE', 'COURTHOUSE', 'JAIL/DETENTION CENTER'
      ) THEN 'Government'

      WHEN location_description IN (
        'CHURCH', 'MOSQUE', 'SYNAGOGUE', 'TEMPLE',
        'PLACE OF WORSHIP'
      ) THEN 'Religious'

      WHEN location_description IN (
        'PARK/PLAYGROUND', 'STREET', 'ALLEY', 'BEACH', 'LAKE',
        'RIVER', 'YARD', 'OPEN LOT'
      ) THEN 'Outdoor'

      ELSE 'Other'
    END AS location_category
  FROM tdf
""")
transform_df.createOrReplaceTempView("tdf1")
transform_df = spark.sql("""
SELECT *,
    CASE
        WHEN victim_race IN ('WHITE') THEN 'White'
        WHEN victim_race IN ('BLACK') THEN 'Black or African American'
        WHEN victim_race IN ('WHITE HISPANIC') THEN 'Hispanic'
        WHEN victim_race IN ('BLACK HISPANIC') THEN 'Hispanic'
        WHEN victim_race IN ('ASIAN / PACIFIC ISLANDER') THEN 'Asian / Pacific Islander'
        WHEN victim_race IN ('AMERICAN INDIAN/ALASKAN NATIVE') THEN 'American Indian / Alaska Native'
        WHEN victim_race IN ('OTHER') THEN 'Other'
        WHEN victim_race IN ('UNKNOWN') THEN 'Unknown'

        WHEN victim_race IN ('W') THEN 'White'
        WHEN victim_race IN ('B') THEN 'Black or African American'
        WHEN victim_race IN ('A') THEN 'Asian'
        WHEN victim_race IN ('I') THEN 'American Indian / Alaska Native'
        WHEN victim_race IN ('H') THEN 'Hispanic'
        WHEN victim_race IN ('X') THEN 'Unknown'

        WHEN victim_race IN ('P', 'F', 'L', 'U', 'V', 'O', 'D', 'K', 'Z', 'C', 'S', 'J', 'G') THEN 'Other'
        WHEN victim_race IS NULL OR victim_race = '-' OR victim_race = '(null)' THEN 'Unknown'
        ELSE 'Other'
    END AS race_group
FROM tdf1
""")
from pyspark.sql.functions import to_date, row_number
transform_df1 = transform_df.withColumn("report_date", to_date("report_date", "MM/dd/yyyy")) \
       .withColumn("occurrence_start_date", to_date("occurrence_start_date", "MM/dd/yyyy")) \
       .withColumn("occurrence_end_date", to_date("occurrence_end_date", "MM/dd/yyyy"))
from pyspark.sql.window import Window

windowSpec = Window.orderBy("report_date")  
transform_df1 = transform_df1.withColumn("case_num", row_number().over(windowSpec))
transform_df2 = transform_df1.withColumnRenamed("occurrence_start_date", "occurred_date") \
       .withColumnRenamed("occurrence_start_time", "occurred_time")
transform_df2.createOrReplaceTempView("tdf2")
transform_df2 = transform_df2.withColumn("suspect_race_group", 
                   when(col("suspect_race").isin("WHITE HISPANIC", "BLACK HISPANIC", "HISPANIC"), "HISPANIC")
                   .otherwise(col("suspect_race")))
from pyspark.sql.functions import when, col

transform_df2 = transform_df2.withColumn(
    "suspect_race_group",
    when(col("suspect_race_group").isNull(), "UNKNOWN")
    .when(col("suspect_race_group") == "(null)", "UNKNOWN")
    .otherwise(col("suspect_race_group"))
)

transform_df2 = transform_df2.withColumnRenamed("race_group", "victim_race_group")
selected_columns = [
    'report_date', 'occurred_date', 'occurred_time', 'crime_code',
    'latitude', 'longitude', 'jurisdiction', 'arrest_made', 'domestic_incident',
    'victim_age', 'victim_sex', 'suspect_age', 'suspect_sex', 'source',
    'crime_category', 'weapon_category', 'city', 'location_category',
    'victim_race_group', 'case_num', 'suspect_race_group'
]

transform_df2 = transform_df2.select(*selected_columns)

transform_df2.createOrReplaceTempView("tdf2")
from pyspark.sql.functions import abs

transform_df2 = transform_df2.withColumn("victim_age", abs(transform_df2["victim_age"]))
from pyspark.sql.functions import when, col, trim

transform_df2 = transform_df2.withColumn(
    "victim_age_group",
    when(trim(col("victim_age")) == "", "")  # preserve blank values
    .when(col("victim_age").cast("int") <= 18, "0-18")
    .when(col("victim_age").cast("int") <= 30, "19-30")
    .when(col("victim_age").cast("int") <= 45, "31-45")
    .when(col("victim_age").cast("int") <= 60, "46-60")
    .when(col("victim_age").cast("int") > 60, "60+")
)
transform_df2.createOrReplaceTempView("tdf2")
transform_df2 = transform_df2.drop("suspect_age")
transform_df2 = transform_df2.withColumn("victim_Sex",
    when(col("victim_sex") == "F", "Female")
    .when(col("victim_sex") == "M", "Male")
    .when(col("victim_sex").isin("E", "D", "H", "L", "N", "U", "X", "-"), "Others")
    .when(col("victim_sex") == "(null)", "Unknown")
    .when((col("victim_sex").isNull()) | (col("victim_sex") == ""), "Unknown")
    .otherwise(col("victim_sex"))  # Keep original if it doesn't match any case
)
from pyspark.sql.functions import when, col, trim

transform_df2 = transform_df2.withColumn(
    "suspect_sex",
    when(trim(col("suspect_sex")) == "F", "Female")
    .when(trim(col("suspect_sex")) == "M", "Male")
    .when(trim(col("suspect_sex")) == "U", "Unknown")
    .when(trim(col("suspect_sex")) == "(null)", "Unknown")
    .when(col("suspect_sex").isNull(), "Unknown")
    .when(trim(col("suspect_sex")) == "", "Unknown")
    .otherwise("Unknown")
)
transform_df2.show()
from pyspark.sql.functions import monotonically_increasing_id

# Dimension: Crime
dim_crime = transform_df2.select("crime_code", "crime_category", "weapon_category", "source") \
               .dropDuplicates() \
               .withColumn("crime_id", monotonically_increasing_id())

# Dimension: Victim
dim_victim = transform_df2.select("victim_age", "Victim_Sex", "victim_race_group", "victim_age_group") \
                .dropDuplicates() \
                .withColumn("victim_id", monotonically_increasing_id())
# Dimension: Suspect
dim_suspect = transform_df2.select("Suspect_Sex", "suspect_race_group") \
                 .dropDuplicates() \
                 .withColumn("suspect_id", monotonically_increasing_id())
# Dimension: Location
dim_location = transform_df2.select("latitude", "longitude", "city", "location_category") \
                  .dropDuplicates() \
                  .withColumn("location_id", monotonically_increasing_id())
# Dimension: Jurisdiction
dim_jurisdiction = transform_df2.select("jurisdiction") \
                      .dropDuplicates() \
                      .withColumn("jurisdiction_id", monotonically_increasing_id())
# Join all dimensions to create fact table
df_fact = transform_df2 \
    .join(dim_crime, on=["crime_code", "crime_category", "weapon_category", "source"], how="left") \
    .join(dim_victim, on=["victim_age", "Victim_Sex", "victim_race_group", "victim_age_group"], how="left") \
    .join(dim_suspect, on=["Suspect_Sex", "suspect_race_group"], how="left") \
    .join(dim_location, on=["latitude", "longitude", "city", "location_category"], how="left") \
    .join(dim_jurisdiction, on=["jurisdiction"], how="left")
fact_crime_cases = df_fact.select(
    "case_num",
    "report_date",
    "occurred_date",
    "occurred_time",
    "arrest_made",
    "domestic_incident",
    "crime_id",
    "victim_id",
    "suspect_id",
    "location_id",
    "jurisdiction_id"
)
from pyspark.sql.functions import monotonically_increasing_id
fact_crime_cases = fact_crime_cases.withColumn("fact_id", monotonically_increasing_id())
fact_crime_cases.show()
dim_suspect.show()
# Common settings
write_mode = "overwrite"
file_format = "csv"

# Write LAPD Raw Data
lapd_df1.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/raw-data/lapd-raw-1/")
lapd_df2.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/raw-data/lapd-raw-2/")

# Write NYPD Raw Data
nypd_df.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/raw-data/nypd-raw/")

# Write CPD Raw Data
cpd_df.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/raw-data/cpd-raw/")

# Write Master Raw Data
master_df.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/master-data1/")

# Write Transformed Data
transform_df2.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/transformed-data1/")

# Write Fact Table
fact_crime_cases.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/facts-dims1/fact1/")

# Write Dimension Tables
dim_crime.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/facts-dims1/crime-dim1/")
dim_victim.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/facts-dims1/victim-dim1/")
dim_suspect.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/facts-dims1/suspect-dim1/")
dim_location.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/facts-dims1/location-dim1/")
dim_jurisdiction.coalesce(1).write.mode(write_mode).option("header", "true").csv("s3://raw-master-transformed-factdim-grp-5/facts-dims1/jurisdiction-dim1/")




