df.createOrReplaceTempView("df")

spark.sql("""
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
  FROM df
""").show()


# Step 1: Register temp view
df.createOrReplaceTempView("victim_data")

# Step 2: Write Spark SQL query
result = spark.sql("""
SELECT *,
    CASE
        WHEN victim_race IN ('WHITE') THEN 'White'
        WHEN victim_race IN ('BLACK') THEN 'Black or African American'
        WHEN victim_race IN ('WHITE HISPANIC') THEN 'White Hispanic'
        WHEN victim_race IN ('BLACK HISPANIC') THEN 'Black Hispanic'
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
FROM victim_data
""")
