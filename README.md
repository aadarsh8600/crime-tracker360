
# Project Title


# 🚔 CrimeTracker 360

> **"Tagline-->Data-driven insights to make US cities safer."**  
CrimeTracker 360 analyzes crime incident data from **NYPD**, **LAPD** (2001–2025) to uncover patterns, hotspots, and weapon trends.  
The goal is to help **government** and **law enforcement** optimize patrol schedules, allocate resources efficiently, and enhance public safety.

# 📌Problem Statement


Despite the availability of extensive crime-related data, cities often struggle to identify high-risk areas, detect patterns in criminal behaviour, and address resource allocation needs effectively.

This project aims to analyze historical crime records using big data technologies to extract actionable insights that can help law enforcement agencies:

🗂 Understand crime distribution by type, time, and location

📈 Identify trends and anomalies across years and neighbourhoods

🔍 Improve prediction and prevention strategies

📊 Support data-driven policing through real-time dashboards

## 🎯Objectives
- 📍 Identify **most common crime types** across NYPD, LAPD datasets.  
- ⏰ Detect **peak crime hours** by day & time.  
- 🔫 Study **weapon usage trends** for different crimes.  
- 🚓 Recommend **patrol scheduling** during high-crime periods.  
- 📊 Provide **data-backed reports** for better policy-making.



## 📂 Data Sources
- 🗓 **Time Range:** 2001–2025  
- 🏙 **Cities Covered:**  
  - 🗽 New York (NYPD)  
  - 🌴 Los Angeles (LAPD)  
    
- 📄 Formats: CSV  
- 🌐 Source Website: data.gov
## Columns discription
| **Column Name**        | **Description** |
|------------------------|-----------------|
| `report_date`          | Date when the crime was officially reported to the police. |
| `occurred_date`        | Actual date when the crime took place. |
| `occurred_time`        | Time of day when the crime occurred (HH:MM format). |
| `crime_code`           | Official crime classification code assigned by the police department. |
| `latitude`             | Latitude coordinate of the crime location. |
| `longitude`            | Longitude coordinate of the crime location. |
| `jurisdiction`         | Police jurisdiction or precinct responsible for handling the case. |
| `arrest_made`          | Indicates whether an arrest was made (`Yes`/`No`). |
| `domestic_incident`    | Specifies if the crime was related to a domestic situation (`Yes`/`No`). |
| `victim_age`           | Age of the victim involved in the incident. |
| `victim_sex`           | Gender of the victim (`Male`/`Female`/`Other`/`Unknown`). |
| `suspect_age`          | Age of the suspect involved in the incident. |
| `suspect_sex`          | Gender of the suspect (`Male`/`Female`/`Other`/`Unknown`). |
| `source`               | Origin of the data (e.g., `NYPD`, `LAPD`, `CPD`). |
| `crime_category`       | General category/type of crime (e.g., `Assault`, `Theft`, `Robbery`). |
| `weapon_category`      | Type of weapon used in the crime (e.g., `Firearm`, `Knife`, `None`). |
| `city`                 | City where the crime occurred (`New York`, `Los Angeles`, `Chicago`). |
| `location_category`    | Type of location where the crime occurred (e.g., `Street`, `Residence`, `Commercial`). |
| `victim_race_group`    | Racial/ethnic group of the victim. |
| `case_num`             | Unique case number or complaint number assigned by police. |
| `suspect_race_group`   | Racial/ethnic group of the suspect. |

## 🛠Tech stack


- ☁ **Cloud & Storage:** AWS S3, AWS Glue, AWS Athena  
- 🔄 **Data Processing:** PySpark, Python (Pandas, NumPy)  
- 🗃 **Database:** AWS Glue Data Catalog 
- 📈 **Visualization:** Power BI 
- 🧾 **Version Control:** GitHub  
- ⚙ **Automation:** Github action and Terraform
## 🔄Workflow

1. 📥 **Data Collection** – Crime records from NYPD, LAPD.  
2. 🔍 **Exploratory Data Analysis (EDA)** – Understand data structure, detect patterns, spot anomalies, and get initial insights.
3. 🗂 **Master Data Creation** – Merge & join all two datasets (NYPD, LAPD) into a standardized unified dataset.
4. 🧹 **Data Cleaning** – Remove duplicates, handle missing data, standardize formats.  
5. 📊 **Data Analysis** – Trends, hotspots, and weapon distribution.  
6. 📉 **Dashboard Creation** – Interactive visuals with filters.  


