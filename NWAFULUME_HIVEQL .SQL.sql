-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS clinicaltrial_2021

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS clinicaltrial_2021 (
Id STRING,
Sponsor STRING,
Status STRING,
Start STRING,
Completion STRING,
Type STRING,
Submission STRING,
Conditions STRING,
Interventions STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "|"
LOCATION "/FileStore/tables/clinicaltrial_2020"


-- COMMAND ----------

--LOAD DATA INPATH "/FileStore/tables/clinicaltrial_2021.csv"
--OVERWRITE INTO TABLE clinicaltrial_2021;

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS mesh (
Term STRING,
Tree STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
LOCATION "/FileStore/tables/mesh"

-- COMMAND ----------

--LOAD DATA INPATH "/FileStore/tables/mesh.csv"
--OVERWRITE INTO TABLE mesh;

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS pharma (
Company STRING,
Parent_Company STRING,
Penalty_Amount STRING,
Subtraction_From_Penalty STRING,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING,
Penalty_Year STRING,
Penalty_Date STRING,
Offense_Group STRING,
Primary_Offense STRING,
Description STRING,
Level_of_Government STRING,
Action_Type STRING,
Agency STRING,
Prosecution_Agreement STRING,
Court STRING,
Case_ID STRING,
Private_Litigation_Case_Title STRING,
Lawsuit_Resolution STRING,
Facility_State STRING,
City STRING,
Address STRING,
Zip STRING,
NAICS STRING,
NAICS_Translation STRING,
HQ_Country_of_Parent STRING,
HQ_State_of_Parent STRING,
Ownership_Structure STRING,
Parent_Company_Stock_Ticker STRING,
Major_Industry_of_Parent STRING,
Specific_Industry_of_Parent STRING,
Info_Source STRING,
Notes STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
LOCATION "/FileStore/tables/pharma"

-- COMMAND ----------

--LOAD DATA INPATH "/FileStore/tables/pharma.csv"
--OVERWRITE INTO TABLE pharma;

-- COMMAND ----------

SELECT COUNT(*)
FROM clinicaltrial_2021
Where Id Like "N%";

-- COMMAND ----------

SELECT Type, count(*) As Total
FROM clinicaltrial_2021
GROUP BY Type
ORDER BY Total DESC
Limit 4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 3

-- COMMAND ----------

SELECT Most_Conditions, count(*) As Total
From clinicaltrial_2021
lateral view outer explode(split(Conditions, ",")) as Most_Conditions
Where Conditions like "%_%"
Group BY Most_Conditions
Order BY Total DESC
Limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 4

-- COMMAND ----------

create view if not exists Conditionsadded AS SELECT Most_Conditions
FROM clinicaltrial_2021
lateral view outer explode(split(Conditions, ",")) as Most_Conditions
Where Conditions like "%_%"

-- COMMAND ----------

Select left(mesh.tree,3) as Roots, count(*) as frequency
from mesh
join Conditionsadded
on Conditionsadded.Most_Conditions=mesh.term
Group by Roots
Order by Frequency desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 5

-- COMMAND ----------

Create view if not exists Pharmaadded as
select replace (ltrim(rtrim(replace(Parent_Company,'"',''))),'', '"')as Parent_company2
from pharma;

-- COMMAND ----------

SELECT clinicaltrial_2021.sponsor, count(*) as frequency
FROM clinicaltrial_2021
LEFT anti join Pharmaadded
ON clinicaltrial_2021.sponsor=pharmaadded.Parent_Company2
GROUP BY clinicaltrial_2021.sponsor
ORDER BY frequency DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 6

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS MONTH AS
SELECT unix_timestamp(left (Completion, 3), "MMM") AS MONTHS, count(*) as total
FROM clinicaltrial_2021
WHERE Status= "Completed" and Completion Like "%2021"
GROUP BY Completion
ORDER BY Months;

-- COMMAND ----------

SELECT from_unixtime(Months, "MMM") as Months,Total
FROM MONTH

-- COMMAND ----------

-- MAGIC %md
-- MAGIC OPTIONAL FURTHER ANALYSIS 1

-- COMMAND ----------

SELECT clinicaltrial_2021.sponsor, count(*) as frequency
FROM clinicaltrial_2021
RIGHT join Pharmaadded
ON clinicaltrial_2021.sponsor=pharmaadded.Parent_Company2
GROUP BY clinicaltrial_2021.sponsor
ORDER BY frequency DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC OPTIONAL QUESTION 2

-- COMMAND ----------

SELECT Explored_Interventions, count(*) As Total
From clinicaltrial_2021
lateral view outer explode(split(interventions, ",")) as Explored_Interventions
Where Interventions like "%_%"
Group BY Explored_Interventions
Order BY Total DESC
Limit 5

-- COMMAND ----------


