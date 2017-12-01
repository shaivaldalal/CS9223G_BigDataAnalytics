# module load java/1.8.0_72
# module load spark/2.2.0
# module load python/gnu/3.4.4
# export PYSPARK2_PYTHON=/share/apps/python/3.4.4/bin/python3
# export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python3
import time
from pyspark.sql.functions import *
#Reading the CSV file. We specify the custom date format by referrign to the data dictionary and specify the nullValue as Unspecified
data=spark.read.csv('Project/new_311.csv',header=True,nullValue='Unspecified',dateFormat='mm/dd/yy')


# data.select([count(when((col(columns)=="NA") | (col(columns)=="Unspecified") | (col(columns)=="N/A") | (col(columns)=="") | (col(columns).isNull()),columns))]).show()


# We create colData to store percentage of missing values in every column
colData=[]
length=data.count()
for columns in data.columns:
	colData.append(data.select([count(when((col(columns)=="NA") | (col(columns)=="Unspecified") | (col(columns)=="N/A") | (col(columns)=="") | (col(columns).isNull()) | (col(columns)=="0 Unspecified"),columns)).alias(columns)]).take(1)[0][0])

# We calculate percentages
for i in range(0,len(colData)):
	colData[i]=(colData[i]/length)*100

# We drop columns when the percent of missing value is greater than the threshold value
headers=data.columns
for i in range(0,len(colData)):
	if(colData[i]>60):
		data=data.drop(headers[i])

# We create a temp SQL table in Spark to allow for easier querying and data manipulation
data.createOrReplaceTempView("table")
# secondIteration=[]
# for columns in data.columns:
# 	secondIteration.append(data.select([count(when((col(columns)=="NA") | (col(columns)=="Unspecified") | (col(columns)=="N/A") | (col(columns)=="") | (col(columns).isNull()) | (col(columns)=="0 Unspecified"),columns)).alias(columns)]).take(1)[0][0])

# We remove entries where the closed date, resolution action action date are missing given that status is either closed or null
data=spark.sql("SELECT * FROM table where `Closed Date` is not null and `Resolution Action Updated Date` is not null and (status is not null or status!='Closed')")
# We fill the missing closed date values based on the resolution action update date
data=data.withColumn("Closed Date", when((col("Closed Date").isNull()) & (col("Status")=='Closed') &(col("Resolution Action Updated Date").isNotNull()),col("Resolution Action Updated Date")).otherwise(col("Closed Date")))
data.createOrReplaceTempView("table")

data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Street.*","Street Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Highway.*","Highway Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Noise.*","Noise Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Taxi.*","Taxi Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Ferry.*","Ferry Complaint"))

data.createOrReplaceTempView("table")

data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=10451) & (col("Incident Zip")<=10475),"BRONX").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=11201) & (col("Incident Zip")<=11239),"BROOKLYN").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=10001) & (col("Incident Zip")<=10280),"MANHATTAN").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=10301) & (col("Incident Zip")<=10314),"STATEN ISLAND").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=11354) & (col("Incident Zip")<=11697),"QUEENS").otherwise(col("Borough")))

data=spark.sql("SELECT * FROM TABLE WHERE BOROUGH IS NOT NULL")
data=data.drop('Location')
data=data.drop('Park Borough')
data.createOrReplaceTempView("table")

colData_new=[]
length_new=data.count()
for columns in data.columns:
	colData_new.append(data.select([count(when((col(columns)=="NA") | (col(columns)=="Unspecified") | (col(columns)=="N/A") | (col(columns)=="") | (col(columns).isNull()) | (col(columns)=="0 Unspecified"),columns)).alias(columns)]).take(1)[0][0])

# We calculate percentages
for i in range(0,len(colData_new)):
	colData_new[i]=(colData_new[i]/length)*100


# data=data.withColumn("Resolution Action Updated Date", when((col("Resolution Action Updated Date").isNull()) & (col("Closed Date").isNotNull()),col("Closed Date")).otherwise(col("Resolution Action Updated Date")))


# data=data.withColumn("Closed Date", when((col("Closed Date").isNull()) & (col("Resolution Action Updated Date").isNotNull()) & (col("Status")=='Closed'),col("Resolution Action Updated Date")))

# for i in range(0,len(data)):
	# if((data['Closed Date'].isNull()) & (data['Resolution Action Updated Date'].isNull() & ((data['Status']=="Closed") | data['Status'].isNull())))

# data.createOrReplaceTempView("table")
# spark.sql("SELECT count(`School Name`) from table WHERE `School Name`=='Unspecified'").show()
