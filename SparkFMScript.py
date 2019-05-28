##
# Note: YT has ~3 million records without writers, I'm going to drop these records
#
##

import pyspark
sc = pyspark.SparkContext(appName="myAppName")

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

from pyspark.sql.session import SparkSession
spark = SparkSession(sc)

from pyspark.sql import functions as F
from pyspark.sql.functions import upper, col, regexp_extract, regexp_replace

#-----------------------------------
#DOWNTOWN
#-----------------------------------

dt = '/Users/valerieangulo/Downtown/fuzzymatching/songtrust_match.csv'
dtdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(dt)
mydt = dtdf
mydt = dtdf[['Custom ID','Title','COMPOSER']]
#mydt = mydt.withColumn('ratio', F.lit(0)) #dont need if we join DFs with levenshtein


sortdt = mydt
#trim whitespaces
sortdt = sortdt.withColumn('Title', F.trim(sortdt.Title))
sortdt = sortdt.withColumn('COMPOSER', F.trim(sortdt.COMPOSER))
#DT tabs
sortdt = sortdt.withColumn("Title", regexp_replace(col("Title"), '[\t]+', ''))
sortdt = sortdt.withColumn("COMPOSER", regexp_replace(col("COMPOSER"), '[\t]+', ''))
#DT new lines
sortdt = sortdt.withColumn("Title", regexp_replace(col("Title"), '[\n]+', ''))
sortdt = sortdt.withColumn("COMPOSER", regexp_replace(col("COMPOSER"), '[\n]+', ''))
#make caps
sortdt = sortdt.withColumn('Title', F.upper(col('Title'))) #test(F.upper(['Title', 'COMPOSER']))
sortdt = sortdt.withColumn('COMPOSER', F.upper(col('COMPOSER')))
#remove quotes, unknowns, |, -
sortdt = sortdt.withColumn("Title", regexp_replace(col("Title"), '"', ''))
sortdt = sortdt.withColumn("Title", regexp_replace(col("Title"), '\'', ''))
sortdt = sortdt.withColumn("Title", regexp_replace(col("Title"), '[\\|]+', ''))
sortdt = sortdt.withColumn("Title", regexp_replace(col("Title"), '-', ''))

sortdt = sortdt.withColumn("COMPOSER", regexp_replace(col("COMPOSER"), '[//bUNKNOWN WRITER (999990)//b]+', ''))
sortdt = sortdt.withColumn("COMPOSER", regexp_replace(col("COMPOSER"), '[//bUNKNOWN WRITER//b]+', ''))
#supposed to reduce multiple whitespaces to just 1
sortdt = sortdt.withColumn('Title', F.trim(regexp_replace(sortdt.Title, " +", " ")))
#drop dups
sortdt = sortdt.dropDuplicates(['Title', 'COMPOSER'])
#drop null titles & composers
sortdt = sortdt.filter(sortdt.Title.isNotNull())
sortdt = sortdt.filter(sortdt.COMPOSER.isNotNull())
#sort the rest alphabetically
sortdt = sortdt.orderBy('Title', ascending=True)


#rename columns
sortdt = sortdt.withColumnRenamed('Custom ID', 'Downtown_ID')
sortdt = sortdt.withColumnRenamed('COMPOSER', 'Downtown_Composer')

#-----------------------------------
#YOUTUBE
#-----------------------------------
yt = '/Users/valerieangulo/Downtown/fuzzymatching/YT_data.csv'
ytdf = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", ":").option("header", "true").option("inferSchema", "true").load(yt)
myyt = ytdf
myyt = ytdf[['asset_id','title','writers']]
myyt = myyt.filter(myyt.asset_id.isNotNull()) 
sortYT = myyt


#trim whitespaces
sortYT = sortYT.withColumn('title', F.trim(sortYT.title))
sortYT = sortYT.withColumn('writers', F.trim(sortYT.writers))
#YT tabs
sortYT = sortYT.withColumn("title", regexp_replace(col("title"), '[\t]+', ''))
sortYT = sortYT.withColumn("writers", regexp_replace(col("writers"), '[\t]+', ''))
#YT new lines
sortYT = sortYT.withColumn("title", regexp_replace(col("title"), '[\n]+', ''))
sortYT = sortYT.withColumn("writers", regexp_replace(col("writers"), '[\n]+', ''))
#make caps
sortYT = sortYT.withColumn('title', F.upper(col('title'))) #test(F.upper(['title', 'writers']))
sortYT = sortYT.withColumn('writers', F.upper(col('writers')))
#remove quotes, unknowns, |, -
sortYT = sortYT.withColumn("title", regexp_replace(col("title"), '"', ''))
sortYT = sortYT.withColumn("title", regexp_replace(col("title"), '\'', ''))
sortYT = sortYT.withColumn("title", regexp_replace(col("title"), '[\\|]+', ''))
sortYT = sortYT.withColumn("title", regexp_replace(col("title"), '-', ''))

sortYT = sortYT.withColumn("writers", regexp_replace(col("writers"), '[//bUNKNOWN WRITER (999990)//b]+', ''))
sortYT = sortYT.withColumn("writers", regexp_replace(col("writers"), '[//bUNKNOWN WRITER//b]+', ''))
#supposed to reduce multiple whitespaces to just 1
sortYT = sortYT.withColumn('title', F.trim(regexp_replace(sortYT.title, " +", " ")))
#drop dups
sortYT = sortYT.dropDuplicates(['title', 'writers'])
#drop null titles and writers
sortYT = sortYT.filter(sortYT.title.isNotNull())
sortYT = sortYT.filter(sortYT.writers.isNotNull())
#sort the rest alphabetically
sortYT = sortYT.orderBy('title', ascending=True)


#rename columns
sortYT = sortYT.withColumnRenamed('asset_id', 'YT_ID')
sortYT = sortYT.withColumnRenamed('title', 'YT_Title')
sortYT = sortYT.withColumnRenamed('writers', 'YT_Writers')

#----------------------------------------------
#Merging by title
#----------------------------------------------
#Join DFs on titles with levenshtein distance 

from pyspark.sql.functions import levenshtein 
joinedDF = sortdt.join(sortYT, levenshtein(sortdt["Title"], sortYT["YT_Title"]) < 3) 
YTDT = joinedDF[['Downtown_ID','YT_ID','Title','Downtown_Composer','YT_Writers','ratio']]

#do levenshtein distance
from pyspark.sql.functions import levenshtein  
ratioYTDT = YTDT.withColumn('ratio',levenshtein(col('Downtown_Composer'), col('YT_Writers')))

#keep all rows with ratio >= 85
#YTDT = YTDT.filter(YTDT['ratio']<= 15) #whats a good ld to stop at?
#drop ratio column
#YTDT = YTDT.drop('ratio')
#save to output file
YTDT.write.csv("matches.csv")










