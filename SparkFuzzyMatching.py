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

#Involving pandas in order to take out emojis, havent tried with spark DF
#--------------------------------------------------------------
#pip install PyArrow #Needed to do pandas <-> DataFrame
import pandas as pd
import re

DTdf = sortdt.select('*').toPandas()
YTdf = sortYT.select('*').toPandas()

#To remove pesky emojis from YT data
emoji_pattern = re.compile("["
                u"\U0001F600-\U0001F64F"  # emoticons
                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                u"\U00002702-\U000027B0"
                u"\U000024C2-\U0001F251"
                u"\U0001f926-\U0001f937"
                u'\U00010000-\U0010ffff'
                u"\u200d"
                u"\u2640-\u2642"
                u"\u2600-\u2B55"
                u"\u23cf"
                u"\u23e9"
                u"\u231a"
                u"\u3030"
                u"\ufe0f"
    "]+", flags=re.UNICODE)

YTdf['YT_Title'] = YTdf['YT_Title'].replace(emoji_pattern, '')

sortYT = sortYT.withColumn("YT_Title", regexp_replace(col("YT_Title"), emoji_pattern, ''))

#merge the pandas dfs
complete = DTdf.merge(YTdf, left_on='Title', right_on='YT_Title', how='inner', suffixes=['_dt', '_yt'])
#convert back to spark
spark_df = sqlContext.createDataFrame(complete)




#----------------------------------------------------
#----------------------------------------------------
####JOIN DFs on LEVENSHTEIN BETWEEN TITLES???

from pyspark.sql.functions import levenshtein 
joinedDF = sortdt.join(sortYT, levenshtein(sortdt["Title"], sortYT["YT_Title"]) < 3) 
YTDT = joinedDF[['Downtown_ID','YT_ID','Title','Downtown_Composer','YT_Writers','ratio']]

#----------------------------------------------------
#----------------------------------------------------

#drop title and rearrange columns
YTDT = spark_df[['Downtown_ID','YT_ID','Title','Downtown_Composer','YT_Writers']]

#do levenshtein distance
from pyspark.sql.functions import levenshtein  

		##things to try:
		ratioYTDT = YTDT.withColumn('ratio',levenshtein(col('Downtown_Composer'), col('YT_Writers')))


		#for DF joined by levenshtein
		ratioYTDT = joinedDF.withColumn('ratio',levenshtein(col('Downtown_Composer'), col('YT_Writers')))

#keep all rows with ratio >= 85
YTDT = YTDT.filter(YTDT['ratio']<= 85) #whats a good ld to stop at?
#drop ratio column
YTDT = YTDT.drop('ratio')
#save to output file
YTDT.write.csv("matches.csv")











################################


/share/apps/anaconda3/4.3.1/lib/python3.6/collections/__init__.py in namedtuple(*args, **kwargs)
    379 
    380     def namedtuple(*args, **kwargs):
--> 381         cls = _old_namedtuple(*args, **kwargs)
    382         return _hack_namedtuple(cls)
    383 

cd /share/apps/anaconda3/4.3.1/lib/python3.6/collections
vi __init__.py 

fix....
cls = _old_namedtuple(*args, **kwargs, verbose=False, rename=False, module=None)

 def namedtuple(typename, field_names, *, verbose=False, rename=False, module=None):
 357     "Returns a new subclass of tuple with named fields."





#-----------------------------------------
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


#from pyspark.context import SparkContext
#sc = SparkContext('local')

from pyspark.sql.session import SparkSession
spark = SparkSession(sc)

#-----------------------------------
#DOWNTOWN
#-----------------------------------
#dt = 'songtrust_match.csv'
dt = '/Users/valerieangulo/Downtown/fuzzymatching/songtrust_match.csv'
dtdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(dt)

mydt = dtdf
mydt = dtdf[['Custom ID',
 'Title',
 'COMPOSER']]
mydt.withColumn('ratio', 0)

sortdt = mydt
sortdt['Title'] = sortdt['Title'].str.upper().str.strip(' \t\n').replace('"','')
sortdt['Title'] = sortdt['Title'].str.strip(' ').str.strip('\t').str.strip('\n')

sortdt.Title = sortdt.Title.str.strip(' ').str.replace('[\\-]+', '')
sortdt.Title = sortdt.Title.str.strip(' ').str.replace('[\\|]+', '')

sortdt['COMPOSER'] = sortdt['COMPOSER'].str.upper().str.strip(' \t\n')
sortdt['COMPOSER'] = sortdt['COMPOSER'].str.strip(' ').str.strip('\t').str.strip('\n')

sortdt.COMPOSER = sortdt.COMPOSER.replace('UNKNOWN', np.NaN)
sortdt.COMPOSER = sortdt.COMPOSER.replace('UNKNOWN WRITER', np.NaN)
sortdt.COMPOSER = sortdt.COMPOSER.replace('UNKNOWN WRITER (999990)', np.NaN)

sortdt = sortdt.sort_values('Title')
sortdt = sortdt.drop_duplicates(subset=['Title', 'COMPOSER'], keep='first') 
sortdt = sortdt.dropna(axis=0, subset=['Title'])

sortdt.to_csv('sortcdb.csv')

#-----------------------------------
#YOUTUBE
#-----------------------------------
yt = 'YT_data.csv'
ytdf = pd.read_csv(yt, header = 0, delimiter = ':', na_values= '', index_col=0)
yttw = ytdf[['asset_id','title','writers']]
yttw = yttw.dropna(axis=0, subset=['asset_id'])
sortyt = yttw

sortyt['title'] = sortyt['title'].str.upper()
sortyt['title'] = sortyt['title'].str.strip(' ').str.strip('\t').str.strip('\n')
sortyt['writers'] = sortyt['writers'].str.upper()
sortyt['writers'] = sortyt['writers'].str.strip(' ').str.strip('\t').str.strip('\n')

#this strip isnt working
sortyt['title'] = sortyt['title'].str.strip(' \t\n').str.replace('"','')
sortyt['writers'] = sortyt['writers'].str.strip(' \t\n').str.replace('?','') 
## take out all unknowns
sortyt['writers'] = sortyt['writers'].replace('UNKNOWN WRITER (999990)', np.NaN)
sortyt['writers'] = sortyt['writers'].replace('UNKNOWN WRITER', np.NaN)
sortyt['writers'] = sortyt['writers'].replace('UNKNOWN', np.NaN)

sortyt['title'] = sortyt['title'].replace('', np.NaN)
sortyt = sortyt.dropna(axis=0, subset=['title'])

sortyt['title'] = sortyt['title'].str.strip(' ').str.replace('[\\-]+', '')
sortyt['title'] = sortyt['title'].str.strip(' ').str.replace('[\\|]+', '')
## This doesnt work
#test = sortyt
##test['title'] = test['title'].str.strip('??').str.replace('[\\?]+', '')

sortyt = sortyt.sort_values('title')
sortyt = sortyt.drop_duplicates(subset=['title', 'writers'])

sortyt.to_csv('sortedyt.csv')

#----------------------------------------------
#Merging by title
#----------------------------------------------
testdt = sortdt
testyt = pd.read_csv('sortedyt.csv', header = 0, delimiter = ',', na_values= '', index_col=0)
fullmergedf = testdt.merge(testyt, left_on='Title', right_on='title', how='inner', suffixes=['_dt', '_yt'])
full = fullmergedf
full = full.drop(['title'], axis = 1)

### Longest process
	#full.info()
	#Int64Index: 27079014 entries, 0 to 27079013
	#Data columns (total 6 columns):
	#memory usage: 1.4+ GB
#10:27 pm ---> ~11:28
full['ratio'] = full.apply(lambda row: fuzz.token_set_ratio(row['COMPOSER'], row['writers']), axis=1)
full.to_csv(('finalCopyCDB.csv'))
## rearrange columns
dt_matches = full
cols = list(dt_matches.columns.values)
df0 = dt_matches.pop('Title')
df1 = dt_matches.pop('asset_id')
df2 = dt_matches.pop('COMPOSER')
df3 = dt_matches.pop('writers')

dt_matches['asset_id'] = df1
dt_matches['Title'] = df0
dt_matches['cdb_composer'] = df2
dt_matches['youtube_writers'] = df3

dt_matches.to_csv(('finalCopyCDB.csv'))


### TO EXCLUDE ALL NULLS ########
copymerge = pd.read_csv('finalCopyCDB.csv', header = 0, delimiter = ',', na_values= '', index_col=0)

### set all nulls to 0 match ratio, fast!
copymerge.loc[copymerge['cdb_composer'].isnull(), 'ratio'] = 0
copymerge.loc[copymerge['youtube_writers'].isnull(), 'ratio'] = 0
ratio = copymerge

#df.filter(df['ratio'] > 84).show()
copy = ratio[~(ratio['ratio'] < 85)] 
finalcopy = copy

finalcopy = finalcopy.drop(['ratio'], axis = 1)
finalcopy.to_csv('cdb_matches_without_nulls.csv')

### TO INCLUDE ONLY NULLS ########
copymerge2 = pd.read_csv('finalCopyCDB.csv', header = 0, delimiter = ',', na_values= '', index_col=0)

copymerge2.loc[copymerge['cdb_composer'].isnull(), 'ratio'] = 110
copymerge2.loc[copymerge['youtube_writers'].isnull(), 'ratio'] = 110
ratio2 = copymerge2
copy2 = ratio2[~(ratio2['ratio'] < 110)] 
finalcopy2 = copy2

finalcopy2 = finalcopy2.drop(['ratio'], axis = 1)
finalcopy2.to_csv('null_cdb_matches.csv')




