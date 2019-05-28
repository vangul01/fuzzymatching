# Notes:
#
#  2 final files
#       1. no nulls for YT DT
#       2. nulls for YT artist, DT composer or both null
#  data was cleaned to make all UNKNOWN artists in YT and DT datafiles null 
#       {'UNKNOWN WRITER', 'UNKNOWN', 'UNKNOWN WRITER (999990)'}
#  merged dfs --> exact match on title
#  fuzzy matching on writers/composers with token_set_ratio
#  entries containing ??? not cleaned....
#  ratio computed for all columns ~15 min
#  For CD Baby:
#  		52545 matches with no nulls, 489043 matches only nulls
#

#-----------------------------------------
import pandas as pd
import numpy as np
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import csv
import re

#-----------------------------------
#DOWNTOWN
#-----------------------------------
dt = 'cdb_match.csv'
dtdf = pd.read_csv(dt, header = 0, delimiter = ',', na_values= '', index_col=0)
dtdf.index.names = ['Song ID']
dtdf.reset_index(level=0, inplace=True) #to keep song ID as column
mydt = dtdf
mydt = dtdf.ix[:, :4]
mydt = mydt.drop(['ISWC'], axis = 1)
mydt['ratio'] = 0

#### clean columns for dt files
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
copymerge = pd.read_csv('finalCopyCDB.csv', header = 0, delimiter = ':', na_values= '', index_col=0)

### set all nulls to 0 match ratio, fast!
copymerge.loc[copymerge['cdb_composer'].isnull(), 'ratio'] = 0
copymerge.loc[copymerge['youtube_writers'].isnull(), 'ratio'] = 0
ratio = copymerge
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




