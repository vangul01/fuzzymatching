My Plan:

import datasets
  add ratio column to ST dataset
  get rid of asset_id nulls in YT, title nulls in ST

Clean datasets  
  capitalize all titles and writers for ST and YT
  strip spaces, 


Notes on file matches:

  songtrust_matches.csv 
      made from copymergest fulldf csv merged with title: [3144738 rows x 7 columns]

      number of matches = 21652
      ST had 821806 rows, 551048 with cleanup
      YT had 11 million rows, 7.6 million with cleanup 
      
      dropped null for titles in ST and YT
      merged titles --> exact match on title
      fuzzy matching on artist with token_set_ratio
      ignore NULL, NaN, UKNOWN any artist name with ???

      What I want to do to maximize matches:
        fuzzy match on titles with fuzz.ratio
            merge on these titles maybe in a new DB
        ignore NULL, NaN, UKNOWN any artist name with ???

      To do this:
        I have to clean the data better especially for YT
            make all weird values NaN for title AND composer
            make sure all NaN title and composer are Nan in ST 
        I can still merge on title, BUT ignore the weird values, I might
        get more matches





-----------------------------------------
import pandas as pd
import numpy as np
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import csv

-----------------------------------
Clean that Songtrust!
-----------------------------------
st = 'songtrust_match.csv'
stdf = pd.read_csv(st, header = 0, delimiter = ',', na_values= '', index_col=0)
stdf.index.names = ['Song ID']
stdf.reset_index(level=0, inplace=True) #to keep song ID as column
sttw = stdf
sttw = stdf.ix[:, :4].drop(['ISWC'], axis = 1)
sttw['ratio'] = 0
sttw = sttw.dropna(axis=0, subset=['Title'])

#### clean columns
sortst = sttw
sortst['Title'] = sortst['Title'].str.upper().str.strip(' \t\n').replace('"','')
sortst['Title'] = sortst['Title'].str.strip(' ').str.strip('\t').str.strip('\n')
sortst.Title = sortst.Title.str.strip(' ').str.replace('[\\-]+', '')
sortst.Title = sortst.Title.str.strip(' ').str.replace('[\\|]+', '')

sortst['COMPOSER'] = sortst['COMPOSER'].str.upper().str.strip(' \t\n')
sortst['COMPOSER'] = sortst['COMPOSER'].str.strip(' ').str.strip('\t').str.strip('\n')
sortst.COMPOSER = sortst.COMPOSER.replace('UNKNOWN WRITER', np.NaN)
sortst.COMPOSER = sortst.COMPOSER.replace('UNKNOWN', np.NaN)

sortst = sortst.sort_values('Title')
sortst = sortst.drop_duplicates(subset=['Title', 'COMPOSER'], keep='first') #keeps first dup as default
sortst = sortst.dropna(axis=0, subset=['Title'])

sortst.to_csv('sortedst.csv')


----------------------
In [165]: sortst.info()
<class 'pandas.core.frame.DataFrame'>
Int64Index: 550977 entries, 2304573 to 3153594
Data columns (total 3 columns):
Title       550977 non-null object
COMPOSER    300794 non-null object
ratio       550977 non-null int64
dtypes: int64(1), object(2)
memory usage: 16.8+ MB
----------------------
## To check string values
(sortst.COMPOSER == 'UNKNOWN WRITER').sum()
(sortst.COMPOSER == 'UNKNOWN').sum()

-----------------------------------
YOUTUBE TIME!
-----------------------------------
## GOAL: to make a universally clean YT dataset! :D

yt = 'YT_data.csv'
ytdf = pd.read_csv(yt, header = 0, delimiter = ':', na_values= '', index_col=0)
yttw = ytdf[['asset_id','title','writers']]
yttw = yttw.dropna(axis=0, subset=['asset_id'])
sortyt = yttw

sortyt['title'] = sortyt['title'].str.upper()
sortyt['title'] = sortyt['title'].str.strip(' ').str.strip('\t').str.strip('\n')
sortyt['writers'] = sortyt['writers'].str.upper()
sortyt['writers'] = sortyt['writers'].str.strip(' ').str.strip('\t').str.strip('\n')

#idk why this strip isnt working......... 
sortyt['title'] = sortyt['title'].str.strip(' \t\n').str.replace('"','')
sortyt['writers'] = sortyt['writers'].str.strip(' \t\n').str.replace('?','') 
## take out all unknowns
sortyt['writers'] = sortyt['writers'].replace('UNKNOWN WRITER (999990)', np.NaN)
sortyt['writers'] = sortyt['writers'].replace('UNKNOWN WRITER', np.NaN)
sortyt['writers'] = sortyt['writers'].replace('UNKNOWN', np.NaN)

sortyt['title'] = sortyt['title'].replace('', np.NaN)
sortyt = sortyt.dropna(axis=0, subset=['title'])

### This works??? but the ? doesn't...
sortyt['title'] = sortyt['title'].str.strip(' ').str.replace('[\\-]+', '')
sortyt['title'] = sortyt['title'].str.strip(' ').str.replace('[\\|]+', '')
## This refuses to work, I'm just gonna leave the ? in....
##test['title'] = test['title'].str.strip('??').str.replace('[\\?]+', '')

sortyt = sortyt.sort_values('title')
sortyt = sortyt.drop_duplicates(subset=['title', 'writers'])

sortyt.to_csv('sortedyt.csv')

#### I have to clean this for the full merged dataset, 
##ideally for the youtube data since I reuse it for all the DT sets
{'UNKNOWN WRITER', 'UNKNOWN', 'UNKNOWN WRITER (999990)'}

## To check string values
(sortyt.writers == 'UNKNOWN WRITER').sum()
(sortyt.writers == 'UNKNOWN').sum()
(sortyt.writers == 'UNKNOWN WRITER (999990)').sum()

----------------------------- ABOVE IS THE PRELIM CLEANING...... 
---------Now its merging time!----------------------------------

testyt
testst = sortst
fullmergest = testst.merge(testyt, left_on='Title', right_on='title', how='inner', suffixes=['_st', '_yt'])

--------------------------------------------------
In [172]: testyt.info()
<class 'pandas.core.frame.DataFrame'>
Int64Index: 7450809 entries, 11574491 to 8368115
Data columns (total 3 columns):
asset_id    object
title       object
writers     object
dtypes: object(3)
memory usage: 227.4+ MB

In [176]: testst.info()
<class 'pandas.core.frame.DataFrame'>
Int64Index: 550977 entries, 2304573 to 3153594
Data columns (total 3 columns):
Title       550977 non-null object
COMPOSER    300794 non-null object
ratio       550977 non-null int64
dtypes: int64(1), object(2)
memory usage: 16.8+ MB

In [178]: fullmergest.info()
<class 'pandas.core.frame.DataFrame'>
Int64Index: 3068811 entries, 0 to 3068810
Data columns (total 6 columns):
Title       object
COMPOSER    object
ratio       int64
asset_id    object
title       object
writers     object
dtypes: int64(1), object(5)
memory usage: 163.9+ MB

In [180]: fullmergest.isnull().sum()
Out[180]: 
Title            0
COMPOSER     12691
ratio            0
asset_id         0
title            0
writers     102861
dtype: int64
-----------------------------------------------




-------------- 2/26/2019 -----------------
###### GOAL FOR TODAY #######3
### do fuzzy matching on writers with merged title csv
#### ISSUE, if 'writers' is NaN, '' , UNKNOWN or contains more than one ? then skip!

### THIS VERSION HAS EXACT MACTHING ON TITLE, fuzzy matching on artist
    ## does not include weird data from youtube....
-------------------    
copymergest = fullmergest

In [187]: copymergest.info()
<class 'pandas.core.frame.DataFrame'>
Int64Index: 3068811 entries, 0 to 3068810
Data columns (total 6 columns):
Title       object
COMPOSER    object
ratio       int64
asset_id    object
title       object
writers     object
dtypes: int64(1), object(5)
memory usage: 163.9+ MB


#(1:35) ---> 
copymergest['ratio'] = copymergest.apply(lambda row: fuzz.token_set_ratio(row['COMPOSER'], row['writers']), axis=1)
copymergest.to_csv('finalCopyST.csv')

copymergest = pd.read_csv('finalCopyST.csv', header = 0, delimiter = ',', na_values= '', index_col=0)
### exclude nulls
copymergest.loc[copymergest['COMPOSER'].isnull(), 'ratio'] = 0
copymergest.loc[copymergest['writers'].isnull(), 'ratio'] = 0
ratiost = copymergest
copy2st = ratiost[~(ratiost['ratio'] < 85)] #keep ratios >= 85
finalcopyst = copy2st

#need dtID, ytID, Title, composers, artists
st_matches = finalcopyst.drop(['title','ratio'], axis = 1)
## rearrange columns
cols = list(st_matches.columns.values)
df0 = st_matches.pop('Title')
df1 = st_matches.pop('asset_id')
df2 = st_matches.pop('COMPOSER')
df3 = st_matches.pop('writers')

st_matches['asset_id'] = df1
st_matches['Title'] = df0
st_matches['songtrust_composer'] = df2
st_matches['youtube_writers'] = df3

st_matches.to_csv('st_matches_without_nulls.csv')

------------- Info -----------------------
###NO NULLS
In [271]: finalcopyst.info()
<class 'pandas.core.frame.DataFrame'>
Int64Index: 21015 entries, 13 to 3068778
Data columns (total 7 columns):
Song ID     21015 non-null int64
Title       21015 non-null object
COMPOSER    21015 non-null object
ratio       21015 non-null int64
asset_id    21015 non-null object
title       21015 non-null object
writers     21015 non-null object
dtypes: int64(2), object(5)
memory usage: 1.3+ MB

###NULLS
In [253]: finalcopystnulls.info()
<class 'pandas.core.frame.DataFrame'>
Int64Index: 114982 entries, 21 to 3068809
Data columns (total 7 columns):
Song ID     114982 non-null int64
Title       114982 non-null object
COMPOSER    102291 non-null object
ratio       114982 non-null int64
asset_id    114982 non-null object
title       114982 non-null object
writers     12121 non-null object
dtypes: int64(2), object(5)
memory usage: 7.0+ MB
------------------------------------------
st_matches.to_csv('st_matches.csv')



##### This makes ratios of Nulls 85 so that they can be included! :) 
##### time = immediate! :D 
copymergest.loc[copymergest['COMPOSER'].isnull(), 'ratio'] = 110
copymergest.loc[copymergest['writers'].isnull(), 'ratio'] = 110 
##########################################################
#Check by counting how many 85's there are in ratio
#In [22]: (copymergest.ratio ==  110).sum()
#Out[22]: 114982
##### Drop ratios != 110
ratiostnulls = copymergest
copy2stnulls = ratiostnulls[~(ratiostnulls['ratio'] < 110)] #keep ratios >= 85
finalcopystnulls = copy2stnulls
#finalcopyst.to_csv('finalCopyST.csv')
#21585 without NULLS

#need dtID, ytID, Title, composers, artists
st_null_matches = finalcopystnulls.drop(['title','ratio'], axis = 1)
## rearrange columns
cols = list(st_null_matches.columns.values)
df0 = st_null_matches.pop('Title')
df1 = st_null_matches.pop('asset_id')
df2 = st_null_matches.pop('COMPOSER')
df3 = st_null_matches.pop('writers')

st_null_matches['asset_id'] = df1
st_null_matches['Title'] = df0
st_null_matches['songtrust_composer'] = df2
st_null_matches['youtube_writers'] = df3

st_null_matches.to_csv('null_st_matches.csv')












































### append rows from both sorted dfs to new df

mergethem = pd.DataFrame()

for x in A_List_ST['Title']:
    strx = x

    for y in A_List_YT['title']:
        stry = y
        ratio = fuzz.ratio(stry, strx) #token_set_ratio(stry, strx)
        
        if ratio >= 85 :


---ideas
-make a new df with all columns of st and yt and an empty column for ratio 
-make copies of both dfs and merge these copies rows if theres a match
  -add empty row to a df and make ratio go in that row 
-

for x in A_List_ST['Title']:
    strx = x

    for y in A_List_YT['title']:
        stry = y
        ratio = fuzz.ratio(stry, strx) #token_set_ratio(stry, strx)
        
        if ratio >= 85 :
           	#print("ST= ", x, "  YT= ", y,"  Ratio = %d" % ratio)
            #MAKE A DF OF THIS STUFF!!! Or a CSV?


### NEW IDEA: since its ordered alphabetically, don't have each search start from zero, have it search in the part that was left off
copyfulldf = fulldf

for x in testST['COMPOSER']:
    stwriter = x
    
    for y in testYT['writers']:
        ytwriter= y
        ratio1 = fuzz.token_sort_ratio(stwriter, ytwriter)
        
        if ratio >= 85 :
              print("ST= ", x, "  YT= ", y, " loc = ", A_List_YT.index(y) ,"  Ratio = %d" % ratio)



****NEW DEVELOPMENT****
mergedf = testst.merge(testyt, left_on='Title', right_on='title', how='inner', suffixes=['_st', '_yt'])

ATTEMPT MERGE ON EXACT TITLES
  -then fuzzy match on artists!! 

###TEST THIS

for x in copy['letter']:
    stwriter = x
    ytwriter= copy['letter2']
    ratio = fuzz.token_sort_ratio(stwriter, ytwriter)
        
    if ratio >= 85 :
      copyfulldf['ratio'] = ratio





------- List stuff ---------
#In [95]: len(ytitles)
#Out[95]: 222485

### get only titles for A, drop duplicates

#AtitlesYT = testyt['title']
#AtitlesYT = AtitlesYT.drop_duplicates()
#A_List_YT = [x for x in AtitlesYT if x.startswith('A')]
#A_List_YT[:50]
#seriesA = pd.Series(A_List_YT)
#seriesA.to_csv('AListYT.csv')

#In [105]: len(A_List_YT)
#Out[105]: 155717
-------------------------

### if any titles from songtrust matches any title in YT A-List, add the title to a list. 
    ##Find the artist and number in youtube
    ##find the composer in songtrust
    ##see if they match
      ## Yes! = add to list of A songs that we get paid for
      ## no = do nothing


# make list of only A's in songtrust 
--------------------
#AtitlesST = sortst['Title']
#AtitlesST = AtitlesST.drop_duplicates()
#A_List_ST = [x for x in AtitlesST if x.startswith('A')] 
#A_List_ST[:50]
#stSeriesA = pd.Series(A_List_ST)
#stSeriesA.to_csv('AListST.csv')

#In [113]: len(stSeriesA)
#Out[113]: 15108
--------------------

#### the fuzzy matching, use lists. First choose ST, the inner loop is YT

#for x in A_List_ST:
#    strx = x
    
#    for y in A_List_YT:
#        stry = y
#        ratio = fuzz.ratio(stry, strx) #token_set_ratio(stry, strx)
        
#        if ratio >= 85 :
#            print("ST= ", x, "  YT= ", y, " loc = ", A_List_YT.index(y) ,"  Ratio = %d" % ratio)



## to do next time, have this append to a list
##also try to get the artists from st and yt and the yt code somehow... make a df and export to csv

  ## this is to keep our dfs
A_List_ST = sortst[sortst['Title'].str.startswith('A')]
A_List_YT = testyt[testyt['title'].str.startswith('A')]
A_List_ST.to_csv('AListST.csv')
A_List_YT .to_csv('AListYT.csv')

------------------------------------------
A_List_ST.info = [16740 rows x 3 columns]
A_List_YT.info = [222107 rows x 3 columns]
------------------------------------------
From here I just want to fuzzy match between the title (and possibly artist) rows 

fuzz.token_sort_ratio

----when starting over....

import pandas as pd
import numpy as np
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import csv

ayt = 'AListYT.csv'
A_List_YT = pd.read_csv(ayt, header = 0, delimiter = ',', na_values= '', index_col=0)
ast = 'AListST.csv'
A_List_ST = pd.read_csv(ast, header = 0, delimiter = ',', index_col=0)


---------------------------



Merging DataFrames


In [11]: df
Out[11]: 
   A  B
0  1  0
1  3  1
2  5  2
3  7  3


In [30]: df2
Out[30]: 
   C  D
0  1  4
1  3  3
2  4  2
3  6  1
4  7  0


In [28]: df3 = df.merge(df2, left_on='A', right_on='C', how='left', suffixes=['', '_B'])

In [29]: df3
Out[29]: 
   A  B   C   D
0  1  0   1   4
1  3  1   3   3
2  5  2 NaN NaN
3  7  3   7   0


In [33]: df3 = df.merge(df2, left_on='A', right_on='C', how='inner', suffixes=['', '_B'])

In [34]: df3
Out[34]: 
   A  B  C  D
0  1  0  1  4
1  3  1  3  3
2  7  3  7  0

We want to see if a song in ST is in YT, we dont care if a song in YT is not in ST
	do we care about if songs in ST arent in YT??? if so do left merge and if not do inner 


Example for YT and ST


1. have dfST that has songcode, title, artist
   have dfYT that has asset_id, title, artist

2. filter *non-duplicate* *A values* for both of these dfs 
		issues: can i filter while still keeping the df and not changing to a list?
		Yes!!
		A_List_ST = sortst[sortst['Title'].str.startswith('A')]
		A_List_YT = testyt[testyt['title'].str.startswith('A')]

		OH!!! Only duplicates with same title and artist name are dropped, no need for duplicate st columns
		But what about duplicates with lotttsss of artists (ie: title = A) ? Have to match on artist too

merge data bases on fuzzy matching!

df3 = ST.merge(YT, left_on='Title(st)', right_on='title(yt)', how='inner', suffixes=['_st', '_yt'])















