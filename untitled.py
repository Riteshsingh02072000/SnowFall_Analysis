import numpy as np
import pandas as pd
from plotly.offline import iplot, init_notebook_mode
import plotly.graph_objs as go
import tarfile
import gzip
import re
import os
import datetime as dt
import warnings
warnings.filterwarnings("ignore")

init_notebook_mode(connected=True)

#stat_num = 1000 # Number of stations to plot for testing
year_num = 20 # Number of past years to consider
#extremes_num = 10 # Number of hottest and coldest places to display

yearfiles = os.listdir("D:/Big data/archive/gsod_all_years")
yearfiles.sort()
yearfiles = yearfiles[-year_num:]
#need to check
years = [int(re.findall('\d+',yearfile)[0]) for yearfile in yearfiles]

station_loc = pd.read_csv('D:/Big data/archive/isd-history.csv')
station_loc = station_loc.replace([0.0, -999.0, -999.9],np.nan)
station_loc = station_loc[pd.notnull(station_loc['LAT']) & pd.notnull(station_loc['LON'])]
#need to check
station_loc = station_loc[[int(re.findall('^\d{4}', str(end_year))[0])==max(years) for end_year in station_loc['END']]]
station_loc = station_loc[[int(re.findall('^\d{4}', str(beg_year))[0])<=min(years) for beg_year in station_loc['BEGIN']]]
#print station_loc

station_loc['LBL'] = station_loc[['STATION NAME','STATE','CTRY']].apply(lambda x: x.str.cat(sep=', '), axis=1)
station_loc['ELEV_LBL'] = station_loc['ELEV(M)'].apply(lambda x: 'Elevation: '+str(x)+' m' if ~np.isnan(x) else np.nan)
station_loc['LBL'] = station_loc[['LBL','ELEV_LBL']].apply(lambda x: x.str.cat(sep='<br>'), axis=1)
station_loc = station_loc.drop(['STATION NAME','STATE','ELEV_LBL','ICAO','BEGIN','END'], axis=1)
#station_loc = station_loc.sample(stat_num)

df = pd.DataFrame([])
df_day = pd.DataFrame([])

def preprocess_station_file_content(content):
    headers=content.pop(0)
    headers=[headers[ind] for ind in [0,1,2,3,4,8,11,12,13]]
    for d in range(len(content)):
        content[d]=[content[d][ind] for ind in [0,1,2,3,5,13,17,18,19]]
    content=pd.DataFrame(content, columns=headers)
    content.rename(columns={'STN---': 'USAF'}, inplace=True)
    content['MAX'] = content['MAX'].apply(lambda x: re.sub("\*$","",x))
    content['MIN'] = content['MIN'].apply(lambda x: re.sub("\*$","",x))
    #content['PRCP'] = content['PRCP'].apply(lambda x: re.sub("\+$","",x))
    #content['PRCP'] = content['PRCP'].apply(lambda x: x.replace("G",""))
    #content['PRCP'] = content['PRCP'].apply(lambda x: x.replace(x[len(x) - 1:], ""))
    content['PRCP'] = content['PRCP'].apply(lambda x: re.sub(x,x[:-1],x))
    content[['WBAN','TEMP','DEWP','WDSP','MAX','MIN','PRCP']] = content[['WBAN','TEMP','DEWP','WDSP','MAX','MIN','PRCP']].apply(pd.to_numeric)
    content['YEARMODA']=pd.to_datetime(content['YEARMODA'], format='%Y%m%d', errors='ignore')
    content['YEAR']=pd.DatetimeIndex(content['YEARMODA']).year
    content['MONTH']=pd.DatetimeIndex(content['YEARMODA']).month
    content['DAY']=pd.DatetimeIndex(content['YEARMODA']).day
    return content

for yearfile in yearfiles:
    print(yearfile)
    i=0
    tar = tarfile.open("D:/Big data/archive/gsod_all_years/"+yearfile, "r")
    print(len(tar.getmembers()[1:]))
    #for member in np.random.choice(tar.getmembers()[1:], size=stat_num, replace=False):
    for member in tar.getmembers()[1:]:
        name_parts = re.sub("\.op\.gz$","",re.sub("^\./","",member.name)).split("-")
        usaf = name_parts[0]
        wban = int(name_parts[1])
        if station_loc[(station_loc['USAF']==usaf) & (station_loc['WBAN']==wban)].shape[0]!=0:
            i=i+1
            #if i%(stat_num//10) == 0: print(i)
            f=tar.extractfile(member)
            f=gzip.open(f, 'rb')
            content=[re.sub(" +", ",", line.decode("utf-8")).split(",") for line in f.readlines()]
            content=preprocess_station_file_content(content)
            #df_day = df_day.append(content[(content['MONTH']==day.month) & (content['DAY']==day.day)])
            df_day = df_day.append(content[content['YEARMODA']==content['YEARMODA'].max()])
            content = content.groupby(['USAF','WBAN','YEAR','MONTH']).agg('median').reset_index()
            df = df.append(content)
    tar.close()

df_loc = pd.merge(df, station_loc, how='inner', on=['USAF','WBAN'])
df_loc.to_csv('D:/Big data/csv/sample.csv')

import findspark
findspark.init()

import pyspark.pandas
 #import databricks.koalas

import numpy
 #import matplotlib.pyplot as plt
import pandas

from pyspark.sql import SparkSession
# from pyspark.ml.regression import RandomForestRegressor
# from pyspark.ml.evaluation import RegressionEvaluator
# from sklearn.ensemble import RandomForestRegressor

# #Reading the data
# #data = spark.read.format("libsvm").load("D:/Big data/csv/sample.csv")
# #data= pd.read_csv('D:/Big data/csv/sample.csv')
# #pyspark.read.option("header","false").csv("D:/Big data/csv/sample.csv")

# data= pyspark.pandas.read_csv('D:/Big data/csv/sample.csv',headers= False)

# # just checking
# #print(data)
# #print(type(data))

# #(trainingData, testData) = data.randomSplit([0.7, 0.3])

# splits = data.to_spark().randomSplit([0.7, 0.3], seed=12)
# trainingData = splits[0].to_koalas()
# testData = splits[1].to_koalas()

# #splits = data.randomSplit([0.7, 0.3], 24)
# #trainingData = splits[0].to_koalas()
# #testData = splits[1].to_koalas()

# train_x= trainingData.iloc [:,5]
# train_y= trainingData.iloc [:,10]
# test_x= testData.iloc [:,5]

# #print ("--------x----------------------------------")
# #print(train_x)

# #print ("--------y----------------------------------")
# #print(train_y)

# #print ("------------------------------------------")
# regressor = RandomForestRegressor(n_estimators = 100, random_state = 0)
# #regressor.fit(train_x.to_frame(), train_y)

# train_x = train_x.to_numpy()
# train_y = train_y.to_numpy()
# test_x = test_x.to_numpy()

# #make them 2D-arrays
# train_x.reshape(-1,1)
# train_y.reshape(-1,1)
# test_x.reshape(-1,1)

# regressor.fit(train_x.reshape(-1,1), train_y.reshape(-1,1))

# #regressor.fit(train_x, train_y)
# #test_x = testData.iloc [:, 5] # ” : ” means it will select all rows
# y_pred = regressor.predict(test_x.reshape(-1,1))

#code to read the output CSV file and feed it to Spark SQl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
sc = SparkContext.getOrCreate();
sqlContext = SQLContext(sc)
snwFlPred = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('D:/Big data/csv/sample.csv')
snwFlPred.take(1)


import six
for i in snwFlPred.columns:
    if not( isinstance(snwFlPred.select(i).take(1)[0][0], six.string_types)):
        print( "Correlation to PRCP for ", i, snwFlPred.stat.corr('PRCP',i))

#code to assemble the vecors as feature and prcp
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols = ['TEMP','DEWP','WDSP','MAX','MIN'], outputCol = 'features')
vsnwFlPred = vectorAssembler.transform(snwFlPred)
vsnwFlPred = vsnwFlPred.select(['features', 'PRCP','LAT','LON','YEAR','MONTH','DAY','TEMP','LBL'])
vsnwFlPred.show(3)

#split the data into training and testing data
splits = vsnwFlPred.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]
# from pyspark.ml.regression import RandomForestRegressor
# from pyspark.ml.evaluation import RegressionEvaluator
# from sklearn.ensemble import RandomForestRegressor

# #Reading the data
# #data = spark.read.format("libsvm").load("D:/Big data/csv/sample.csv")
# #data= pd.read_csv('D:/Big data/csv/sample.csv')
# #pyspark.read.option("header","false").csv("D:/Big data/csv/sample.csv")

# data= pyspark.pandas.read_csv('D:/Big data/csv/sample.csv',headers= False)

# # just checking
# #print(data)
# #print(type(data))

# #(trainingData, testData) = data.randomSplit([0.7, 0.3])

# splits = data.to_spark().randomSplit([0.7, 0.3], seed=12)
# trainingData = splits[0].to_koalas()
# testData = splits[1].to_koalas()

# #splits = data.randomSplit([0.7, 0.3], 24)
# #trainingData = splits[0].to_koalas()
# #testData = splits[1].to_koalas()

# train_x= trainingData.iloc [:,5]
# train_y= trainingData.iloc [:,10]
# test_x= testData.iloc [:,5]

# #print(train_x)

# #print(train_y)

# regressor = RandomForestRegressor(n_estimators = 100, random_state = 0)
# #regressor.fit(train_x.to_frame(), train_y)

# train_x = train_x.to_numpy()
# train_y = train_y.to_numpy()
# test_x = test_x.to_numpy()

# #make them 2D-arrays
# train_x.reshape(-1,1)
# train_y.reshape(-1,1)
# test_x.reshape(-1,1)

# regressor.fit(train_x.reshape(-1,1), train_y.reshape(-1,1))

# #regressor.fit(train_x, train_y)
# #test_x = testData.iloc [:, 5] # ” : ” means it will select all rows
# y_pred = regressor.predict(test_x.reshape(-1,1))


#gradient boost regressor
from pyspark.ml.regression import GBTRegressor
gradient_boost = GBTRegressor(featuresCol = 'features', labelCol = 'PRCP', maxIter=100)
gradient_boost_model = gradient_boost.fit(train_df)
gradient_boost_predictions = gradient_boost_model.transform(test_df)
gradient_boost_predictions.select('prediction', 'PRCP', 'features').show(5)

#evaluating gradient boost regression test data
gradient_boost_evaluator = RegressionEvaluator(labelCol="PRCP", predictionCol="prediction", metricName="rmse")
rmse = gradient_boost_evaluator.evaluate(gradient_boost_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
gradient_boost_predictions.toPandas().to_csv('D:/Big data/csv/prediction.csv')
#Rigved Patil(862322104) and Shadhrush(862394040): Worked on the Data Pre-Processing, Machine Learning Models and Finding the correlation of different entities with each other
