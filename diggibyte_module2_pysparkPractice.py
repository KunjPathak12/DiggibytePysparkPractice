pip install pyspark

from google.colab import drive
drive.mount('/content/drive')

# initiating a spark sesssion
from pyspark.sql import SparkSession as SS
from pyspark.sql import *
spark = SS.builder.appName("sparkModule_2_Diggibyte_Internship").config("spark.some.config.option","some-value").getOrCreate()
df = spark.read.option("header","True").csv("/content/drive/MyDrive/spotify_millsongdata.csv",inferSchema = True)
df.show(5)
df.schema

# rdd using parallelize because data is in list format
columns = ["language","userCount"]
data = [("Java",20000),("Python", 15000),("Scala",10000)]
rdd = spark.sparkContext.parallelize(data)
rdd.collect()
data1 = [Row(name="James,Smith",lang=["Java","Scala","C++"],state="CA"), 
    Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
    Row(name="Robert,Williams",lang=["CSharp","VB"],state="NV")] 
# rdd with row
rdd1 = spark.sparkContext.parallelize(data1)
rdd.collect()

output = rdd1.collect()
# for i in output:
#   print(i.name+str(i.lang))

# df using rdd
df = rdd.toDF(columns)
df.show()
df.printSchema()
df_98 = spark.createDataFrame(rdd, schema = columns)
df_98.show()

# rdd using txt file
rddText = spark.sparkContext.textFile("/content/drive/MyDrive/week1.txt")
rddText.collect()

# creating dataframes from list
df1 = spark.createDataFrame(data = data,schema =columns)
df1.show()
df1.schema

df_csv = spark.read.option("header", "true").csv("/content/sample_data/california_housing_train.csv", inferSchema = True)
df_csv.show(5)
df_csv.describe().show()

df_json = spark.read.option("mode","DROPMALFORMED").json("/content/sample_data/anscombe.json")
df_json1 = spark.read.option("mode","PERMISSIVE").json("/content/sample_data/anscombe.json")
df_json1.show(5)
df_json.show(5)
df_json.describe().show()
df_json1.na.drop(how = 'any',subset=['_corrupt_record']).show(5)
df_json1.na.drop(how = 'all').show(5)
df_json1.na.drop(how = 'any',thresh = 2).count()

dfr = spark.read.option("mode","PERMISSIVE").option("multiline","true").json("/content/drive/MyDrive/kaffee_reviews.json")
dfn = spark.read.option("mode","DROPMALFORMED").json("/content/drive/MyDrive/kaffee_reviews.json")
dfr.show(3)
dfn.show(3)
df_json.na.drop(how = 'any',thresh=2).count()

df.na.fill("missingInfo").show(5)
# df.select("rating").count()
df.count()

from pyspark.sql.functions import *
from pyspark.sql.types import *
# df = spark.read.option("mode","PERMISSIVE").option("multiline","true").json("/content/drive/MyDrive/data.json")
schema = StructType([StructField('geoLat', StringType(), False), StructField('geoLng', StringType(), False),\
                     StructField('lineColorHex', StringType(), False), StructField('lineName',StringType(), True), \
                     StructField('lineNameEng', StringType(), False), StructField('lineServiceName', StringType(), True), \
                     StructField('name', StringType(), True), StructField('nameEng', StringType(), False), StructField('stationId', StringType(), False)])
df = spark.read.option("mode","PERMISSIVE").option("multiline","true").schema(schema).json("/content/drive/MyDrive/data.json")
df.show(1)
df1 = df.drop("lineName","name")
df1.show(1)
df.printSchema()
df1.count()

from pyspark.sql.functions import col
dfAdd = df1.withColumn("lat+lng",col("geoLat")+ col("geoLng"))
# to Add extra column in the df
dataF = [0,1,2,3,4,5,6,7,8,9]
dfAdd = dfAdd.withColumn("Extra",lit(monotonically_increasing_id()))
dfAdd.withColumn("Extra",dfAdd.Extra.cast("integer")).printSchema()
dfAdd.withColumn("Extra", array([lit(i) for i in dataF])).show(1)
# dfAdd.show(5)
# df.na.drop().count()
# dfAdd.printSchema()

df1.na.fill("MissingVAlues")

dftrunc = df1.withColumnRenamed("LineNameEng","lineName")\
          .withColumnRenamed("lineServiceName", "service")\
          .withColumnRenamed("nameEng","name")\
          .withColumnRenamed("geoLat","lat")\
          .withColumnRenamed("geoLng","lng")

dftrunc.show(1)
dftrunc.describe()

dfmodified = dftrunc.withColumn("lat",col("lat").cast("double"))\
          .withColumn("lng",col("lng").cast("double"))
dfmodified.show(1)
dfmodified.printSchema()

from pyspark.sql.functions import monotonically_increasing_id, row_number
dfFinal = dfmodified.withColumn("sr_no", monotonically_increasing_id())\
          .withColumnRenamed("name", "stationName")
dfFinal.show(3)

from pyspark.sql.window import Window
# auto increase with value = 1
dfFinal = dfFinal.withColumn("sr_no",row_number().over(Window.orderBy(monotonically_increasing_id())))
dfFinal.show(3)
dfFinal.printSchema()

dfAirport = dfFinal.filter(dfFinal.service == 'AIRPORTLINK')
dfAirport.show();
dfAirport.count()

dfAirport.select("lat","lng","lineName","stationName").show()

dfAirport.select(dfAirport.columns[3:]).show()

dfAirport.collect()[-1]

dfAirport.na.drop()
dfAirport.withColumn("stationName",when(dfAirport.stationName == "Suvamabhumi","ShivaBhoomi" )).show(2)
dfAirport.withColumn("stationName",regexp_replace("stationName","Makkasan","Mahisagar")).show()
# dfAirport.withColumn()

dfAirport.withColumn("stationId",when(dfAirport.stationId == "A1",lit("A-01")).otherwise(col("stationId"))).show()

dfAirport.select(dfAirport.lat.alias("latitude"),\
          dfAirport.lng.alias("longitude")).show()

dfFinal.sort(dfFinal.sr_no.desc()).show(5)

dfFinal.show(3)
dfFinal.printSchema()

dfFinal.filter(dfFinal.sr_no.between(51,60)).show()
# dfFinal.select(dfFinal.service).show()
dfFinal.where(dfFinal.service == "BTS").count()
dfFinal.select("service").where(dfFinal.service == "BTS").show(5)

dfFinal.filter(dfFinal.service.contains("BTS")).show(10)

dfFinal.select(dfFinal.stationName,when(dfFinal.service == "BTS","TS").alias("newService")).show(truncate = False)

# create column from existing
dfFinal.withColumn("details",col("lineName")).show(5)

dfFinal.select(dfFinal.service.like("B%").alias("pattern")).show(10)

dfFinal.show(5)

dfFinal.groupBy("service").count().show()

dfFinal.groupBy("lineName").min("lng").show(10)

dfFinal.groupBy("lineName").max("sr_no").alias("maxDistance").show(5)

dfFinal.groupBy("lineName").mean("sr_no").show(5)

dfFinal.groupBy("lineName").pivot("service").mean("lng").show(5)

dfDo = dfFinal.drop("service", "lineName")
dfD = dfDo.filter(dfDo.sr_no.contains(10))
dfD.show()

myList = dfDo.columns
def changeName(myList, dfDo):
  for i in range(len(myList)):
    if myList[i] == "lat":
      myList[i] = "latitude"
    if myList[i] == "lng":
      myList[i] = "longitude"
    if myList[i] == "lineColorHex":
      myList[i] = "lColor"
    else:
      pass
  dfDo = dfDo.toDF(*myList)
  return dfDo
  
  # print(myList)
d = changeName(myList, dfDo)
d.show()

dfFinal.join(dfD, dfFinal.sr_no == dfD.sr_no,"inner").show(3)

dfFinal.join(dfD, dfFinal.sr_no == dfD.sr_no,"right").show()

newDF=dfFinal.withColumn("service",when(dfFinal.service == "BTS","Agrahara").otherwise(dfFinal.service))

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].lower() + x[1:len(x)].upper() + " "
    return resStr 
str = "john karsinski"
print(convertCase(str))

from pyspark.sql.functions import *

myUdf = udf(lambda string: convertCase(string))

udfDF=dfFinal.select(dfFinal.sr_no,myUdf(dfFinal.stationName).alias("newName")).show(5)
dfFinal.withColumn("udfName",myUdf(col("stationName"))).show(5)
# dfFinal.select(dfFinal.stationId,convertCase(dfFinal.stationName).alias("anotherOne")).show(5)

newDF.union(dfFinal).distinct().show(5)

dfMapped = dfFinal.where(dfFinal.sr_no<=10)
dfMapped.show()

myRDD = dfMapped.rdd
myRDD.collect()

def myFunction(string):
  if string == "BTS":
    string = "BMTC"
  return string
newRDD = myRDD.map(lambda x:(x[2],x[3],myFunction(x[4]),x[5],x[6],x[7]))

columns = list(("lcolor","lname","service","sname","s''Id","sr"))
newDF = spark.createDataFrame(data = newRDD,schema= columns)
newDF.show()
newRDD.collect()

rdd2=myRDD.flatMap(lambda x:(x[2],x[3],myFunction(x[4]),x[5],x[6],x[7]))
# for i in rdd2.collect():
#   print(i)
rdd2.collect()[2:6]

def myFunctionh(newDF):
   print (newDF.sname, newDF.lname)
newDF.foreach(myFunctionh)

accmc = spark.sparkContext.accumulator(0)
def countFn(x):
  global accmc
  for i in range(x[5]):
    accmc+=1
  return accmc
newRDD.foreach(countFn)
print(accmc.value)

accm = spark.sparkContext.accumulator(0)
newRDD.foreach(lambda x: accm.add(x[5]))
print(accm.value)
