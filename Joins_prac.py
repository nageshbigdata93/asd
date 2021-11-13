from numpy.ma import count

from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
sc = spark.sparkContext
'''
ip="C:\\Users\mungi\OneDrive\Desktop\python\emp_spa"
ipp="C:\\Users\mungi\OneDrive\Desktop\python\Emp_spa1"
df=spark.read.format("csv").option("inferSchema","True").option("header","True").option("delimeter",",").load(ip)
df.show()
#df.printSchema()
df1=spark.read.format("csv").option("inferSchema","True").option("header","True").option("delimeter",",").load(ipp)
df1.show()


res=df.join(df1,on="city_id",how="left").filter("name=akash").select("id","name")
res.show()
#r=res.na.fill({"name": "default","sal": 0,"CITY":"Unknown"}).show() replace null with similar data types
#r=res.na.drop() drop null values

#r=res.na.fill({"sal":res.agg(sum("sal"))})
#r.show()
'''

ip="C:\\Users\mungi\OneDrive\Desktop\python\emp_spa"
df=spark.read.format("csv").option("inferSchema","true").option("header","True").option("delimiter",",").load(ip)
df.printSchema()
df.show()
res=df.groupBy("name").agg(sum("sal").alias("sum_salary")).sort(desc("sum_salary")).limit(2)
res.show()

'''
res=df.select("id","name","sal", F.dense_rank().over(Window.partitionBy().orderBy(df['sal'])).alias("dn")) #.filter("row_num=1")
res.show()
#res=df.dropDuplicates().show()



a=["nagesh","nagesh","nagesh","nagesh","somesh","akash","manish","manish"]
b=sc.parallelize(a)
c=b.flatMap(lambda x:x.split(","))
d=c.map(lambda x:(x,1))
e=d.reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]> 1).collect()
print(e)


df=spark.read.csv("C:\\bigdata\\datasets\\athlete_events.csv",inferSchema="true",sep=",",header="true",nullValue="false")
df.printSchema()
df.show(truncate=True)
res=df.select[("*",F.col("Year").rlike('^[0-9]*$').alias("su"))].show(truncate=True)
'''