from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
sc = spark.sparkContext
'''

input="C:\\bigdata\\datasets\\2015.csv"
output="C:\\bigdata\\datasets\\output"
df=spark.read.format("csv").option("inferSchema","True").option("header","True").option("delimiter",",").load(input)
df.createOrReplaceTempView("tab")
res=spark.sql("select * from tab ")
res.printSchema()
res.show()
#res.write.mode(saveMode='overwrite').format('csv').option('header','true').option('delimiter',',').save(output)

data = ['nagesh','somesh','nagesh']
rdd = sc.parallelize(data)
rdd2 = rdd.flatMap(lambda x:x.split(","))\
    .map(lambda x:(x,1))\
    .reduceByKey(lambda x,y:x+y)
print(rdd2.collect())


def multipliers():
    return [lambda x:i*x for i in range(4)]

print[m(2) for m in multipliers()]
'''
input="C:\\Users\\mungi\\OneDrive\\Desktop\\python\\psysp"
input2="C:\\Users\\mungi\\OneDrive\\Desktop\\python\\na\\part-00000-1c2f8c66-3a45-4b18-8319-b31785fd4c2d-c000.json"
output="C:\\Users\\mungi\\OneDrive\\Desktop\\python\\na"
df=spark.read.format("csv").option("inferSchema","True").option("header","True").option("delimiter",",").load(input)
df2=spark.read.format("text").load(input2)
df.createOrReplaceTempView("tab1")
df2.createOrReplaceTempView("tab2")
#df3=spark.sql("select * from tab1") + spark.sql("select * from tab2")

df3=df.coalesce(df2)
df3.show()

