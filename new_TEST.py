from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
sc = spark.sparkContext


LIST="C:\\Users\\mungi\\OneDrive\\Desktop\\python\\Clair.TXT" # Location path of file
rdd1=sc.textFile(LIST)
rdd2=rdd1.flatMap(lambda x:x.split())
rdd3=rdd2.map(lambda x:(x,1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y).collect()

print(rdd4)



