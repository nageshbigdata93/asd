from pyspark.sql import *

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
sc = spark.sparkContext

Country_HB =spark.read.format("jdbc").option("url","jdbc:postgresql://nagesh.c6msimclgipx.ap-south-1.rds.amazonaws.com:5432/prod").option("user","puser").option("password","ppassword").option("driver","org.postgresql.Driver").option("dbtable","country").load()
Address =spark.read.format("jdbc").option("url","jdbc:postgresql://nagesh.c6msimclgipx.ap-south-1.rds.amazonaws.com:5432/prod").option("user","puser").option("password","ppassword").option("driver","org.postgresql.Driver").option("dbtable","address").load()
Card =spark.read.format("jdbc").option("url","jdbc:postgresql://nagesh.c6msimclgipx.ap-south-1.rds.amazonaws.com:5432/prod").option("user","puser").option("password","ppassword").option("driver","org.postgresql.Driver").option("dbtable","card").load()
Card_Type =spark.read.format("jdbc").option("url","jdbc:postgresql://nagesh.c6msimclgipx.ap-south-1.rds.amazonaws.com:5432/prod").option("user","puser").option("password","ppassword").option("driver","org.postgresql.Driver").option("dbtable","card_type").load()
CC_Debit =spark.read.format("jdbc").option("url","jdbc:postgresql://nagesh.c6msimclgipx.ap-south-1.rds.amazonaws.com:5432/prod").option("user","puser").option("password","ppassword").option("driver","org.postgresql.Driver").option("dbtable","cc_debit").load()
CC_Paid =spark.read.format("jdbc").option("url","jdbc:postgresql://nagesh.c6msimclgipx.ap-south-1.rds.amazonaws.com:5432/prod").option("user","puser").option("password","ppassword").option("driver","org.postgresql.Driver").option("dbtable","cc_paid").load()
City =spark.read.format("jdbc").option("url","jdbc:postgresql://nagesh.c6msimclgipx.ap-south-1.rds.amazonaws.com:5432/prod").option("user","puser").option("password","ppassword").option("driver","org.postgresql.Driver").option("dbtable","city").load()
Tx_Type =spark.read.format("jdbc").option("url","jdbc:postgresql://nagesh.c6msimclgipx.ap-south-1.rds.amazonaws.com:5432/prod").option("user","puser").option("password","ppassword").option("driver","org.postgresql.Driver").option("dbtable","tx_type").load()

Country_HB.write.format("org.apache.phoenix.spark").option("table","Country_HB").option("zkUrl","localhost:2181").mode("overwrite").save()
Address.write.format("org.apache.phoenix.spark").option("table","Address_HB").option("zkUrl","localhost:2181").mode("overwrite").save()
Card.write.format("org.apache.phoenix.spark").option("table","Card_HB").option("zkUrl","localhost:2181").mode("overwrite").save()
Card_Type.write.format("org.apache.phoenix.spark").option("table","Card_Type_HB").option("zkUrl","localhost:2181").mode("overwrite").save()
CC_Debit.write.format("org.apache.phoenix.spark").option("table","CC_Debit_HB").option("zkUrl","localhost:2181").mode("overwrite").save()
CC_Paid.write.format("org.apache.phoenix.spark").option("table","CC_Paid_HB").option("zkUrl","localhost:2181").mode("overwrite").save()
City.write.format("org.apache.phoenix.spark").option("table","City_HB").option("zkUrl","localhost:2181").mode("overwrite").save()
Tx_Type.write.format("org.apache.phoenix.spark").option("table","Tx_Type_HB").option("zkUrl","localhost:2181").mode("overwrite").save()
