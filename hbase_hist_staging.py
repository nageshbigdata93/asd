from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("testing").enableHiveSupport().getOrCreate()
sc = spark.sparkContext

print("reading data from hbase")
Address_HB=spark.read.format("org.apache.phoenix.spark").option("table","Address_HB").option("zkUrl","localhost:2181").load()
Card_HB=spark.read.format("org.apache.phoenix.spark").option("table","Card_HB").option("zkUrl","localhost:2181").load()
Card_Type_HB=spark.read.format("org.apache.phoenix.spark").option("table","Card_Type_HB").option("zkUrl","localhost:2181").load()
CC_Debit_HB=spark.read.format("org.apache.phoenix.spark").option("table","CC_Debit_HB").option("zkUrl","localhost:2181").load()
CC_Paid_HB=spark.read.format("org.apache.phoenix.spark").option("table","CC_Paid_HB").option("zkUrl","localhost:2181").load()
City_HB=spark.read.format("org.apache.phoenix.spark").option("table","City_HB").option("zkUrl","localhost:2181").load()
Country_HB=spark.read.format("org.apache.phoenix.spark").option("table","Country_HB").option("zkUrl","localhost:2181").load()
Tx_Type_HB=spark.read.format("org.apache.phoenix.spark").option("table","Tx_Type_HB").option("zkUrl","localhost:2181").load()

print("creating temp view")
Address_HB.createTempView("Address_HB")
Card_HB.createTempView("card_hb")
Card_Type_HB.createTempView("Card_Type_HB")
CC_Debit_HB.createTempView("CC_Debit_HB")
CC_Paid_HB.createTempView("CC_Paid_HB")
City_HB.createTempView("City_HB")
Country_HB.createTempView("Country_HB")
Tx_Type_HB.createTempView("Tx_Type_HB")

print("creting table card_details_stg")
card_details_stg=spark.sql("""select c.c_number as cardNumber,c.c_type as cardType,c.full_name as full_name,
c.mob as contactnumber,c.email as emailid,ad.street as address,ct.ct_name as city,cn.cn_name as country,
c.issue_date as issuedate,c.update_date as update_date,c.billing_date as billingdate,c.c_limit as cardlimit,
 c.act_flag as active_flag from card_hb as c left join Address_HB as ad on c.add_id=ad.add_id
 left join City_HB as ct on ad.ct_id=ct.ct_id left join Country_HB as cn on ct.cn_id=cn.cn_id""")

print("creting table credit_details")

card_details=spark.sql("""select c.c_number as cardNumber,c.c_type as cardType,c.full_name as full_name,
c.mob as contactnumber,c.email as emailid,ad.street as address,ct.ct_name as city,cn.cn_name as country,
c.issue_date as issuedate,c.update_date as update_date,c.billing_date as billingdate,c.c_limit as cardlimit,
 c.act_flag as active_flag from card_hb as c left join Address_HB as ad on c.add_id=ad.add_id
 left join City_HB as ct on ad.ct_id=ct.ct_id left join Country_HB as cn on ct.cn_id=cn.cn_id""")


card_details.createOrReplaceTempView("card_detail")
card_details_stg.createOrReplaceTempView("card_detail_stg")

print("Card details de-duplication and compaction started")
cd_dff=spark.sql("select * from card_detail")
cd_dfs=spark.sql("select * from card_detail_stg")
un=cd_dff.union(cd_dfs)

res=un.withColumn("new_upd",F.when(un.update_date.isNull(),F.to_timestamp(F.lit("1970-01-01 00:00:00"),format="WWW-MM-dd HH:mm:SS"))
                  .otherwise(un.update_date)).drop("update_date")
res1=res.withColumn("rn",F.row_number().over(Window.partitionBy("cardNumber").orderBy(F.desc("new_upd"))))
res2=res1.filter(res1.rn==1).drop("rn")


credit_details_stg=spark.sql("select * from CC_Paid_HB ").dropDuplicates()
credit_details=spark.sql("select * from CC_Paid_HB ").dropDuplicates()
print("creting table debit_details_stg")
debit_details_stg=spark.sql("select * from CC_Debit_HB ").dropDuplicates()
debit_details=spark.sql("select * from CC_Debit_HB ").dropDuplicates()


card_details_stg.write.mode("overwrite").format("hive").saveAsTable("card_details_stg")
credit_details_stg.write.mode("overwrite").format("hive").saveAsTable("credit_details_stg")
debit_details_stg.write.mode("overwrite").format("hive").saveAsTable("debit_details_stg")
card_details.write.mode("overwrite").format("hive").saveAsTable("card_details")
credit_details.write.mode("overwrite").format("hive").saveAsTable("credit_details")
debit_details.write.mode("overwrite").format("hive").saveAsTable("debit_details")

a=spark.sql("select * from card_details_stg")
b="delta.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(b)
df.createOrReplaceTempView("delta")




