from pyspark.sql import *
from pyspark.sql.types import StructType,StructField,StringType,ArrayType,MapType,IntegerType,TimestampType,DoubleType
from pyspark.sql import functions as F
import datetime
input1="D:\\Nagesh\\SPARK\\RetailStore_Data\\Data\\Inputs\\Sales_Landing\\SalesDump"
input2="D:\\Nagesh\\SPARK\\RetailStore_Data\\Data\\Inputs\\Sales_Landing\\SalesDump"
output1="D:\\Nagesh\\SPARK\\RetailStore_Data\\Data\\myOutput\\Valid\\ValidData"
output2="D:\\Nagesh\\SPARK\\RetailStore_Data\\Data\\myOutput\\hold\\holdData"


spark=SparkSession.builder.master("local[*]").appName("DailyDataIngestion").getOrCreate()
sc = spark.sparkContext



landingData=StructType([
    StructField("Sale_Id", StringType(), True),
    StructField("Product_id", StringType(), True),
    StructField("Quantity_Sold", IntegerType(), True),
    StructField("Vendor_Id", StringType(), True),
    StructField("Sale_Date", TimestampType(), True),
    StructField("Sale_amount", DoubleType(), True),
    StructField("Sale_currency", StringType(), True)
    ])
print(landingData)


holddataschema=StructType([
      StructField("Sale_Id", StringType(), True),
      StructField("Product_id", StringType(), True),
      StructField("Quantity_Sold", IntegerType(), True),
      StructField("Vendor_Id", StringType(), True),
      StructField("Sale_Date", TimestampType(), True),
      StructField("Sale_amount", DoubleType(), True),
      StructField("Sale_currency", StringType(), True)
    ])
print(holddataschema)

todaysDay=datetime.date.today()
curDate="_"+todaysDay.strftime('%d%m%Y')
print(curDate)

FreshData=spark.read.schema(landingData).option("delimiter","|")\
    .csv(input1 + curDate)
FreshData.show()
FreshData.createOrReplaceTempView("freshData")

prevday=datetime.date.today()-datetime.timedelta(days=1)
preDay ="_"+prevday.strftime('%d%m%Y')
print(preDay)

HoldData = spark.read.schema(holddataschema)\
    .option("header",True)\
    .option("delimiter","|")\
    .csv(input2 + preDay)
HoldData.createOrReplaceTempView("holddata")

refereshLandingData=spark.sql("select f.Sale_Id,f.Product_ID," +
      "if(f.Quantity_Sold is NUll,h.Quantity_Sold,f.Quantity_Sold) as Quantity_Sold ," +
      "if(f.Vendor_Id is NULL,h.Vendor_Id,f.Vendor_Id) as Vendor_Id," +
      "f.Sale_Date,f.Sale_Amount,f.Sale_Currency " +
      "from freshData f left join holddata h " +
      "on f.Sale_Id=h.Sale_Id")

refereshLandingData.show()



refereshLandingData.createOrReplaceTempView("validData")

idDataDf=refereshLandingData.filter(F.col("Quantity_Sold").isNotNull() & F.col("Vendor_Id").isNotNull())
idDataDf.createOrReplaceTempView("idData")

idDataDf.show()

releaseholdData=spark.sql("select v.Sale_Id from validData v " +
      "inner join holddata h " +
      "on v.Sale_Id=h.Sale_Id")

releaseholdData.show()

releaseholdData.createOrReplaceTempView("releaseholdData")
notRealeaseFromHoldData=spark.sql("select * from holddata " +
      "where Sale_Id NOT IN (select Sale_Id from releaseholdData)")
notRealeaseFromHoldData.show()

inValidData=refereshLandingData.filter(F.col("Quantity_Sold").isNull() | (F.col("Quantity_Sold") <= 0) | F.col("Vendor_Id").isNull()).union(notRealeaseFromHoldData)

inValidData.show()


