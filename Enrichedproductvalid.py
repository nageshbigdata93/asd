from pyspark.sql import *
from pyspark.sql import functions as F

import datetime
from pyspark.sql import *
from pyspark.sql.types import StructType,StructField,StringType,ArrayType,MapType,IntegerType,TimestampType,DoubleType
from pyspark.sql import functions as F

spark=SparkSession.builder.master("local[*]").appName("EnrichProductReference").getOrCreate()
sc = spark.sparkContext
#Reading landing data from config file
validdata="D:\\Nagesh\\SPARK\\RetailStore_Data\\Data\\myOutput\\Valid\\ValidData"
products="D:\\Nagesh\\SPARK\\RetailStore_Data\\Data\\Inputs\\Products"
SaleAmountEnriched="D:\\Nagesh\\SPARK\\RetailStore_Data\\Data\\myOutput\\Enriched\\SaleAmountEnrichment\\SaleAmountEnriched"
todaysDay=datetime.date.today()


curDateSuffix="_"+todaysDay.strftime('%d%m%Y')


prevday=datetime.date.today()-datetime.timedelta(days=1)
preDay ="_"+prevday.strftime('%d%m%Y')



#preDay="_"+todaysDay.minusDays(1).format(DateTimeFormatter.ofPattern("ddMMyyyy"))


idDataDf=spark.read.option("delimiter","|").option("header",True)\
    .csv(validdata +curDateSuffix)

idDataDf.createOrReplaceTempView("idData")


productReferenceSchema=spark.read.option("header",True).option("delimiter","|")\
    .csv(products)

productReferenceSchema.createOrReplaceTempView("productSchema")

productPriceReference=spark.sql("select a.Sale_Id,a.Product_Id,b.Product_Name," +
  "a.Quantity_Sold,a.Vendor_Id,a.Sale_Date," +
  "b.Product_Price * a.Quantity_Sold as Sale_Amount,a.Sale_Currency" +
  " from idData a inner join productSchema b " +
  "on a.Product_Id=b.Product_Id")

#    using dataframe functions
'''
   productPriceReference=idDataDf.join(productReferenceSchema,Seq("Product_Id"),"inner")
  .withColumn("Total_SaleAmount",col("Product_Price")*col("Quantity_Sold"))
  .select("Sale_Id","Product_Id","Product_Name","Quantity_Sold","Vendor_Id","Sale_Date"
    ,"Total_SaleAmount","Sale_Currency")
'''
#    productPriceReference.show()

productPriceReference.write.option("header","true").option("delimiter","|").mode("overwrite")\
    .csv(SaleAmountEnriched + curDateSuffix)

