from pyspark.sql import SparkSession

#ASN
spark = SparkSession.builder.appName('ASN').getOrCreate()
dataset = spark.read.csv('ASN_arin_db_2-20-2019_parsed.csv', header=True, inferSchema=True)
dataset.show(5)
dataset.write \
  .mode("append") \
  .option("path", "/user/hive/warehouse/<db_name>.db/arin_asn") \
  .saveAsTable("<db_name>.arin_asn")
  
#Networks
spark = SparkSession.builder.appName('Networks').getOrCreate()
dataset = spark.read.csv('Networks_arin_db_2-20-2019_parsed.csv', header=True, inferSchema=True)
dataset.show(5)
dataset.write \
  .mode("append") \
  .option("path", "/user/hive/warehouse/<db_name>.db/arin_network") \
  .saveAsTable("<db_name>.arin_network")  
  
#Orgs
spark = SparkSession.builder.appName('ORGS').getOrCreate()
dataset = spark.read.csv('ORGS_arin_db_2-20-2019_parsed.csv', header=True, inferSchema=True)
dataset.show(5)
dataset.write \
  .mode("append") \
  .option("path", "/user/hive/warehouse/<db_name>.db/arin_organization") \
  .saveAsTable("<db_name>.arin_organization")  
  
#Pocs
spark = SparkSession.builder.appName('POCS').getOrCreate()
dataset = spark.read.csv('POCS_arin_db_2-20-2019_parsed.csv', header=True, inferSchema=True)
dataset.show(5)
dataset.write \
  .mode("append") \
  .option("path", "/user/hive/warehouse/<db_name>.db/arin_pocs") \
  .saveAsTable("<db_name>.arin_pocs")
  
  
