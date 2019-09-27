#Read SQL
var dds = spark.sql("select a,b,c from db.table")

var data =  spark.sql("select * from db.table")

#Read CSV

var data = spark.read.format("csv").
    schema(customSchema).
    option("header", "true").
    load("/user/---.csv")
    
#Read Parquet
var data = spark.read.parquet("/user/(parquet file)")

var data2 = spark.read.parquet("hdfs:///user/data/file.parquet")

#Write CSV

dataframe.repartition(1).write.format("com.databricks.spark.csv").save("/user/dataframe.csv")

dataframe.orderBy(asc("var1"),asc("var2")).repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save("/user/dataframe.csv")

#Write Parquet

df.filter($"a" === 1).write.parquet("hdfs:///user/file.parquet")

#Read Multiple Files

spark.read.parquet("hdfs:///user/file.parquet").join(spark.read.parquet("hdfs:///user/file.parquet"), Seq("key")).write.parquet("hdfs:///user/file.parquet")
spark.read.parquet("hdfs:///user/file.parquet").join(spark.read.parquet("hdfs:///user/file.parquet"), Seq("key")).write.parquet("hdfs:///user/file.parquet")

spark.read.parquet("hdfs:///user/FILE.parquet").withColumn("len", tamanoUDF($"nombre_sunarp")).withColumn("len", explode($"len")).write.partitionBy("len").parquet("hdfs:///user/FILE.parquet")

#Select distinct

var df = data1.select("a","b","c","d","e","f").distinct()

#Rename Header

datos = df.withColumnRenamed("old_value", "new_value")

#Diference Between Dates

var df = df1.withColumn("diferencia_fecha", datediff(to_date($"fecha1", "dd-MM-yyyy") , to_date($"fecha2", "dd-MM-yyyy"))/30)

var final_df = datos.withColumn("anno", datediff(to_date($"fecha1", "dd-MM-yyyy") , to_date($"fecha2", "dd-MM-yyyy"))/30)

var datos = datos.withColumn("anno", ((unix_timestamp($"fecha1", "dd-MM-yyyy") - unix_timestamp($"fecha2", "dd-MM-yyyy"))/3600.0)/24/365/1000)

//MAX
data.groupBy("a","b","c","d").distinct.agg(max("fecha")).show()

Calculo Suma

val df_data = datos.
groupBy("id","name").
agg(sum("sueldo"))

JOIN DF
var dataf = df1.join(df2,df("id")===dds("id"),"left")


    
    
