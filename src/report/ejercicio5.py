from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, udf, min as spark_min
from pyspark.sql.types import DoubleType
import math
import os


os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

spark = SparkSession.builder.appName("CentrosEducacionMadrid").getOrCreate()


input_path = "D:/ManuelChoez/2025/PruebaTecnicaDEBP/src/content/CENTROS_EDUCACION_MADRID.json"
df = spark.read.option("multiline", "true").json(input_path)

avg_coord = df.groupBy("centro_titularidad").agg(
    avg("direccion_coor_x").alias("avg_x"),
    avg("direccion_coor_y").alias("avg_y")
)

df_con_avg = df.join(avg_coord, on="centro_titularidad")

def calcular_distancia(x, y, avg_x, avg_y):
    return math.sqrt((x - avg_x)**2 + (y - avg_y)**2)

dist_udf = udf(calcular_distancia, DoubleType())

df_with_dist = df_con_avg.withColumn(
    "distancia", 
    dist_udf(col("direccion_coor_x"), col("direccion_coor_y"), col("avg_x"), col("avg_y"))
)


from pyspark.sql.window import Window
import pyspark.sql.functions as F

window = Window.partitionBy("centro_titularidad").orderBy("distancia")
df_result = df_with_dist.withColumn("rank", F.row_number().over(window)).filter(col("rank") == 1).drop("rank")

df_result.write.mode("overwrite").parquet("/tmp/resultado_centro_reunion.parquet")

df_result.write.mode("overwrite").json("/tmp/resultado_centro_reunion.json")

df_result.write.mode("overwrite").option("header", True).option("delimiter", "|").csv("/tmp/resultado_centro_reunion.csv")

spark.stop()
