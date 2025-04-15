from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, expr, struct, collect_list
from pyspark.sql.window import Window
import os


os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"



spark = SparkSession.builder.appName("Ranking Jugadores").getOrCreate()


jugador = [("1", "Cristiano Ronaldo", "32", "115", "184"),("2", "Ali Daei", "24", "109", "148"),
("3", "Mokhtar Dahari", "28", "89", "142"),("4", "Ferenc Puskás", "20", "84", "89"),
("5", "Lionel Messi", "5", "81", "158"),("6", "Sunil Chhetri", "21", "80", "125"),
("7", "Ali Mabkhout", "13", "79", "104"),("8", "Godfrey Chitalu", "39", "79", "111"),
("9", "Hussein Saeed", "23", "78", "137"),("10", "Pelé", "8", "77", "92")]

pais = [("1", "Alemania"),("2", "Alemania Federal"),("3", "Alemania Oriental"),("4", "Arabia Saudita"),
("5", "Argentina"),("6", "Bélgica"),("7", "Bosnia y Herzegovina"),("8", "Brasil"),("9", "Camerún"),
("10", "Corea del Sur"),("11", "Costa de Marfil"),("12", "Egipto"),("13", "Emiratos Arabes Unidos"),
("14", "España"),("15", "Estados Unidos"),("16", "Federación Malaya/Malasia"),("17", "Guatemala"),
("18", "Honduras"),("19", "Hungría"),("20", "Hungría/España"),("21", "India"),("22", "Indonesia"),
("23", "Irak"),("24", "Irán"),("25", "Irlanda"),("26", "Japón"),("27", "Kuwait"),("28", "Malasia"),
("29", "Malawi"),("30", "Maldivas"),("31", "Polonia"),("32", "Portugal"),("33", "República Checa"),
("34", "Singapur"),("35", "Suecia"),("36", "Tailandia"),("37", "Trinidad y Tobago"),("38", "Uruguay"),
("39", "Zambia")]


columns_jugador = ["id", "nombre", "id_pais", "goles", "partidos"]
df_jugador = spark.createDataFrame(jugador, columns_jugador)

columns_pais = ["id_pais", "nombre_pais"]
df_pais = spark.createDataFrame(pais, columns_pais)


df_full = df_jugador.join(df_pais, on="id_pais")


df_full = df_full.withColumn("ranking_anterior", expr("int(rand() * 30 + 1)"))
df_full = df_full.withColumn("ranking_actual", expr("int(rand() * 30 + 1)"))

df_estructurado = df_full.select(
    col("id_pais").alias("codigo_pais"),
    col("nombre_pais"),
    struct(
        col("nombre").alias("nombre_jugador"),
        col("ranking_anterior"),
        col("ranking_actual")
    ).alias("jugador")
)


df_final = df_estructurado.groupBy("codigo_pais", "nombre_pais") \
    .agg(collect_list("jugador").alias("jugadores"))


os.makedirs("data/output", exist_ok=True)
df_final.write.mode("overwrite").json("D:/ManuelChoez/2025/PruebaTecnicaDEBP/data/output/ranking_anidado.json")