import sqlite3
import pandas as pd
from src.config.db import get_connection

def asignar_partidos_clasificados() -> pd.DataFrame:
    conn = get_connection()

    # Cargamos los equipos clasificados
    df_clasificados = pd.read_sql("SELECT posicion, equipo, grupo, puntos FROM Clasificados_Qatar", conn)

    # Cargamos los partidos (cruces de octavos)
    df_partidos = pd.read_sql("SELECT partido, fecha, hora, sede FROM Clasificados", conn)

    conn.close()

    # Creamos las claves de emparejamiento: "1º del grupo A"
    df_clasificados["match_key"] = df_clasificados["posicion"].astype(str) + "º del grupo " + df_clasificados["grupo"].str[-1]

    # Expandimos los partidos en dos filas: local y visitante
    partidos_expandidos = []

    for _, row in df_partidos.iterrows():
        equipos_texto = row["partido"]
        partes = equipos_texto.split(" vs. ")
        if len(partes) == 2:
            for lado in partes:
                partidos_expandidos.append({
                    "match_key": lado.strip(),
                    "partido": equipos_texto,
                    "fecha": row["fecha"],
                    "hora": row["hora"],
                    "estadio": row["sede"]
                })

    df_partidos_expandidos = pd.DataFrame(partidos_expandidos)

    # Hacemos el merge para asignar equipos a partidos
    df_final = pd.merge(df_clasificados, df_partidos_expandidos, on="match_key", how="left")

    # Ordenamos y seleccionamos columnas finales
    df_final = df_final[["equipo", "grupo", "posicion", "partido", "fecha", "hora", "estadio"]].sort_values(by=["partido", "posicion"])

    return df_final
