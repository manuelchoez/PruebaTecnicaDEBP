import sqlite3
import pandas as pd
from src.config.db import get_connection

def asignar_partidos_clasificados() -> pd.DataFrame:
    conn = get_connection()
    
    df_clasificados = pd.read_sql("SELECT posicion, equipo, grupo, puntos FROM Clasificados_Qatar", conn)
    df_partidos = pd.read_sql("SELECT partido, fecha, hora, sede FROM Clasificados", conn)
    conn.close()

    df_clasificados["match_key"] = df_clasificados["posicion"].astype(str) + "º del grupo " + df_clasificados["grupo"].str[-1]
  
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
    df_final = pd.merge(df_clasificados, df_partidos_expandidos, on="match_key", how="left")
    df_final = df_final[["equipo", "grupo", "posicion", "partido", "fecha", "hora", "estadio"]].sort_values(by=["partido", "posicion"])

    return df_final
