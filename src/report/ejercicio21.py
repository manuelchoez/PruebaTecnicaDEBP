import sqlite3
import pandas as pd
from src.config.db import get_connection

def calcular_clasificados() -> pd.DataFrame:
    conn = get_connection()    
        
    query = "SELECT equipo, grupo, puntos FROM Resultados_Qatar"
    df = pd.read_sql(query, conn)
    
    df["puntos"] = df["puntos"].astype(int)    

    clasificados = df.sort_values(["grupo", "puntos"], ascending=[True, False])
    top2_por_grupo = clasificados.groupby("grupo").head(2).reset_index(drop=True)

    top2_por_grupo["posicion"] = top2_por_grupo.groupby("grupo").cumcount() + 1
    
    top2_por_grupo = top2_por_grupo[["posicion", "equipo", "grupo", "puntos"]]
    
    top2_por_grupo.to_sql("Clasificados_Qatar", conn, if_exists="replace", index=False)
    
    conn.close()

    return top2_por_grupo