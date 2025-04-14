import sqlite3
import pandas as pd
from src.config.db import get_connection

def calcular_estadisticas_equipos() -> pd.DataFrame:
    conn = get_connection()
    
    
    partidos = pd.read_sql("SELECT partido,fechas,grupo,resultado FROM Partidos", conn)
    conn.close()

    
    partidos[['Equipo1', 'Equipo2']] = partidos['partido'].str.split(' vs. ', expand=True)
    partidos[['Goles1', 'Goles2']] = partidos['resultado'].str.split(',', expand=True)
    partidos['Goles1'] = partidos['Goles1'].astype(int)
    partidos['Goles2'] = partidos['Goles2'].astype(int)

    resultados = []

    for _, row in partidos.iterrows():
        grupo = row['grupo']
        equipo1 = row['Equipo1']
        equipo2 = row['Equipo2']
        goles1 = row['Goles1']
        goles2 = row['Goles2']

        if goles1 > goles2:
            puntos1, puntos2 = 3, 0
        elif goles1 < goles2:
            puntos1, puntos2 = 0, 3
        else:
            puntos1, puntos2 = 1, 1

        resultados.append({'Equipo': equipo1, 'Grupo': grupo, 'Puntos': puntos1, 'GolDif': goles1 - goles2})
        resultados.append({'Equipo': equipo2, 'Grupo': grupo, 'Puntos': puntos2, 'GolDif': goles2 - goles1})

    df_resultados = pd.DataFrame(resultados)
    
    resumen = df_resultados.groupby(['Equipo', 'Grupo']).agg(
        Total_Puntos=('Puntos', 'sum'),
        Gol_Diferencia=('GolDif', 'sum')
    ).reset_index()

    return resumen
