from src.report.ejercicio1 import calcular_estadisticas_equipos
from src.report.ejercicio21 import calcular_clasificados
from src.report.ejercicio22 import asignar_partidos_clasificados
from src.load.load_to_db import load_to_db  

load_to_db()
df = calcular_estadisticas_equipos()
df21 = calcular_clasificados()
df22 = asignar_partidos_clasificados()
print(df)
print(df21)
print(df22)
