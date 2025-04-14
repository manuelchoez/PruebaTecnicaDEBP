# Proyecto: Simulación de Resultados del Mundial de Fútbol

Este proyecto simula y analiza los resultados de un torneo de fútbol, generando estadísticas de equipos, clasificando a los mejores equipos por grupo y asignando partidos para las rondas eliminatorias. 

## Estructura del Proyecto

El proyecto está organizado en los siguientes módulos:

### 1. **Cargar Datos a la Base de Datos**
- **Archivo:** [`src/load/load_to_db.py`](src/load/load_to_db.py)
- **Función Principal:** `load_to_db()`
- **Descripción:** Este módulo inicializa la base de datos SQLite y carga los datos de los partidos, clasificados y resultados simulados.

### 2. **Cálculo de Estadísticas de Equipos**
- **Archivo:** [`src/report/ejercicio1.py`](src/report/ejercicio1.py)
- **Función Principal:** `calcular_estadisticas_equipos()`
- **Descripción:** Calcula estadísticas como puntos totales y diferencia de goles para cada equipo basado en los resultados de los partidos.

### 3. **Clasificación de Equipos**
- **Archivo:** [`src/report/ejercicio21.py`](src/report/ejercicio21.py)
- **Función Principal:** `calcular_clasificados()`
- **Descripción:** Clasifica a los dos mejores equipos de cada grupo y los guarda en la tabla `Clasificados_Qatar`.

### 4. **Asignación de Partidos Clasificados**
- **Archivo:** [`src/report/ejercicio22.py`](src/report/ejercicio22.py)
- **Función Principal:** `asignar_partidos_clasificados()`
- **Descripción:** Asigna los equipos clasificados a los partidos de las rondas eliminatorias basándose en las posiciones de los grupos.

### 5. **Configuración de la Base de Datos**
- **Archivo:** [`src/config/db.py`](src/config/db.py)
- **Función Principal:** `get_connection()`
- **Descripción:** Proporciona una conexión a la base de datos SQLite.

## Ejecución del Proyecto

El archivo principal para ejecutar el proyecto es [`main.py`](main.py). Este archivo realiza las siguientes acciones:
1. Carga los datos iniciales en la base de datos.
2. Calcula las estadísticas de los equipos.
3. Clasifica a los mejores equipos por grupo.
4. Asigna los partidos para las rondas eliminatorias.
5. Imprime los resultados en la consola.

### Comando para ejecutar:
```bash
python [main.py](http://_vscodecontentref_/1)