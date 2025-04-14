# src/db/populate_db.py
from src.config.db import get_connection

def load_to_db():
    conn = get_connection()

    try:        
        conn.execute('''drop table if exists Partidos;''')
        conn.execute("create table Partidos(partido text, fechas text, grupo text, resultado text);")

        conn.execute("insert into Partidos values('Qatar vs. Ecuador','21/11/2022','GRUPO A','0,1');")
        conn.execute("insert into Partidos values('Senegal vs. Holanda','21/11/2022','GRUPO A','2,5');")
        conn.execute("insert into Partidos values('Qatar vs. Senegal','25/11/2021','GRUPO A','0,5');")
        conn.execute("insert into Partidos values('Holanda vs. Ecuador','25/11/2021','GRUPO A','3,3');")
        conn.execute("insert into Partidos values('Holanda vs. Qatar','29/11/2021','GRUPO A','5,5');")
        conn.execute("insert into Partidos values('Ecuador vs. Senegal','29/11/2021','GRUPO A','3,2');")
        conn.execute("insert into Partidos values('Inglaterra vs. Irán','21/11/2022','GRUPO B','4,0');")
        conn.execute("insert into Partidos values('Estados Unidos vs. Ucrania-Escocia-Gales','21/11/2022','GRUPO B','1,5');")
        conn.execute("insert into Partidos values('Inglaterra vs. Estados Unidos','25/11/2021','GRUPO B','4,1');")
        conn.execute("insert into Partidos values('Ucrania-Escocia-Gales vs. Iran','25/11/2021','GRUPO B','2,2');")
        conn.execute("insert into Partidos values('Ucrania-Escocia-Gales vs. Inglaterra','29/11/2021','GRUPO B','0,1');")
        conn.execute("insert into Partidos values('Irán vs. Estados Unidos','29/11/2021','GRUPO B','0,0');")
        conn.execute("insert into Partidos values('Argentina vs. Arabia Saudi','22/11/2022','GRUPO C','5,1');")
        conn.execute("insert into Partidos values('México vs. Polonia','22/11/2022','GRUPO C','1,5');")
        conn.execute("insert into Partidos values('Argentina vs. México','26/11/2021','GRUPO C','3,0');")
        conn.execute("insert into Partidos values('Polonia vs. Arabia Saudí','26/11/2021','GRUPO C','1,2');")
        conn.execute("insert into Partidos values('Polonia vs. Argentina','30/11/2021','GRUPO C','2,4');")
        conn.execute("insert into Partidos values('Arabia Saudí vs. México','30/11/2021','GRUPO C','5,5');")
        conn.execute("insert into Partidos values('Francia vs. Peru-Australia-EAU','22/11/2022','GRUPO D','1,1');")
        conn.execute("insert into Partidos values('Dinamarca vs. Túnez','22/11/2022','GRUPO D','5,2');")
        conn.execute("insert into Partidos values('Francia vs. Dinamarca','26/11/2021','GRUPO D','0,0');")
        conn.execute("insert into Partidos values('Túnez vs. Perú-Australia-EAU','26/11/2021','GRUPO D','0,5');")
        conn.execute("insert into Partidos values('Túnez vs. Francia','30/11/2021','GRUPO D','4,0');")
        conn.execute("insert into Partidos values('Perú-Australia-EAU vs. Dinamarca','30/11/2021','GRUPO D','0,0');")
        conn.execute("insert into Partidos values('España vs. Costa Rica-Nueva Zelanda.','23/11/2022','GRUPO E','2,0');")
        conn.execute("insert into Partidos values('Alemania vs. Japón','23/11/2022','GRUPO E','5,3');")
        conn.execute("insert into Partidos values('España vs. Alemania','27/11/2021','GRUPO E','1,2');")
        conn.execute("insert into Partidos values('Japón vs. Costa Rica-Nueva Zelanda','27/11/2021','GRUPO E','2,2');")
        conn.execute("insert into Partidos values('Japón vs. Espana','01/12/2021','GRUPO E','2,1');")
        conn.execute("insert into Partidos values('Costa Rica-Nueva Zelanda vs. Alemania','01/12/2021','GRUPO E','3,0');")
        conn.execute("insert into Partidos values('Portugal vs. Ghana','24/11/2022','GRUPO F','2,1');")
        conn.execute("insert into Partidos values('Uruguay vs. Corea del Sur','24/11/2022','GRUPO F','0,2');")
        conn.execute("insert into Partidos values('Portugal vs. Uruguay','28/11/2021','GRUPO F','1,1');")
        conn.execute("insert into Partidos values('Corea del Sur vs. Ghana','28/11/2021','GRUPO F','3,1');")
        conn.execute("insert into Partidos values('Corea del Sur vs. Portugal.','02/12/2021','GRUPO F','4,5');")
        conn.execute("insert into Partidos values('Ghana vs. Uruguay','02/12/2021','GRUPO F','2,3');")

        # Localidades y fecha de los partidos de la segunda ronda

        conn.execute('''drop table if exists Clasificados;''')
        conn.execute("create table Clasificados(partido text, fecha text, hora text, sede text);")

        conn.execute("insert into Clasificados values('1º del grupo A vs. 2º del grupo B','03/12/2022','06:00:00 PM','Estadio Khalifa, Rayán');")
        conn.execute("insert into Clasificados values('1º del grupo C vs. 2º del grupo D','03/12/2022','10:00:00 PM','Estadio Ahmed bin Ali, Rayán');")
        conn.execute("insert into Clasificados values('1º del grupo D vs. 2º del grupo C','04/12/2022','06:00:00 PM','Estadio Al Thumama, Doha');")
        conn.execute("insert into Clasificados values('1º del grupo B vs. 2º del grupo A','04/12/2022','10:00:00 PM','Estadio Al Bayt, Jor');")
        conn.execute("insert into Clasificados values('1º del grupo E vs. 2º del grupo F','05/12/2022','06:00:00 PM','Estadio Al Janoub, Al Wakrah');")
        conn.execute("insert into Clasificados values('1º del grupo G vs. 2º del grupo H','05/12/2022','10:00:00 PM','Estadio 974, Doha');")
        conn.execute("insert into Clasificados values('1º del grupo F vs. 2º del grupo E','06/12/2022','06:00:00 PM','Estadio Qatar Foundation, Rayán');")
        conn.execute("insert into Clasificados values('1º del grupo H vs. 2º del grupo G','06/12/2022','10:00:00 PM','Estadio Icónico, Lusail');")

        # Totales obtenidos de la simulación por equipo

        conn.execute('''drop table if exists Resultados_Qatar;''')
        conn.execute("create table Resultados_Qatar(equipo text,grupo text, puntos int, gol_diferencia int);")

        conn.execute("insert into Resultados_Qatar values('ALEMANIA','GRUPO E','6','0');")
        conn.execute("insert into Resultados_Qatar values('ARABIA SAUDI','GRUPO C','4','-3');")
        conn.execute("insert into Resultados_Qatar values('ARGENTINA','GRUPO C','9','9');")
        conn.execute("insert into Resultados_Qatar values('COREA DEL SUR','GRUPO F','6','3');")
        conn.execute("insert into Resultados_Qatar values('COSTA RICA-NUEVA ZELANDA','GRUPO E','4','1');")
        conn.execute("insert into Resultados_Qatar values('DINAMARCA','GRUPO D','5','3');")
        conn.execute("insert into Resultados_Qatar values('ECUADOR','GRUPO A','7','2');")
        conn.execute("insert into Resultados_Qatar values('ESPANA','GRUPO E','3','0');")
        conn.execute("insert into Resultados_Qatar values('ESTADOS UNIDOS','GRUPO B','1','-7');")
        conn.execute("insert into Resultados_Qatar values('FRANCIA','GRUPO D','2','-4');")
        conn.execute("insert into Resultados_Qatar values('GHANA','GRUPO F','0','-4');")
        conn.execute("insert into Resultados_Qatar values('HOLANDA','GRUPO A','5','3');")
        conn.execute("insert into Resultados_Qatar values('INGLATERRA','GRUPO B','9','8');")
        conn.execute("insert into Resultados_Qatar values('IRAN','GRUPO B','2','-4');")
        conn.execute("insert into Resultados_Qatar values('JAPON','GRUPO E','4','-1');")
        conn.execute("insert into Resultados_Qatar values('MEXICO','GRUPO C','1','-7');")
        conn.execute("insert into Resultados_Qatar values('PERU-AUSTRALIA-EAU','GRUPO D','5','5');")
        conn.execute("insert into Resultados_Qatar values('POLONIA','GRUPO C','3','1');")
        conn.execute("insert into Resultados_Qatar values('PORTUGAL','GRUPO F','7','2');")
        conn.execute("insert into Resultados_Qatar values('QATAR','GRUPO A','1','-6');")
        conn.execute("insert into Resultados_Qatar values('SENEGAL','GRUPO A','3','1');")
        conn.execute("insert into Resultados_Qatar values('TUNEZ','GRUPO D','3','-4');")
        conn.execute("insert into Resultados_Qatar values('UCRANIA-ESCOCIA-GALES','GRUPO B','4','3');")
        conn.execute("insert into Resultados_Qatar values('URUGUAY','GRUPO F','4','-1');")

        conn.commit()
        print("Valores insertados con éxito");
    except Exception as e:
        print("Error al insertar la base de datos:", e)
    finally:
        conn.close()
