# Importar los modulos 
import pandas as pd
import mysql.connector as mysql
import os

# Cadena de conexión, en un servidor MySQL, cambiar el user y pass según el caso
cnx = mysql.connect(user='root', password='',
                              host='127.0.0.1',
                              database='RNPDNO')

# Query que obtiene todo el contenido de la tabla INFO_VF y lo guarda en un CSV que se trabajara en Google Colab
dffinal = pd.read_sql(sql='SELECT * FROM INFO_VF', con=cnx)
dffinal.to_csv('RNPDNO.csv', index=False, encoding = 'utf-8-sig')
cnx.close()