
# Importar los modulos
import json 
import pandas as pd
import mysql.connector as mysql


# Cadena de conexión, en un servidor MySQL, cambiar el user y pass según el caso
cnx = mysql.connect(user='root', password='',
                              host='127.0.0.1',
                              database='RNPDNO')

# Definicion de query para insertar registros
mycursor = cnx.cursor() 
sql = "INSERT INTO info01 (consecutivo,nro_reporte,tipo_reporte,datos_reporte_clb_status_victima,datos_reporte_clb_feciniexpediente,datos_reporte_clb_hipotesis,datos_generales_clb_lugar_nacimiento,datos_generales_clb_fecnacimiento,datos_generales_clb_edad_a,datos_generales_clb_edad_m,datos_generales_clb_edad_d,hechos_clb_fechorahechos,hechos_clb_fechora_percato_desaparicion,actividades_rutas_clb_fecha,convida_rutas_clb_feclocalizacion,convida_rutas_clb_lugarlocaliza,sinvida_rutas_clb_feclocalizacion,sinvida_rutas_clb_lugarlocaliza,datos_reporte_clb_delito,datos_reporte_clb_sexo,datos_reporte_clb_genero,datos_reporte_clb_nacionalidad,datos_reporte_clb_estcivil,lugarhechos_clb_estado,lugarhechos_clb_municipio,lugarhechos_clb_colonia,lugarhechos_clb_estado2,lugarhechos_clb_municipio2,lugarhechos_clb_colonia2,hechos_clb_fechorahechos2,hechos_clb_fechora_percato_desaparicion2,datos_reporte_institucion,datos_reporte_clb_status_victima2,datos_reporte_clb_feciniexpediente2,datos_reporte_clb_hipotesis2,datos_reporte_clb_delito2,datos_reporte_institucion2) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

errores = 0
aciertos = 0
errores_masrep = 0

# La siguiente linea lee la data desde los archivos json, reemplazar según la ruta de los archivos
with open('/Users/javier/Downloads/Json/Archivos/Json01_Id01.json', encoding='utf-8' ) as f:
    js = json.loads(f.read().replace('\xa0',''))


# Inicialización de variables
colConsecutivo = 0
nro_reporte = 0
tipo_reporte = ""
datos_reporte_clb_status_victima = ""
datos_reporte_clb_feciniexpediente = ""
datos_reporte_clb_hipotesis = ""
datos_reporte_clb_delito = ""
datos_reporte_institucion = ""
datos_reporte_clb_status_victima2 = ""
datos_reporte_clb_feciniexpediente2 = ""
datos_reporte_clb_hipotesis2 = ""
datos_reporte_clb_delito2 = ""
datos_reporte_institucion2 = ""
datos_reporte_clb_sexo = ""
datos_reporte_clb_genero = ""
datos_reporte_clb_nacionalidad = ""
datos_reporte_clb_estcivil = ""
datos_generales_clb_lugar_nacimiento = ""
datos_generales_clb_fecnacimiento = ""
datos_generales_clb_edad_a = ""
datos_generales_clb_edad_m = ""
datos_generales_clb_edad_d = ""
hechos_clb_fechorahechos = ""
hechos_clb_fechora_percato_desaparicion = ""
hechos_clb_fechorahechos2 = ""
hechos_clb_fechora_percato_desaparicion2 = ""
convida_rutas_clb_feclocalizacion = ""
convida_rutas_clb_lugarlocaliza = ""
sinvida_rutas_clb_feclocalizacion = ""
sinvida_rutas_clb_lugarlocaliza = ""
actividades_rutas_clb_fecha = ""
lugarhechos_clb_estado = ""
lugarhechos_clb_municipio = ""
lugarhechos_clb_colonia = ""
lugarhechos_clb_estado2 = ""
lugarhechos_clb_municipio2 = ""
lugarhechos_clb_colonia2 = ""


# Procesamiento de la información
for registro in js: 
    try:
        colConsecutivo = registro['Consecutivo']
        df = pd.json_normalize(registro['reportes'])
        nro_filas = df.shape[0]
            
        if nro_filas == 1: 
            nombres_columnas = df.columns.values.tolist()
            nro_reporte = 1

            busqueda = [x for x in nombres_columnas if "Tipo Reporte".upper() in x.upper()]
            if len(busqueda) > 0: 
                tipo_reporte = df[busqueda[0]][0]
            
            busqueda = [x for x in nombres_columnas if "Estatus  de la víctima".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_status_victima = df[busqueda[0]][0]
                indexpunto3 = str(busqueda[0]).rfind(".")
                indexpunto2 = str(busqueda[0]).rfind(".",0,indexpunto3)
                indexpunto1 = str(busqueda[0]).rfind(".",0,indexpunto2)
                datos_reporte_institucion = str(busqueda[0])[indexpunto1+1:indexpunto2]
            if len(busqueda) > 1: 
                datos_reporte_clb_status_victima2 = df[busqueda[1]][0]
                indexpunto3 = str(busqueda[1]).rfind(".")
                indexpunto2 = str(busqueda[1]).rfind(".",0,indexpunto3)
                indexpunto1 = str(busqueda[1]).rfind(".",0,indexpunto2)
                datos_reporte_institucion2 = str(busqueda[1])[indexpunto1+1:indexpunto2]
            
            busqueda = [x for x in nombres_columnas if "Fecha de inicio del expediente".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_feciniexpediente = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                datos_reporte_clb_feciniexpediente2 = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if "Hipótesis de la No Localización".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_hipotesis = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                datos_reporte_clb_hipotesis2 = df[busqueda[1]][0]

            busqueda = [x for x in nombres_columnas if "Delito(s)".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_delito = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                datos_reporte_clb_delito2 = df[busqueda[1]][0]

            busqueda = [x for x in nombres_columnas if "Sexo".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_sexo = df[busqueda[0]][0]

            busqueda = [x for x in nombres_columnas if "Género".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_genero = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Nacionalidad".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_nacionalidad = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Estado civil".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_estcivil = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Lugar de nacimiento".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_lugar_nacimiento = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha de nacimiento".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_fecnacimiento = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Edad  en años".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_edad_a = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Edad en meses".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_edad_m = df[busqueda[0]][0]
    
            busqueda = [x for x in nombres_columnas if "Edad en días".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_edad_d = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha y hora de hechos".upper() in x.upper()]
            if len(busqueda) > 0: 
                hechos_clb_fechorahechos = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                hechos_clb_fechorahechos2 = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha y hora en que se percató de la desaparición".upper() in x.upper()]
            if len(busqueda) > 0: 
                hechos_clb_fechora_percato_desaparicion = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                hechos_clb_fechora_percato_desaparicion2 = df[busqueda[1]][0]
            
            busqueda = [x for x in nombres_columnas if "Fecha de Localización".upper() in x.upper()]
            if len(busqueda) > 0: 
                convida_rutas_clb_feclocalizacion = df[busqueda[0]][0]
    
            busqueda = [x for x in nombres_columnas if "Lugar donde se localiza".upper() in x.upper()]
            if len(busqueda) > 0: 
                convida_rutas_clb_lugarlocaliza = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha de Localización".upper() in x.upper()]
            if len(busqueda) > 0: 
                sinvida_rutas_clb_feclocalizacion = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if "Lugar donde se Localiza".upper() in x.upper()]
            if len(busqueda) > 0: 
                sinvida_rutas_clb_lugarlocaliza = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if ("Fecha".upper() in x.upper() and "ACTIVIDADES Y RUTAS".upper() in x.upper())]
            if len(busqueda) > 0: 
                actividades_rutas_clb_fecha = df[busqueda[0]][0]
            
            busqueda = [x for x in nombres_columnas if "LUGAR DE LOS HECHOS".upper() in x.upper()]
            if len(busqueda) > 0 and type(df[busqueda[0]][0][0]) == dict: 
                if len(busqueda) > 0: 
                    lugarhechos = df[busqueda[0]][0][0]
                    lugarhechos_clb_estado = lugarhechos["Estado "]
                    lugarhechos_clb_municipio = lugarhechos["Alcaldía o Municipio "]
                    lugarhechos_clb_colonia = lugarhechos["Colonia"]
                if len(busqueda) > 1: 
                    lugarhechos = df[busqueda[1]][0][0]
                    lugarhechos_clb_estado2 = lugarhechos["Estado "]
                    lugarhechos_clb_municipio2 = lugarhechos["Alcaldía o Municipio "]
                    lugarhechos_clb_colonia2 = lugarhechos["Colonia"]
            else: 
                busqueda = [x for x in nombres_columnas if ("Estado".upper() in x.upper() and "LUGAR DE LOS HECHOS".upper() in x.upper())]
                if len(busqueda) > 0: 
                    lugarhechos_clb_estado = df[busqueda[0]][0]
                busqueda = [x for x in nombres_columnas if ("Alcaldía o Municipio".upper() in x.upper() and "LUGAR DE LOS HECHOS".upper() in x.upper())]
                if len(busqueda) > 0: 
                    lugarhechos_clb_municipio = df[busqueda[0]][0]
                busqueda = [x for x in nombres_columnas if ("Colonia".upper() in x.upper() and "LUGAR DE LOS HECHOS".upper() in x.upper())]
                if len(busqueda) > 0: 
                    lugarhechos_clb_colonia = df[busqueda[0]][0]

            val = (str(colConsecutivo),str(nro_reporte),tipo_reporte,datos_reporte_clb_status_victima,datos_reporte_clb_feciniexpediente,datos_reporte_clb_hipotesis,datos_generales_clb_lugar_nacimiento,datos_generales_clb_fecnacimiento,datos_generales_clb_edad_a,datos_generales_clb_edad_m,datos_generales_clb_edad_d,hechos_clb_fechorahechos,hechos_clb_fechora_percato_desaparicion,actividades_rutas_clb_fecha,convida_rutas_clb_feclocalizacion,convida_rutas_clb_lugarlocaliza,sinvida_rutas_clb_feclocalizacion,sinvida_rutas_clb_lugarlocaliza,datos_reporte_clb_delito,datos_reporte_clb_sexo,datos_reporte_clb_genero,datos_reporte_clb_nacionalidad,datos_reporte_clb_estcivil,lugarhechos_clb_estado,lugarhechos_clb_municipio,lugarhechos_clb_colonia,lugarhechos_clb_estado2,lugarhechos_clb_municipio2,lugarhechos_clb_colonia2,hechos_clb_fechorahechos2,hechos_clb_fechora_percato_desaparicion2,datos_reporte_institucion,datos_reporte_clb_status_victima2,datos_reporte_clb_feciniexpediente2,datos_reporte_clb_hipotesis2,datos_reporte_clb_delito2,datos_reporte_institucion2)
            mycursor.execute(sql, val)
            cnx.commit()
            aciertos = aciertos + 1

        elif nro_filas == 2: 
            df2 = df
            
            df = df.loc[0,].to_frame().T.dropna(axis=1).reset_index()
            nombres_columnas = df.columns.values.tolist()
            nro_reporte = 1          
            
            busqueda = [x for x in nombres_columnas if "Tipo Reporte".upper() in x.upper()]
            if len(busqueda) > 0: 
                tipo_reporte = df[busqueda[0]][0]
            
            busqueda = [x for x in nombres_columnas if "Estatus  de la víctima".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_status_victima = df[busqueda[0]][0]
                indexpunto3 = str(busqueda[0]).rfind(".")
                indexpunto2 = str(busqueda[0]).rfind(".",0,indexpunto3)
                indexpunto1 = str(busqueda[0]).rfind(".",0,indexpunto2)
                datos_reporte_institucion = str(busqueda[0])[indexpunto1+1:indexpunto2]
            if len(busqueda) > 1: 
                datos_reporte_clb_status_victima2 = df[busqueda[1]][0]
                indexpunto3 = str(busqueda[1]).rfind(".")
                indexpunto2 = str(busqueda[1]).rfind(".",0,indexpunto3)
                indexpunto1 = str(busqueda[1]).rfind(".",0,indexpunto2)
                datos_reporte_institucion2 = str(busqueda[1])[indexpunto1+1:indexpunto2]
            
            busqueda = [x for x in nombres_columnas if "Fecha de inicio del expediente".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_feciniexpediente = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                datos_reporte_clb_feciniexpediente2 = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if "Hipótesis de la No Localización".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_hipotesis = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                datos_reporte_clb_hipotesis2 = df[busqueda[1]][0]

            busqueda = [x for x in nombres_columnas if "Delito(s)".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_delito = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                datos_reporte_clb_delito2 = df[busqueda[1]][0]

            busqueda = [x for x in nombres_columnas if "Sexo".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_sexo = df[busqueda[0]][0]

            busqueda = [x for x in nombres_columnas if "Género".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_genero = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Nacionalidad".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_nacionalidad = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Estado civil".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_estcivil = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Lugar de nacimiento".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_lugar_nacimiento = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha de nacimiento".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_fecnacimiento = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Edad  en años".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_edad_a = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Edad en meses".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_edad_m = df[busqueda[0]][0]
    
            busqueda = [x for x in nombres_columnas if "Edad en días".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_edad_d = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha y hora de hechos".upper() in x.upper()]
            if len(busqueda) > 0: 
                hechos_clb_fechorahechos = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                hechos_clb_fechorahechos2 = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha y hora en que se percató de la desaparición".upper() in x.upper()]
            if len(busqueda) > 0: 
                hechos_clb_fechora_percato_desaparicion = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                hechos_clb_fechora_percato_desaparicion2 = df[busqueda[1]][0]
            
            busqueda = [x for x in nombres_columnas if "Fecha de Localización".upper() in x.upper()]
            if len(busqueda) > 0: 
                convida_rutas_clb_feclocalizacion = df[busqueda[0]][0]
    
            busqueda = [x for x in nombres_columnas if "Lugar donde se localiza".upper() in x.upper()]
            if len(busqueda) > 0: 
                convida_rutas_clb_lugarlocaliza = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha de Localización".upper() in x.upper()]
            if len(busqueda) > 0: 
                sinvida_rutas_clb_feclocalizacion = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if "Lugar donde se Localiza".upper() in x.upper()]
            if len(busqueda) > 0: 
                sinvida_rutas_clb_lugarlocaliza = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if ("Fecha".upper() in x.upper() and "ACTIVIDADES Y RUTAS".upper() in x.upper())]
            if len(busqueda) > 0: 
                actividades_rutas_clb_fecha = df[busqueda[0]][0]
            
            busqueda = [x for x in nombres_columnas if "LUGAR DE LOS HECHOS".upper() in x.upper()]
            if len(busqueda) > 0 and type(df[busqueda[0]][0][0]) == dict: 
                if len(busqueda) > 0: 
                    lugarhechos = df[busqueda[0]][0][0]
                    lugarhechos_clb_estado = lugarhechos["Estado "]
                    lugarhechos_clb_municipio = lugarhechos["Alcaldía o Municipio "]
                    lugarhechos_clb_colonia = lugarhechos["Colonia"]
                if len(busqueda) > 1: 
                    lugarhechos = df[busqueda[1]][0][0]
                    lugarhechos_clb_estado2 = lugarhechos["Estado "]
                    lugarhechos_clb_municipio2 = lugarhechos["Alcaldía o Municipio "]
                    lugarhechos_clb_colonia2 = lugarhechos["Colonia"]
            else: 
                busqueda = [x for x in nombres_columnas if ("Estado".upper() in x.upper() and "LUGAR DE LOS HECHOS".upper() in x.upper())]
                if len(busqueda) > 0: 
                    lugarhechos_clb_estado = df[busqueda[0]][0]
                busqueda = [x for x in nombres_columnas if ("Alcaldía o Municipio".upper() in x.upper() and "LUGAR DE LOS HECHOS".upper() in x.upper())]
                if len(busqueda) > 0: 
                    lugarhechos_clb_municipio = df[busqueda[0]][0]
                busqueda = [x for x in nombres_columnas if ("Colonia".upper() in x.upper() and "LUGAR DE LOS HECHOS".upper() in x.upper())]
                if len(busqueda) > 0: 
                    lugarhechos_clb_colonia = df[busqueda[0]][0]

            val = (str(colConsecutivo),str(nro_reporte),tipo_reporte,datos_reporte_clb_status_victima,datos_reporte_clb_feciniexpediente,datos_reporte_clb_hipotesis,datos_generales_clb_lugar_nacimiento,datos_generales_clb_fecnacimiento,datos_generales_clb_edad_a,datos_generales_clb_edad_m,datos_generales_clb_edad_d,hechos_clb_fechorahechos,hechos_clb_fechora_percato_desaparicion,actividades_rutas_clb_fecha,convida_rutas_clb_feclocalizacion,convida_rutas_clb_lugarlocaliza,sinvida_rutas_clb_feclocalizacion,sinvida_rutas_clb_lugarlocaliza,datos_reporte_clb_delito,datos_reporte_clb_sexo,datos_reporte_clb_genero,datos_reporte_clb_nacionalidad,datos_reporte_clb_estcivil,lugarhechos_clb_estado,lugarhechos_clb_municipio,lugarhechos_clb_colonia,lugarhechos_clb_estado2,lugarhechos_clb_municipio2,lugarhechos_clb_colonia2,hechos_clb_fechorahechos2,hechos_clb_fechora_percato_desaparicion2,datos_reporte_institucion,datos_reporte_clb_status_victima2,datos_reporte_clb_feciniexpediente2,datos_reporte_clb_hipotesis2,datos_reporte_clb_delito2,datos_reporte_institucion2)
            mycursor.execute(sql, val)
            cnx.commit()
            aciertos = aciertos + 1
            
            
            df = df2
            df = df.loc[1,].to_frame().T.dropna(axis=1).reset_index()
            nombres_columnas = df.columns.values.tolist()
            nro_reporte = 2
            
            busqueda = [x for x in nombres_columnas if "Tipo Reporte".upper() in x.upper()]
            if len(busqueda) > 0: 
                tipo_reporte = df[busqueda[0]][0]
            
            busqueda = [x for x in nombres_columnas if "Estatus  de la víctima".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_status_victima = df[busqueda[0]][0]
                indexpunto3 = str(busqueda[0]).rfind(".")
                indexpunto2 = str(busqueda[0]).rfind(".",0,indexpunto3)
                indexpunto1 = str(busqueda[0]).rfind(".",0,indexpunto2)
                datos_reporte_institucion = str(busqueda[0])[indexpunto1+1:indexpunto2]
            if len(busqueda) > 1: 
                datos_reporte_clb_status_victima2 = df[busqueda[1]][0]
                indexpunto3 = str(busqueda[1]).rfind(".")
                indexpunto2 = str(busqueda[1]).rfind(".",0,indexpunto3)
                indexpunto1 = str(busqueda[1]).rfind(".",0,indexpunto2)
                datos_reporte_institucion2 = str(busqueda[1])[indexpunto1+1:indexpunto2]
            
            busqueda = [x for x in nombres_columnas if "Fecha de inicio del expediente".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_feciniexpediente = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                datos_reporte_clb_feciniexpediente2 = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if "Hipótesis de la No Localización".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_hipotesis = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                datos_reporte_clb_hipotesis2 = df[busqueda[1]][0]

            busqueda = [x for x in nombres_columnas if "Delito(s)".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_delito = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                datos_reporte_clb_delito2 = df[busqueda[1]][0]

            busqueda = [x for x in nombres_columnas if "Sexo".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_sexo = df[busqueda[0]][0]

            busqueda = [x for x in nombres_columnas if "Género".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_genero = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Nacionalidad".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_nacionalidad = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Estado civil".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_reporte_clb_estcivil = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Lugar de nacimiento".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_lugar_nacimiento = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha de nacimiento".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_fecnacimiento = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Edad  en años".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_edad_a = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Edad en meses".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_edad_m = df[busqueda[0]][0]
    
            busqueda = [x for x in nombres_columnas if "Edad en días".upper() in x.upper()]
            if len(busqueda) > 0: 
                datos_generales_clb_edad_d = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha y hora de hechos".upper() in x.upper()]
            if len(busqueda) > 0: 
                hechos_clb_fechorahechos = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                hechos_clb_fechorahechos2 = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha y hora en que se percató de la desaparición".upper() in x.upper()]
            if len(busqueda) > 0: 
                hechos_clb_fechora_percato_desaparicion = df[busqueda[0]][0]
            if len(busqueda) > 1 and datos_reporte_institucion2 != "": 
                hechos_clb_fechora_percato_desaparicion2 = df[busqueda[1]][0]
            
            busqueda = [x for x in nombres_columnas if "Fecha de Localización".upper() in x.upper()]
            if len(busqueda) > 0: 
                convida_rutas_clb_feclocalizacion = df[busqueda[0]][0]
    
            busqueda = [x for x in nombres_columnas if "Lugar donde se localiza".upper() in x.upper()]
            if len(busqueda) > 0: 
                convida_rutas_clb_lugarlocaliza = df[busqueda[0]][0]
                
            busqueda = [x for x in nombres_columnas if "Fecha de Localización".upper() in x.upper()]
            if len(busqueda) > 0: 
                sinvida_rutas_clb_feclocalizacion = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if "Lugar donde se Localiza".upper() in x.upper()]
            if len(busqueda) > 0: 
                sinvida_rutas_clb_lugarlocaliza = df[busqueda[1]][0]
                
            busqueda = [x for x in nombres_columnas if ("Fecha".upper() in x.upper() and "ACTIVIDADES Y RUTAS".upper() in x.upper())]
            if len(busqueda) > 0: 
                actividades_rutas_clb_fecha = df[busqueda[0]][0]
            
            busqueda = [x for x in nombres_columnas if "LUGAR DE LOS HECHOS".upper() in x.upper()]
            if len(busqueda) > 0 and type(df[busqueda[0]][0][0]) == dict: 
                if len(busqueda) > 0: 
                    lugarhechos = df[busqueda[0]][0][0]
                    lugarhechos_clb_estado = lugarhechos["Estado "]
                    lugarhechos_clb_municipio = lugarhechos["Alcaldía o Municipio "]
                    lugarhechos_clb_colonia = lugarhechos["Colonia"]
                if len(busqueda) > 1: 
                    lugarhechos = df[busqueda[1]][0][0]
                    lugarhechos_clb_estado2 = lugarhechos["Estado "]
                    lugarhechos_clb_municipio2 = lugarhechos["Alcaldía o Municipio "]
                    lugarhechos_clb_colonia2 = lugarhechos["Colonia"]
            else: 
                busqueda = [x for x in nombres_columnas if ("Estado".upper() in x.upper() and "LUGAR DE LOS HECHOS".upper() in x.upper())]
                if len(busqueda) > 0: 
                    lugarhechos_clb_estado = df[busqueda[0]][0]
                busqueda = [x for x in nombres_columnas if ("Alcaldía o Municipio".upper() in x.upper() and "LUGAR DE LOS HECHOS".upper() in x.upper())]
                if len(busqueda) > 0: 
                    lugarhechos_clb_municipio = df[busqueda[0]][0]
                busqueda = [x for x in nombres_columnas if ("Colonia".upper() in x.upper() and "LUGAR DE LOS HECHOS".upper() in x.upper())]
                if len(busqueda) > 0: 
                    lugarhechos_clb_colonia = df[busqueda[0]][0]

            val = (str(colConsecutivo),str(nro_reporte),tipo_reporte,datos_reporte_clb_status_victima,datos_reporte_clb_feciniexpediente,datos_reporte_clb_hipotesis,datos_generales_clb_lugar_nacimiento,datos_generales_clb_fecnacimiento,datos_generales_clb_edad_a,datos_generales_clb_edad_m,datos_generales_clb_edad_d,hechos_clb_fechorahechos,hechos_clb_fechora_percato_desaparicion,actividades_rutas_clb_fecha,convida_rutas_clb_feclocalizacion,convida_rutas_clb_lugarlocaliza,sinvida_rutas_clb_feclocalizacion,sinvida_rutas_clb_lugarlocaliza,datos_reporte_clb_delito,datos_reporte_clb_sexo,datos_reporte_clb_genero,datos_reporte_clb_nacionalidad,datos_reporte_clb_estcivil,lugarhechos_clb_estado,lugarhechos_clb_municipio,lugarhechos_clb_colonia,lugarhechos_clb_estado2,lugarhechos_clb_municipio2,lugarhechos_clb_colonia2,hechos_clb_fechorahechos2,hechos_clb_fechora_percato_desaparicion2,datos_reporte_institucion,datos_reporte_clb_status_victima2,datos_reporte_clb_feciniexpediente2,datos_reporte_clb_hipotesis2,datos_reporte_clb_delito2,datos_reporte_institucion2)
            mycursor.execute(sql, val)
            cnx.commit()
            aciertos = aciertos + 1
            
        else:
            errores_masrep = errores_masrep + 1
            continue;         
    except:
        errores = errores + 1
    finally:        
        colConsecutivo = 0
        nro_reporte = 0
        tipo_reporte = ""
        datos_reporte_clb_status_victima = ""
        datos_reporte_clb_feciniexpediente = ""
        datos_reporte_clb_hipotesis = ""
        datos_reporte_clb_delito = ""
        datos_reporte_institucion = ""
        datos_reporte_clb_status_victima2 = ""
        datos_reporte_clb_feciniexpediente2 = ""
        datos_reporte_clb_hipotesis2 = ""
        datos_reporte_clb_delito2 = ""
        datos_reporte_institucion2 = ""
        datos_reporte_clb_sexo = ""
        datos_reporte_clb_genero = ""
        datos_reporte_clb_nacionalidad = ""
        datos_reporte_clb_estcivil = ""
        datos_generales_clb_lugar_nacimiento = ""
        datos_generales_clb_fecnacimiento = ""
        datos_generales_clb_edad_a = ""
        datos_generales_clb_edad_m = ""
        datos_generales_clb_edad_d = ""
        hechos_clb_fechorahechos = ""
        hechos_clb_fechora_percato_desaparicion = ""
        hechos_clb_fechorahechos2 = ""
        hechos_clb_fechora_percato_desaparicion2 = ""
        convida_rutas_clb_feclocalizacion = ""
        convida_rutas_clb_lugarlocaliza = ""
        sinvida_rutas_clb_feclocalizacion = ""
        sinvida_rutas_clb_lugarlocaliza = ""
        actividades_rutas_clb_fecha = ""
        lugarhechos_clb_estado = ""
        lugarhechos_clb_municipio = ""
        lugarhechos_clb_colonia = ""
        lugarhechos_clb_estado2 = ""
        lugarhechos_clb_municipio2 = ""
        lugarhechos_clb_colonia2 = ""

cnx.close()
# Mostrar resumen del procesamiento
print(aciertos)
print(errores)
print(errores_masrep)


            