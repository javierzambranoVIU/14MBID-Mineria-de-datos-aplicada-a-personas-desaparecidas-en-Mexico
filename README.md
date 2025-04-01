A continuación, se realiza una pequeña explicación de los elementos incluidos:
• Extracción de datos Json.py: Archivo que contiene la lógica para extraer la información de los archivos json y realizar limpieza, transformación y carga en un ambiente estructurado (MySQL).
• Generación de data del INEGI.zip: Archivo comprimido que contiene el script SQL para generar ladata del censo 2020 realizado por el INEGI.
• Tratamiento de datos.sql: Archivo que contiene el script necesario para realizar la limpieza, transformación y agregar información complementaria respecto a la base de datos creada en el paso anterior.
• Consulta a Repositorio MySQL.py: Archivo que contiene la lógica necesaria para leer el repositorio estructurado creado en MySQL y poderlo convertir a un archivo CSV que pueda ser explotado en Google Colab.
• Proceso de KDD.ipynb: Archivo que contiene el proceso KDD, desde la minería de datos, creado en Google Colab, cuyo input es el archivo CSV obtenido desde el repositorio MySQL.

La carpeta "Informacion a subir Google Colab", contiene los archivos a subir a Google Colab para que pueda ejecutarse el archivo "Proceso de KDD.ipynb". Se debe descomprimir el archivo RNPDNO.zip que dentro contiene al archivo RNPDNO.csv.
La carpeta "Informacion fuente en formato json", contiene comprimidos los archivos fuente en formato json que utiliza el programa "Extracción de datos Json.py" para extraer la información.
