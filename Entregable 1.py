#Entregable 1

import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

#Declaracion de credenciales + conexion
credenciales = {
    'username': 'juanaescobarpinzon_coderhouse',
    'password': 'nG6Z4M9Z80',
    'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    'port': '5439',
    'database': 'data-engineer-database'
}
db_conn_str = f"postgresql://{credenciales['username']}:{credenciales['password']}@{credenciales['host']}:{credenciales['port']}/{credenciales['database']}"
conn = create_engine(db_conn_str)

#Request de la API meteostat
url = "https://meteostat.p.rapidapi.com/stations/daily"

querystring = {"station":"10637","start":"2023-08-01","end":"2023-12-01"}
headers = {
    "X-RapidAPI-Key": "a5b08030f7mshb2c9409a627ea15p10ce88jsn90cea41c9648",
    "X-RapidAPI-Host": "meteostat.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

data = response.json()

#Aca se filtran los campos que quiero tener y los puedo renombrar
filtered_data = []
for entry in data['data']:
    filtered_entry = {
        'fecha': entry['date'],
        'temp_promedio': entry['tavg'],
        'temp_minima': entry['tmin'],
        'temp_maxima': entry['tmax'],
        'precipitaciones': entry['prcp'],
        'nieve': entry['snow'],
        'dir_viento': entry['wdir'],
        'vel_viento': entry['wspd'],
        'presion': entry['pres'],
        'minutos_atardecer': entry['tsun']
        
    }
    filtered_data.append(filtered_entry)

#Crear el dataframe
df = pd.DataFrame(filtered_data)

table_name = 'prueba_clima'

#Ver si existe la tabla
inspector = inspect(conn)
table_exists = any(table_name.lower() == table.lower() for table in inspector.get_table_names())

# Si no existe se crea
if not table_exists:
    df[:0].to_sql(table_name, conn, index=False)  # Crea la estructura de la tabla vacía

# Ingestar los datos en la tabla
df.to_sql(table_name, conn, index=False, if_exists='append')

# Cerrar la conexión
conn.dispose()











