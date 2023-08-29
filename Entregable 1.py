#Entregable 1
"""
Script que extraiga datos de una API pública y crear la tabla 
en Redshift para posterior carga de sus datos.

- El script debería extraer datos en JSON
y poder leer el formato en un
diccionario de Python.

- La entrega involucra la creación de una
versión inicial de la tabla donde los
datos serán cargados posteriormente.

"""

import requests
import pandas as pd

url = "https://meteostat.p.rapidapi.com/stations/daily"

querystring = {"station":"10637","start":"2020-01-01","end":"2020-01-31"}

headers = {
    "X-RapidAPI-Key": "a5b08030f7mshb2c9409a627ea15p10ce88jsn90cea41c9648",
    "X-RapidAPI-Host": "meteostat.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# Convertir la respuesta JSON en un DataFrame de pandas
data = response.json()
df = pd.DataFrame(data['data'])

# Imprimir el DataFrame en formato de tabla
print(df)
