import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import fitz
from io import BytesIO
import mysql.connector
import time

inicio = time.time()

# Configuración de la conexión a MySQL
mydb = mysql.connector.connect(
    host="localhost",      
    user="root",      
    password="root",  
    database="palma_db" 
)

mycursor = mydb.cursor()

url = 'https://seuelectronica.palma.cat/es/web/seuelectronica/classificaci%C3%B3n-cronol%C3%B2gica?categoryId=40483'
ciudad = 'Palma'
date = datetime.today().strftime('%Y-%m-%d')

# Extraer enlaces de normativas (grupos)
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')
filtered_urls_base = {}
for a_tag in soup.find_all('a', href=True):
   href = a_tag['href']
   if '/es/-/' in href or '/es/web/' in href:
      #group = a_tag.find_parent('tr').find_previous_sibling('tr').find('p').get_text(strip=True)
      #subgroup = a_tag.get_text(strip=True)
      group = a_tag.get_text(strip=True)
      if group not in filtered_urls_base:
         filtered_urls_base[group] = []
      filtered_urls_base[group].append(href)

# Extraer documentos dentro de cada normativa
filtered_urls = []
errorDocs = contentDocs = 0
for group, urls in filtered_urls_base.items():
    for base_url in urls:
        response = requests.get(base_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        for a_tag in soup.find_all('a', href=True):  
            if 'documents' in a_tag['href']:
                if 'https' in a_tag['href']:
                    href = a_tag['href']
                else:
                    href = 'https://seuelectronica.palma.cat' + a_tag['href']                        
                response = requests.get(href)
                if response.status_code == 200:
                    pdf_data = BytesIO(response.content)
                    try: 
                        doc = fitz.open("pdf", pdf_data)
                    except:
                        print(f"Error al abrir documento {href} para el grupo {group}")
                        errorDocs += 1
                    content  = ""
                    for page in doc:
                        content  += page.get_text()
                    if content != "":
                        contentDocs += 1
                else:
                    print(f"Error al descargar el archivo {url}: {response.status_code}")
                title = a_tag.get_text(strip=True)
                filtered_urls.append({
                       'ciudad': ciudad,
                       'date': date,
                       'grupo': group,
                       'subgrupo': '',                   
                       'titulo': title,
                       'url': href,
                       'content': content
                })

df = pd.DataFrame(filtered_urls)

# Insertar los datos en la tabla de MySQL
for index, row in df.iterrows():
    sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (row['ciudad'], row['date'], row['titulo'], row['grupo'], row['subgrupo'], row['url'], row['content'])
    mycursor.execute(sql, val)
    print(row['titulo'], " insertado en tabla.")

mydb.commit()

print(mycursor.rowcount, "registro(s) insertado(s).")

fin = time.time()
total = fin-inicio

print("Grupos extraídos: ", len(filtered_urls_base))
print("Documentos extraídos: ", len(filtered_urls))
print("Documentos erróneos: ", errorDocs)
print("Documentos contenido: ", contentDocs)
print("Porcentaje éxito (contenido/extraídos): ", (contentDocs/len(filtered_urls))*100, "%")
print("Tiempo transcurrido: ", total, "s")