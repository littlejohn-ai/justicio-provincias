import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import fitz
from io import BytesIO
import mysql.connector

# Configuración de la conexión a MySQL
print("Conectando a la base de datos...")
mydb = mysql.connector.connect(
    host="localhost",
    user="colab_user",
    password="justicio_PASS1234",
    database="justicio"
)
print("Conexión establecida.")

mycursor = mydb.cursor()

url = 'https://concellodelugo.gal/es/ordenanzas-municipales/'
ciudad = 'Lugo'
date = datetime.today().strftime('%Y-%m-%d')

print(f"Fecha actual: {date}")

headers = {
 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

print(f"Haciendo petición GET a {url}...")
response = requests.get(url, headers=headers)
print(f"Respuesta recibida. Código de estado: {response.status_code}")

soup = BeautifulSoup(response.content, 'html.parser')
filtered_urls_base = {}
print("Buscando enlaces en la página principal...")

for a_tag in soup.find_all('a', href=True):
    href = a_tag['href']
    if 'documentos-repositorio?combine=' in href:
        group = "Ordenanza fiscal " + a_tag.get_text(strip=True)
        if group not in filtered_urls_base:
            filtered_urls_base[group] = []
        filtered_urls_base[group].append(href)
        print(f"Grupo encontrado: {group} - URL añadida: {href}")

filtered_urls = []
print("Explorando enlaces secundarios...")

for group, urls in filtered_urls_base.items():
    print(f"Procesando grupo: {group}")
    for base_url in urls:
        page_number = 0
        while True:
            paginated_url = f"{base_url}&page={page_number}"
            print(f"Haciendo petición GET a {paginated_url}...")
            response = requests.get(paginated_url, headers=headers)
            print(f"Respuesta recibida. Código de estado: {response.status_code}")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            document_links_found = False  # Controla si se encontraron enlaces de documentos en esta página
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if 'adjuntos' in href:
                    document_links_found = True
                    full_href = 'https://concellodelugo.gal' + href if href.startswith('/es') else href
                    print(f"Encontrado enlace de adjunto: {full_href}")
                    response = requests.get(full_href)
                    if response.status_code == 200:
                        print(f"Descargando archivo PDF desde {full_href}...")
                        pdf_data = BytesIO(response.content)
                        if pdf_data.getbuffer().nbytes > 0:
                            try:
                                doc = fitz.open("pdf", pdf_data)
                                content = ""
                                for page in doc:
                                    content += page.get_text()
                                print(f"Contenido del PDF extraído.")
                            except Exception as e:
                                print(f"Error al abrir el PDF desde {full_href}: {str(e)}")
                                content = ""
                        else:
                            print(f"Advertencia: El archivo descargado desde {full_href} está vacío.")
                            content = ""
                    else:
                        print(f"Error al descargar el archivo {full_href}: {response.status_code}")
                        content = ""
                    
                    title = a_tag.get_text(strip=True)
                    print(f"Título del documento: {title}")
                    filtered_urls.append({
                        'ciudad': ciudad,
                        'date': date,
                        'grupo': group,
                        'subgrupo': '',
                        'titulo': title,
                        'url': full_href,
                        'content': content
                    })
            
            if not document_links_found:
                print(f"No se encontraron más documentos en la página {page_number} para el grupo {group}. Deteniendo.")
                break
            
            page_number += 1  # Incrementa el número de página para la siguiente iteración

df = pd.DataFrame(filtered_urls)
print(f"DataFrame creado con {len(df)} filas.")

# Insertar los datos en la tabla de MySQL
for index, row in df.iterrows():
    sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (row['ciudad'], row['date'], row['titulo'], row['grupo'], row['subgrupo'], row['url'], row['content'])
    print(f"Ejecutando SQL: {sql} con valores: {val}")
    mycursor.execute(sql, val)
    print(row['titulo'], "insertado en tabla. Rowcount:", mycursor.rowcount)

mydb.commit()
print("Cambios guardados en la base de datos.")

print(mycursor.rowcount, "registro(s) insertado(s).")
