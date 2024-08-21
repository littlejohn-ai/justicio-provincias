import os
import requests
from PyPDF2 import PdfReader
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import datetime
import mysql.connector

# Configurar el driver de Chrome
options = Options()
# options.add_argument('--headless')  # Ejecutar en modo headless si no quieres abrir el navegador
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

# Función para descargar un archivo PDF
def descargar_pdf(url, nombre_archivo):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open(nombre_archivo, 'wb') as f:
                f.write(response.content)
            return True
        else:
            print(f"No se pudo descargar el PDF. Estado de la respuesta: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error al descargar el PDF: {e}")
        return False

# Función para leer el contenido de un archivo PDF
def leer_pdf(nombre_archivo):
    contenido = ""
    try:
        with open(nombre_archivo, 'rb') as f:
            lector_pdf = PdfReader(f)
            for pagina in lector_pdf.pages:
                contenido += pagina.extract_text()
    except Exception as e:
        print(f"Error al leer el PDF {nombre_archivo}: {e}")
    return contenido

# Función para convertir la fecha al formato MySQL (YYYY-MM-DD)
def convertir_fecha(fecha):
    try:
        return datetime.datetime.strptime(fecha, '%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return fecha

# Conectar a la base de datos
conn = mysql.connector.connect(
    host="localhost",
    user="isole",
    password="wLjR2596HV8brNUYxtqFm7",
    database="justicio"
)
cursor = conn.cursor()

# Crear un directorio para almacenar los archivos PDF
if not os.path.exists("pdfs"):
    os.makedirs("pdfs")

try:
    # Abrir la página web
    url = "https://seu.tarragona.cat/sta/CarpetaPublic/doEvent?APP_CODE=STA&PAGE_CODE=PTS2_ORDENANZA&lang=es#"
    driver.get(url)

    # Encontrar todos los elementos <a>
    enlaces = driver.find_elements(By.TAG_NAME, 'a')

    # Extraer y listar los títulos de todos los enlaces
    encontrado = False
    for enlace in enlaces:
        texto = enlace.text
        if texto.strip() == "Todos los tablones\nVer más ...":
            print("******ENCONTRADO!!!!")
            enlace.click()
            encontrado = True
            break

    if not encontrado:
        print("No se encontró el enlace con el texto 'Todos los tablones'.")

    # Esperar hasta que el select esté presente
    wait = WebDriverWait(driver, 20)
    select_element = wait.until(EC.presence_of_element_located((By.NAME, "PTS2_ORDENANZA__LISTA_length")))

    # Seleccionar el valor -1 para mostrar todos los registros
    select = Select(select_element)
    select.select_by_value("-1")

    # Esperar hasta que la tabla se actualice
    time.sleep(5)  # Ajusta según sea necesario

    filas_procesadas = set()
    filas_restantes = True
    while filas_restantes:
        # Encontrar la tabla con id PTS2_ORDENANZA__LISTA
        tabla = driver.find_element(By.ID, "PTS2_ORDENANZA__LISTA")
        filas = tabla.find_elements(By.XPATH, ".//tbody/tr")

        if not filas:
            break

        filas_restantes = False  # Asumir que no hay más filas restantes a menos que encontremos alguna sin procesar

        for index, fila in enumerate(filas):
            if index in filas_procesadas:
                continue

            filas_restantes = True  # Encontramos una fila sin procesar, por lo que hay más filas restantes
            
            # Marcar la fila como procesada
            filas_procesadas.add(index)

            # Hacer clic en la fila
            fila.click()

            # Esperar hasta que la nueva página cargue
            time.sleep(5)  # Ajusta según sea necesario

            # Extraer datos de la nueva pantalla
            detalles = {}
            detalle_divs = driver.find_elements(By.XPATH, "//div[@class='section-entry ']")
            for detalle in detalle_divs:
                label = detalle.find_element(By.CLASS_NAME, 'entry-label').text.strip()
                value = detalle.find_element(By.CLASS_NAME, 'entry-value').text.strip()
                detalles[label] = value
            
            print(f"Datos de la fila {index + 1}: {detalles}")

            # Descargar los enlaces a archivos PDF
            pdf_links = driver.find_elements(By.XPATH, "//a[contains(@class, 'pdfLink')]")
            for pdf_link in pdf_links:
                pdf_nombre = pdf_link.text.strip()
                pdf_url = pdf_link.get_attribute('href')
                nombre_archivo = os.path.join("pdfs", pdf_nombre + ".pdf")
                
                if not os.path.exists(nombre_archivo):
                    print(f"Descargando {pdf_nombre} desde {pdf_url}")
                    if descargar_pdf(pdf_url, nombre_archivo):
                        contenido_pdf = leer_pdf(nombre_archivo)
                    else:
                        print(f"No se pudo descargar el PDF {pdf_url}")
                        contenido_pdf = ""
                else:
                    print(f"El archivo {nombre_archivo} ya existe. Leyendo contenido del archivo existente.")
                    contenido_pdf = leer_pdf(nombre_archivo)

                # Escapar los caracteres de apóstrofe en el contenido del PDF
                contenido_pdf_escapado = contenido_pdf.replace("'", "''")

                # Convertir la fecha al formato MySQL
                fecha_publicacion = convertir_fecha(detalles.get('Fecha publicación', ''))

                # Crear el comando SQL para insertar los datos
                grupo = detalles.get('Procedencia', '')
                subgrupo = detalles.get('Origen', '')
                insert_query = """
                INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                # Ejecutar el comando SQL
                cursor.execute(insert_query, (
                    'Tarragona', fecha_publicacion, pdf_nombre, grupo, subgrupo, pdf_url, contenido_pdf_escapado
                ))
                conn.commit()

            # Ejecutar la función JavaScript para volver a la lista
            driver.execute_script("callWidgetEvent('PTS2_ORDENANZA', 'DETALLE', 'LISTA', '')")

            # Esperar a que la tabla se cargue nuevamente
            wait.until(EC.presence_of_element_located((By.ID, "PTS2_ORDENANZA__LISTA")))
            time.sleep(5)  # Ajusta según sea necesario
            break

finally:
    # Cerrar el navegador
    driver.quit()
    cursor.close()
    conn.close()
