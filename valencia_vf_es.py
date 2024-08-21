import requests
from bs4 import BeautifulSoup
from datetime import datetime
from io import BytesIO
import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import mysql.connector
import time

# Configuración de la ruta a Tesseract OCR
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

# Configuración de la conexión a MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="laura",
    password="1234",
    database="valencia_v1",
    charset="utf8mb4",
    use_unicode=True
)
mycursor = mydb.cursor()

# Configuración inicial
url = 'https://sede.valencia.es/sede/ordenanzas/index.xhtml?lang=1'
url_base = 'https://sede.valencia.es'
ciudad = 'Valencia'
date = datetime.today().strftime('%Y-%m-%d')

specific_pdfs = [
    # Lista de URLs de PDFs específicos si es necesario
]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

# Función para limpiar el texto
def limpiar_texto(texto):
    return texto.encode('utf-8', 'ignore').decode('utf-8').strip()

# Método para extraer texto de PDF usando PyMuPDF y OCR
def extract_text_from_pdf(pdf_data, force_ocr=False):
    text = ""
    pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
    for page_num in range(pdf_document.page_count):
        page = pdf_document.load_page(page_num)
        page_text = page.get_text()
        if page_text and not force_ocr:
            text += page_text
        else:
            image = page.get_pixmap()
            img = Image.frombytes("RGB", [image.width, image.height], image.samples)
            page_text = pytesseract.image_to_string(img, lang='spa')
            text += page_text
    pdf_document.close()
    return limpiar_texto(text)

# Configurar el tiempo de espera para las solicitudes (en segundos)
TIMEOUT = 30
RETRY_COUNT = 2

# Función para verificar si una URL apunta a un archivo PDF con manejo de errores
def is_pdf_content(url):
    for attempt in range(RETRY_COUNT):
        try:
            head = requests.head(url, headers=headers, allow_redirects=True, timeout=TIMEOUT)
            content_type = head.headers.get('Content-Type', '')
            return content_type == 'application/pdf'
        except requests.exceptions.ReadTimeout:
            print(f"Tiempo de espera agotado para la URL {url}. Intento {attempt + 1} de {RETRY_COUNT}")
            if attempt < RETRY_COUNT - 1:
                time.sleep(2)  # Esperar un poco antes de reintentar
                continue
            return False
        except requests.RequestException as e:
            print(f"Error al verificar la URL {url}: {e}")
            return False

# Función para procesar PDF desde una URL con manejo de errores y tiempo de espera
def process_pdf_url(href, title, group, subgrupo, update_existing=False, force_ocr=False):
    for attempt in range(RETRY_COUNT):
        try:
            response = requests.get(href, timeout=TIMEOUT)
            if response.status_code == 200:
                pdf_data = BytesIO(response.content)
                content = extract_text_from_pdf(pdf_data, force_ocr)
                if update_existing:
                    mycursor.execute("SELECT COUNT(*) FROM normativa WHERE url = %s", (href,))
                    result = mycursor.fetchone()
                    if result[0] > 0:
                        sql = "UPDATE normativa SET content = %s WHERE url = %s"
                        val = (content, href)
                        try:
                            mycursor.execute(sql, val)
                            print(f"Contenido del PDF {href} actualizado en la tabla.")
                        except mysql.connector.Error as err:
                            print(f"Error al actualizar {href}: {err}")
                    else:
                        print(f"La URL {href} no existe en la base de datos, no se puede actualizar.")
                else:
                    filtered_urls.append({
                        'ciudad': ciudad,
                        'date': date,
                        'grupo': group,
                        'subgrupo': subgrupo,
                        'titulo': title,
                        'url': href,
                        'content': content
                    })
            else:
                if not update_existing:
                    filtered_urls.append({
                        'ciudad': ciudad,
                        'date': date,
                        'grupo': group,
                        'subgrupo': subgrupo,
                        'titulo': title,
                        'url': href,
                        'content': 'Error al descargar el archivo'
                    })
            break
        except requests.exceptions.ReadTimeout:
            print(f"Tiempo de espera agotado para la URL {href}. Intento {attempt + 1} de {RETRY_COUNT}")
            if attempt < RETRY_COUNT - 1:
                time.sleep(2)  # Esperar un poco antes de reintentar
                continue
        except requests.RequestException as e:
            print(f"Error al procesar la URL {href}: {e}")
            break

# Obtener y procesar URLs de PDFs desde el sitio web
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.content, 'html.parser')

filtered_urls_base = {}
for a_tag in soup.find_all('a', href=True):
    href = url_base + a_tag['href'] +'?lang=1'
    if '/detalle/' in href:
        group = a_tag.get_text(strip=True)
        filtered_urls_base.setdefault(group, []).append(href)
#print(f"La LISTA DE LAS PRIMERAS URL's SERIA: {filtered_urls_base}")

filtered_urls = []
filtered_urls_pdf = {}
for group, urls in filtered_urls_base.items():
    for base_url in urls:
        response = requests.get(base_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if ('/doc/' in href or '/bop/' in href) and is_pdf_content(href):
                title = a_tag.get_text(strip=True)
                subgrupo = a_tag.find_previous('div', class_='rotuloDetalleProc').get_text(strip=True) if a_tag.find_previous('div', class_='rotuloDetalleProc') else ''
                filtered_urls_pdf.setdefault(title, []).append(href)
                process_pdf_url(href, title, group, subgrupo)
                          
#print(f"La LISTA DE LAS URL's DE LOS PDF QUEDARIA ASÍ: {filtered_urls_pdf}")

# Insertar los datos en la tabla de MySQL
for row in filtered_urls:
    sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (row['ciudad'], row['date'], row['titulo'], row['grupo'], row['subgrupo'], row['url'], row['content'])
    try:
        mycursor.execute(sql, val)
        print(f"{row['titulo']} insertado en tabla.")
    except mysql.connector.Error as err:
        print(f"Error al insertar {row['titulo']}: {err}")

# Procesar URLs de PDFs específicos con OCR forzado
for pdf_url in specific_pdfs:
    process_pdf_url(pdf_url, 'Documento específico', 'General', '', update_existing=True, force_ocr=True)

mydb.commit()
print(f"{mycursor.rowcount} registro(s) insertado(s).")
