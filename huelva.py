"""
This script scrapes the huelva city council website to get the normative documents.
Tested with Python 3.12.4

Run local db:
    $ docker run --name justicio-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=justicio -d mysql:latest
Create table:
    $ docker exec -i justicio-mysql mysql -uroot -ppassword justicio < justicio.sql
Run script:
    $ python3 huelva.py


Exceptions:
    - Certificate for <www.huelva.es> is issued by FNMT, you may get an error like if you are not trusting the issuer certificate:
        ssl.SSLCertVerificationError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1000)
    - Data too long for column 'url'
        https://www.huelva.es/portal/sites/default/files/documentos/ordenanzas/fiscales/5.2_ordenanza_reguladora_de_la_prestacion_patrimonial_de_caracter_publico_no_tributario_por_prestacion_del_servicio_de_abastecimiento_de_agua_y_otros_derechos_economicos_por_actividades.pdf

"""
import pdftotext
from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime
import ssl
import mysql.connector

# Remove if you are trusting the FNMT root certificate
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Constants
HOST = "https://www.huelva.es"
BASE_URL = "https://www.huelva.es/portal/es/listado-documentos?page="
CITY = 'huelva'
DATE = datetime.today().strftime('%Y-%m-%d')

# Connect to the database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="justicio"
)
mycursor = mydb.cursor()

# Start scraping
pagenum = 0
processed = 0
while True:
    url = BASE_URL+str(pagenum)
    print("Processing page", pagenum, "|", url)
    page = urlopen(url, context=ctx)
    html = page.read()
    soup = BeautifulSoup(html, "html.parser")

    if soup.select_one('li.active a').text != str(pagenum+1):
        print("No more pages to process")
        break
    groups = soup.select(".view-grouping .view-grouping")

    for entry in groups:
        titletd = entry.select_one(".view-grouping-header p")
        if titletd is None:
            continue
        title = titletd.text.strip()
        for a in entry.select("a"):
            pdfurl = a.get('href')
            if pdfurl.endswith(".pdf"):
                if not pdfurl.startswith("http"):
                    pdfurl = HOST+pdfurl
                print("Loading PDF", pdfurl)
                pdfdata = urlopen(pdfurl, context=ctx)
                pdf = pdftotext.PDF(pdfdata)
                text = "\n\n".join(pdf)
                # The PDF scraped content has thousands of dashed lines, remove them
                text = text.replace("-----", "")
                sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                val = (CITY, DATE, title, '', '', pdfurl[:254], text)
                mycursor.execute(sql, val)
                print("Processed", title)
                processed += 1
    pagenum += 1

# Commit changes
mydb.commit()
print("Total processed", processed, "items")