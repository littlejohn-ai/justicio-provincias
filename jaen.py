"""
This script scrapes the Jaen city council website to get the normative documents.
Tested with Python 3.12.4

Run local db:
    $ docker run --name justicio-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=justicio -d mysql:latest
Create table:
    $ docker exec -i justicio-mysql mysql -uroot -ppassword justicio < justicio.sql
Run script:
    $ python3 jaen.py

"""
import pdftotext
from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime
import mysql.connector

# Constants
BASE_URL = "https://sede.aytojaen.es/portal/sede/"
DOCS_URL = BASE_URL+"se_contenedor1.jsp?seccion=s_ldoc_d10_v1.jsp&codbusqueda=1255&language=es&codResi=1&layout=se_contenedor1.jsp&codAdirecto=266&numeroPagina="
CITY = 'Jaen'
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
pagenum = 1
processed = 0
while True:
    url = DOCS_URL+str(pagenum)
    print("Processing page", pagenum, "|", url)
    page = urlopen(url)
    html = page.read()
    soup = BeautifulSoup(html, "html.parser")

    if "No se han encontrado datos" in soup.text:
        break
    table = soup.select_one("#lista")

    for entry in table.find_all('tr'):
        titletd = entry.select_one("td:nth-of-type(1) a")
        if titletd is None:
            continue
        title = titletd.text.strip()
        pdfurl = entry.select_one("td:nth-of-type(3) a").get("href")
        if pdfurl.endswith(".pdf"):
            print("Loading PDF", BASE_URL+pdfurl)
            pdfdata = urlopen(BASE_URL+pdfurl)
            pdf = pdftotext.PDF(pdfdata)
            text = "\n\n".join(pdf)
            sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (CITY, DATE, title, '', '', BASE_URL+pdfurl, text)
            mycursor.execute(sql, val)
            print("Processed", title)
            processed += 1
    pagenum += 1

# Commit changes
mydb.commit()
print("Total processed", processed, "items")