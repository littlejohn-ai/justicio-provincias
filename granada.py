"""
This script scrapes the Granada city council website to get the normative documents.
Tested with Python 3.12.4

Run local db:
    $ docker run --name justicio-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=justicio -d mysql:latest
Create table:
    $ docker exec -i justicio-mysql mysql -uroot -ppassword justicio < justicio.sql
Run script:
    $ python3 granada.py

"""
import pdftotext
from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime
import mysql.connector

# Constants
HOST = "https://www.granada.org"
BASE_URL = "https://www.granada.org/inet/wordenanz.nsf/ww6!OpenView&Start="
CITY = 'Granada'
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
    url = BASE_URL+str(pagenum)
    print("Processing page", pagenum, "|", url)
    page = urlopen(url)
    html = page.read()
    soup = BeautifulSoup(html, "html.parser")

    if "No se ha hallado ningún documento" in soup.text:
        break

    for entry in soup.select('.tablatr td b a'):
        try:
            title = entry.text[0:150]
            if title == "":
                continue
            ref = entry.get("href")
            linkpage = urlopen(HOST+ref)
            linkhtml = linkpage.read()
            linksoup = BeautifulSoup(linkhtml, "html.parser")

            # Tipo:
            group = ""
            for td in linksoup.find_all('td'):
                if "Tipo: " in td.text:
                    tipo = td.text.replace("Tipo: ", "")
                    tipo = tipo.split(" ")[0]
                    group = tipo
                    break
            
            content = linksoup.select_one("#mibody").text
            sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (CITY, DATE, title, group, '', HOST+ref, content)
            mycursor.execute(sql, val)
            print("Processed", title)
            processed += 1
        except Exception as e:
            print("Error when processing", entry.text, ":", e)
    pagenum += 1

# Commit changes
mydb.commit()
print("Total processed", processed, "items")