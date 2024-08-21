"""
This script scrapes the Almería city council website to get the normative documents.
Tested with Python 3.12.4

Run local db:
    $ docker run --name justicio-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=justicio -d mysql:latest
Create table:
    $ docker exec -i justicio-mysql mysql -uroot -ppassword justicio < justicio.sql
Run script:
    $ python3 almeria.py

Exceptions:
-   This one is a sharepoint
    https://almeriaciudad.es/normas-municipales/plan-general-de-ordenacion-urbana-de-almeria-1998
"""
import pdftotext
from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime
import mysql.connector

# Constants
BASE_URL = "https://almeriaciudad.es"
CITY = 'Almeria'
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
    page = urlopen(BASE_URL+'/normas-municipales?page='+str(pagenum))
    html = page.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")

    sections = soup.find_all("h3")
    if not sections:
        print("No more pages")
        break

    for group in sections:
        groupname = group.text
        for link in group.parent.find_all("a"):
            try:
                title = link.text
                linkpage = urlopen(BASE_URL+link.get("href"))
                linkhtml = linkpage.read().decode("utf-8")
                linksoup = BeautifulSoup(linkhtml, "html.parser")
                pdfelement = linksoup.select_one('a[type="application/pdf"]')
                pdfurl = pdfelement.get("href")
                if pdfurl.endswith(".pdf"):
                    pdfdata = urlopen(BASE_URL+pdfurl)
                    pdf = pdftotext.PDF(pdfdata)
                    text = "\n\n".join(pdf)
                    sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    val = (CITY, DATE, title, groupname, '', BASE_URL+pdfurl, text)
                    mycursor.execute(sql, val)
                    print("Processed", title, "in", groupname)
                    processed += 1
            except Exception as e:
                print("Error when processing", link.get("href"), ":", e)

    pagenum += 1

# Commit changes
mydb.commit()
print("Total processed", processed, "items")