
# ------------------------------------------------------------------------------------------------------------------ #

from bs4 import BeautifulSoup
from datetime import datetime
from io import BytesIO
from random import randint

import fitz
import mysql.connector
import os
import requests
import subprocess
import time

# ------------------------------------------------------------------------------------------------------------------ #

mydb = mysql.connector.connect(
    host = "127.0.0.1",
    user = "justicio",
    password = "sanjavier208xx",
    database = "justicio_db"
)

mycursor = mydb.cursor()

ciudad = 'Murcia'
fecha = datetime.today().strftime('%Y-%m-%d')

# ------------------------------------------------------------------------------------------------------------------ #

def scrape_links_with_document(url, visited=None):

    if visited is None:
        visited = set()
    
    if url in visited:
        return []
    
    visited.add(url)

    print(f"\nVisiting => {url}")

    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=True)
        
        for link in links:

            href = link['href']

            if 'document' in href and href not in visited:
                full_url = href if href.startswith('http') else f"{url}/{href}"

                if ('.pdf' in full_url or '.zip' in full_url):

                    if full_url not in document_links:
                        document_links.append(full_url)
                        document_links_full.append({
                            'full_url': full_url,
                            'group': soup.find(class_='header-title').text,
                            'title': link.text
                        })

                        print(f"  Document found => {full_url}")
                else:
                    scrape_links_with_document(full_url, visited)

        return document_links_full
    
    except Exception as e:
        # Error!!!
        print(f"ERROR => Failed to process {url}: {e}")
        return []

# ------------------------------------------------------------------------------------------------------------------ #

def download_and_scrap_pdf(pdf):

    try:
        # download pdf
        response = requests.get(pdf.get('full_url'), headers=headers)

        if response.status_code == 200:

            # read pdf
            pdf_data = BytesIO(response.content)
            doc = fitz.open("pdf", pdf_data)

            # scrap content
            content  = ""
            for page in doc:
                content  += page.get_text()

            # insert in DB
            sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (ciudad, fecha, pdf['title'], pdf['group'], '', pdf['full_url'], content)

            mycursor.execute(sql, val)

            print(f"    => Inserted in DB!")

    except Exception as e:
        # Error!!!
        print(f"ERROR => Failed to insert {pdf}: {e}")
        return []

def open_and_scrap_pdf(url, pdf):

    try:
        # read pdf
        doc = fitz.open(pdf)

        # scrap content
        content  = ""
        for page in doc:
            content  += page.get_text()

        # insert in DB
        sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (ciudad, fecha, url['title'], url['group'], '', url['full_url'], content)

        mycursor.execute(sql, val)

        print(f"    => Inserted in DB!")

    except Exception as e:
        # Error!!!
        print(f"ERROR => Failed to insert {pdf}: {e}")
        return []

def scrape_pdfs_from_folder(url, folder_path):

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.pdf'):
                pdf_path = os.path.join(root, file)
                print(f"    Processing => {pdf_path}")
                open_and_scrap_pdf(url, pdf_path)

# ------------------------------------------------------------------------------------------------------------------ #

def unzip_with_7zip(url, zip_path, extract_to='.'):

    if not os.path.exists(extract_to):
        os.makedirs(extract_to)

    command = f'"C:\\Program Files\\7-Zip\\7z.exe" x "{zip_path}" -o"{extract_to}"'

    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if result.returncode == 0:
        print(f"  Extracted ZIP file to {extract_to}")
        scrape_pdfs_from_folder(url, extract_to)
    else:
        print(f"  Failed to extract ZIP file: {result.stderr.decode()}")

# ------------------------------------------------------------------------------------------------------------------ #

def download_and_unzip_zip(url, download_to='.'):

    r = requests.get(url['full_url'])

    zip_path = os.path.join(download_to, url['full_url'].rsplit('/', 1)[-1])
    
    with open(zip_path, 'wb') as f:
        f.write(r.content)
    
    unzip_with_7zip(url, zip_path, os.path.join(download_to, url['full_url'].rsplit('/', 1)[-1] + '_unzip'))
    
    os.remove(zip_path)

    print(f"  Downloaded and unzipped => {url['full_url']}\n")

# ------------------------------------------------------------------------------------------------------------------ #

# INIT & START

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

print('\n=> Spider in Action')

start_url = 'https://www.murcia.es/web/portal/ordenanzas'

download_to_folder = 'C:\\Users\\joseluisgx\\Desktop\\TEMP_ZIP'

document_links = []

document_links_full = []
document_links_full = scrape_links_with_document(start_url)

number_of_pdf_files = 0
number_of_zip_files = 0

# All PDFs files

print('\n=> Scrapping Files [PDFs]\n')

for pdf in document_links_full:

    if '.pdf' in pdf['full_url']:
        print(f"  Link => {pdf['full_url']}")
        download_and_scrap_pdf(pdf)
        number_of_pdf_files += 1

print(f'\n  {number_of_pdf_files} PDF files')

mydb.commit()

# All ZIPs files

print('\n=> Scrapping Files [ZIPs]\n')

for zip in document_links_full:

    if '.zip' in zip['full_url']:
        print(f"  Link => {zip['full_url']}")
        time.sleep(randint(1, 2))
        download_and_unzip_zip(zip, download_to_folder)
        number_of_zip_files += 1

print(f'  {number_of_zip_files} ZIP files')

mydb.commit()

# END

print('\n=> DONE !!!!!!')

# ------------------------------------------------------------------------------------------------------------------ #
