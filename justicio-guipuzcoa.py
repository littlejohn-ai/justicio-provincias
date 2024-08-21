#!/usr/bin/env python3


import requests
from urllib.parse import urlparse, urlunparse, parse_qs
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime
import fitz
from io import BytesIO
import mysql.connector


def create_table_if_not_exists(conn):
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `normativa` (
        `id` int unsigned NOT NULL AUTO_INCREMENT,
        `ciudad` varchar(100) DEFAULT NULL,
        `date` date DEFAULT NULL,
        `titulo` varchar(255) DEFAULT NULL,
        `grupo` varchar(255) DEFAULT NULL,
        `subgrupo` varchar(255) DEFAULT NULL,
        `url` varchar(255) DEFAULT NULL,
        `content` longtext,
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
    """

    cursor.execute(create_table_query)
    conn.commit()

    cursor.close()


def get_default_headers():
    return {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8,fr;q=0.7',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Cache-Control': 'no-cache',
        'Referer': 'https://www.google.com/',
        'Sec-Ch-Ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
    }


def clean_string(s):
    if s is not None and isinstance(s, str):
        return s.replace('\t', '').replace('\n', '').strip()
    return s


def main():
    # Database connection
    conn = mysql.connector.connect(
        host='localhost',
        user='xxx',
        password='xxx',
        database='xxx'
    )

    create_table_if_not_exists(conn)
    cursor = conn.cursor()


    location = 'San Sebastián'
    date = datetime.today().strftime('%Y-%m-%d')

    pending_areas = []
    pending_subjects = []
    pending_docs = []


    page_url = 'https://www.donostia.eus/secretaria/NorMunicipal.nsf/frmWeb?ReadForm&sf=1&id=C671670436837&idioma=cas'

    # Extract areas from index
    print("Extracting areas from index")
    
    response = requests.get(page_url, headers=get_default_headers())
    if response.status_code != 200:
        print(f"Error reading {page_url}... Response code: {response.status_code}")
    if 'Request Rejected' in response.text:
        print(f"Error reading {page_url}... Request Rejected")

    soup = BeautifulSoup(response.content, 'html.parser')

    for item_list in soup.select('div.row-fluid.no-padding ul.unstyled'):
        for item in item_list.find_all('li'):
            item_a = item.find('a')
            if item_a is not None:
                if not any(i['url'] == item_a.get('href') for i in pending_areas):
                    area_url = urlparse(item_a.get('href'))
                    area_query_params = parse_qs(area_url.query)
                    area_type = area_query_params.get('tipo', [None])[0]

                    pending_areas.append({'url': f"{'https://www.donostia.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}", 'group': area_type.title() if area_type is not None else None, 'area': clean_string(item_a.text)})


    area_index = 0
    for area in pending_areas:
        area_index += 1

        if area['url'] is not None:
            page_url = area['url']

            # Extract subjects from current area
            print(f"({area_index}/{len(pending_areas)}) Extracting subjects from {area['area']}")

            time.sleep(random.uniform(1, 5))
            response = requests.get(page_url, headers=get_default_headers())
            if response.status_code != 200:
                print(f"Error reading {page_url}... Response code: {response.status_code}")
                continue
            if 'Request Rejected' in response.text:
                print(f"Error reading {page_url}... Request Rejected")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            for item_list in soup.select('div.row-fluid.no-padding ul.unstyled ul'):
                for item in item_list.find_all('li'):
                    item_a = item.find('a')
                    if item_a is not None:
                        if not any(i['url'] == item_a.get('href') for i in pending_subjects):
                            pending_subjects.append({'url': f"{'https://www.donostia.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}", 'group': area['group'], 'area': area['area'], 'subject': clean_string(item_a.text)})
        

    subject_index = 0
    for subject in pending_subjects:
        subject_index += 1

        if subject['url'] is not None:
            page_url = subject['url']

            # Extract document pages from current subject
            print(f"({subject_index}/{len(pending_subjects)}) Extracting documents from {subject['area']} - {subject['subject']}")

            time.sleep(random.uniform(1, 5))
            response = requests.get(page_url, headers=get_default_headers())
            if response.status_code != 200:
                print(f"Error reading {page_url}... Response code: {response.status_code}")
                continue
            if 'Request Rejected' in response.text:
                print(f"Error reading {page_url}... Request Rejected")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            for item_list in soup.select('div.row-fluid.no-padding ul.unstyled ul ul'):
                for item in item_list.find_all('li'):
                    item_a = item.find('a')
                    if item_a is not None:
                        if not any(i['url'] == item_a.get('href') for i in pending_subjects):
                            pending_docs.append({'url': f"{'https://www.donostia.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}", 'group': area['group'], 'area': subject['area'], 'subject': subject['subject'], 'title': clean_string(item_a.text)})
        

    docs_index = 0
    for doc_page in pending_docs:
        docs_index += 1

        if doc_page['url'] is not None:
            page_url = doc_page['url']

            # Extract document url from current document page
            print(f"({docs_index}/{len(pending_docs)}) Getting document from {doc_page['area']} - {doc_page['subject']} - {doc_page['title']}")

            time.sleep(random.uniform(5, 10))
            response = requests.get(page_url, headers=get_default_headers())
            if response.status_code != 200:
                print(f"Error reading {page_url}... Response code: {response.status_code}")
                continue
            if 'Request Rejected' in response.text:
                print(f"Error reading {page_url}... Request Rejected")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            for table_row in soup.select('div.row-fluid.no-padding table.table tr'):
                table_row_th = table_row.select_one('th')
                table_row_th_title = clean_string(table_row_th.text).replace(':', '')

                for item_a in table_row.select('a'):
                    item_a_title = clean_string(item_a.text)

                    if item_a_title == 'Volver':
                        continue

                    if item_a.get('href').lower().endswith('pdf'):
                        # Create document url to download
                        doc_page_url = urlparse(page_url)
                        path_segments = doc_page_url.path.rstrip('/').split('/')
                        if len(path_segments) > 1:
                            path_segments.pop()
                        else:
                            path_segments = ['']
                        
                        document_url = f"https://www.donostia.eus{'/'.join(path_segments)}/{item_a.get('href')}"
                        print(f"- Downloading {document_url}")

                        time.sleep(random.uniform(1, 5))
                        response = requests.get(document_url)

                        if response.status_code != 200:
                            print(f"- Error: {response.status_code}")
                            continue
                        if 'Request Rejected' in response.text:
                            print(f"- Error: Request Rejected")
                            continue


                        content = ''
                        pdf_data = BytesIO(response.content)
                        try:
                            doc = fitz.open("pdf", pdf_data)
                            for page in doc:
                                content += page.get_text()
                        except Exception as e:
                            print(f"Error trying to read document pages {document_url}: {e}")

                        query = "INSERT INTO `normativa` (`ciudad`, `date`, `titulo`, `grupo`, `subgrupo`, `url`, `content`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(query, (location, date, f"{doc_page['title']} - {table_row_th_title}", doc_page['group'], f"{doc_page['area']} - {doc_page['subject']}", document_url, content))
                        conn.commit()

                    else:
                        print(f"- Error getting document url")


    # Close database connection
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()