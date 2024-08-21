#!/usr/bin/env python3


import requests
from urllib.parse import urlparse, parse_qs
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


    location = 'Santander'
    date = datetime.today().strftime('%Y-%m-%d')


    pending_serps = []
    pending_dps = []


    pending_serps.append('https://www.santander.es/ayuntamiento/gobierno-municipal/normativa-municipal')

    
    while len(pending_serps):
        page_url = pending_serps.pop(0)

        time.sleep(random.uniform(1, 5))
        response = requests.get(page_url, headers=get_default_headers())
        if response.status_code != 200:
            print(f"Error reading {page_url}... Response code: {response.status_code}")
            continue
        if 'Request Rejected' in response.text:
            print(f"Error reading {page_url}... Request Rejected")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')

        
        # Detect, from page 1, how many serps we have to track
        if 'page' not in page_url:
            print(f"Detecting how many serps we have to track")

            pagination = soup.find('ul', class_='pagination')
            if pagination is not None:
                pager_last = pagination.find('li', class_='pager-last')
                if pager_last is not None:
                    pager_last_a = pager_last.find('a')
                    if pager_last_a is not None:
                        pager_last_a_url = urlparse(f"{'https://www.santander.es' if 'https' not in pager_last_a.get('href') else ''}{pager_last_a.get('href')}")
                        pager_last_a_query_params = parse_qs(pager_last_a_url.query)
                        pager_last_n = pager_last_a_query_params.get('page', None)
                        if pager_last_n is not None:
                            for i in range(1, int(pager_last_n[0]) + 1):
                                pending_serps.append(f"https://www.santander.es/ayuntamiento/gobierno-municipal/normativa-municipal?page={i}")
        

        # Extract dps from current serp
        print(f"Extracting document detail pages from {page_url}")

        view = soup.find('div', class_='view-estructura-administrativa')
        if view is not None:
            view_content = view.find('div', class_='view-content')
            if view_content is not None:
                for item_list in view_content.find_all('div', class_='lista-agrupada'):
                    item_list_title = item_list.find('h3')
                    if item_list_title is not None:
                        item_list_title = clean_string(item_list_title.text)
                    
                    for item in item_list.find_all('li'):
                        item_a = item.find('a')
                        if item_a is not None:
                            pending_dps.append({'url': f"{'https://www.santander.es' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}", 'title': clean_string(item_a.text), 'group': item_list_title})
    

    for dp in pending_dps:
        if dp['url'] is not None:
            time.sleep(random.uniform(1, 5))
            response = requests.get(dp['url'], headers=get_default_headers())
            if response.status_code != 200:
                print(f"Error reading {dp['url']}... Response code: {response.status_code}")
                continue
            if 'Request Rejected' in response.text:
                print(f"Error reading {dp['url']}... Request Rejected")
                continue

            print(f"Analyzing document from {dp['url']}")
            dp_soup = BeautifulSoup(response.content, 'html.parser')
            for action_list in dp_soup.find_all('div', class_='acciones'):
                action_list_h4 = action_list.find('h4')
                if action_list_h4 is not None and clean_string(action_list_h4.text) == 'Documentación relacionada':
                    related_documents = action_list.find_all('li')
                    for related_document in related_documents:
                        related_document_a = related_document.find('a')
                        if related_document_a is not None:
                            related_document_url = f"https:{related_document_a.get('href')}"
                            related_document_title = clean_string(related_document_a.text)
                            content = ''

                            print(f"Document found! {related_document_url}")
                            if related_document_url.lower().endswith('.pdf'):
                                time.sleep(random.uniform(1, 5))
                                response = requests.get(related_document_url)
                                if response.status_code == 200:
                                    pdf_data = BytesIO(response.content)
                                    try:
                                        doc = fitz.open("pdf", pdf_data)
                                        for page in doc:
                                            content += page.get_text()
                                    except Exception as e:
                                        print(f"Error trying to read document pages {related_document_url}: {e}")
                                else:
                                    print(f"Error downloading document {related_document_url}: {response.status_code}")

                            query = "INSERT INTO `normativa` (`ciudad`, `date`, `titulo`, `grupo`, `subgrupo`, `url`, `content`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                            cursor.execute(query, (location, date, f"{dp['title']}{' - ' + related_document_title if len(related_documents) > 1 else ''}", dp['group'], None, related_document_url, content))

                            conn.commit()


    # Close database connection
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()