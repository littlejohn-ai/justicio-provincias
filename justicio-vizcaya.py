#!/usr/bin/env python3


import requests
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


    location = 'Bilbao'
    date = datetime.today().strftime('%Y-%m-%d')

    pending_subgroups = []
    pending_docs = []


    page_url = 'https://www.bilbao.eus/cs/Satellite?c=Page&cid=3000009288&language=es&pageid=3000009288&pagename=Bilbaonet%2FPage%2FBIO_detallePagina'

    # Extract subgroups from index
    print("Extracting subgroups from index")
    
    response = requests.get(page_url, headers=get_default_headers())
    if response.status_code != 200:
        print(f"Error reading {page_url}... Response code: {response.status_code}")
    if 'Request Rejected' in response.text:
        print(f"Error reading {page_url}... Request Rejected")

    soup = BeautifulSoup(response.content, 'html.parser')

    for block in soup.find_all('div', class_='blq-menu'):
        block_title = block.find('h3')
        if block_title is not None:
            block_title = clean_string(block_title.text)

        for item in block.find_all('li'):
            item_a = item.find('a')
            if item_a is not None:
                if item_a.get('href').lower().endswith('.pdf'):
                    if not any(i['url'] == f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}" for i in pending_docs):
                        pending_docs.append({'url': f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}", 'group': block_title, 'subgroup': None, 'title': clean_string(item_a.text)})
                else:
                    if not any(i['url'] == f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}" for i in pending_subgroups):
                        pending_subgroups.append({'url': f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}", 'group': block_title, 'subgroup': clean_string(item_a.text)})
    

    subgroup_index = 0
    for subgroup in pending_subgroups:
        subgroup_index += 1

        if subgroup['url'] is not None:
            page_url = subgroup['url']

            # Extract documents from current subgroup
            print(f"({subgroup_index}/{len(pending_subgroups)}) Extracting document details from {subgroup['subgroup']}")

            time.sleep(random.uniform(20, 30))
            response = requests.get(page_url, headers=get_default_headers())
            if response.status_code != 200:
                print(f"Error reading {page_url}... Response code: {response.status_code}")
                continue
            if 'Request Rejected' in response.text:
                print(f"Error reading {page_url}... Request Rejected")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            item_list = soup.find('ul', class_='lista_ul_menu')
            if item_list is not None:
                for item in item_list.find_all('li'):
                    item_a = item.find('a')
                    if item_a is not None:
                        if 'Satellite' not in item_a.get('href') or 'blobcol' in item_a.get('href'):
                            if not any(i['url'] == f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}" for i in pending_docs):
                                pending_docs.append({'url': f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}", 'group': subgroup['group'], 'subgroup': subgroup['subgroup'], 'title': clean_string(item_a.text)})

                        else:
                            if not any(i['url'] == f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}" for i in pending_subgroups):
                                pending_subgroups.append({'url': f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}", 'group': subgroup['group'], 'subgroup': f"{subgroup['subgroup']} - {clean_string(item_a.text)}"})

            elements = soup.find('div', id='cont-readspeaker')
            if elements is not None:
                last_h3_title = None
                last_p_title = None
                for element in elements.find_all(recursive=False):
                    if element.name == 'h3':
                        last_h3_title = clean_string(element.text)
                    elif element.name == 'p':
                        last_p_title = clean_string(element.text)
                    elif element.name == 'ul':
                        for item in element.find_all('li'):
                            item_a = item.find('a')
                            if item_a is not None:
                                item_title = f"{last_h3_title + ' - ' if last_h3_title is not None else ''}{last_p_title + ' - ' if last_p_title is not None else ''}{clean_string(item_a.text)}"
                                
                                if 'Satellite' not in item_a.get('href') or 'blobcol' in item_a.get('href'):
                                    if not any(i['url'] == f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}" for i in pending_docs):
                                        pending_docs.append({'url': f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}", 'group': subgroup['group'], 'subgroup': subgroup['subgroup'], 'title': item_title})

                                else:
                                    if not any(i['url'] == f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}" for i in pending_subgroups):
                                        pending_subgroups.append({'url': f"{'https://www.bilbao.eus' if 'https' not in item_a.get('href') else ''}{item_a.get('href')}", 'group': subgroup['group'], 'subgroup': f"{subgroup['subgroup']} - {clean_string(item_a.text)}"})


    pending_docs_index = 0
    for document in pending_docs:
        pending_docs_index += 1

        if document['url'] is not None:
            # Download document
            print(f"({pending_docs_index}/{len(pending_docs)}) Downloading document {document['title']}")

            time.sleep(random.uniform(20, 30))
            response = requests.get(document['url'])

            if response.status_code != 200:
                print(f"Error downloading document {document['url']}: {response.status_code}")
                continue
            if 'Request Rejected' in response.text:
                print(f"Error downloading document {document['url']}: Request Rejected")
                continue

            # Documents in external websites
            if 'www.bilbao.eus' not in document['url']:
                if 'www.surbisa.eus' in document['url'] and not document['url'].lower().endswith('.pdf'):
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    page_content = soup.find('div', class_='page-content')
                    if page_content is not None:
                        for item in page_content.find_all('li'):
                            item_a = item.find('a')
                            if item_a is not None:
                                if not any(i['url'] == item_a.get('href') for i in pending_docs):
                                    pending_docs.append({'url': item_a.get('href'), 'group': document['group'], 'subgroup': document['subgroup'], 'title': f"{document['title']} - {clean_string(item_a.text)}"})
                    continue

                elif 'www.zorrotzaurre.com' in document['url'] and not document['url'].lower().endswith('.pdf'):
                    continue
                    

            content = ''
            pdf_data = BytesIO(response.content)
            try:
                doc = fitz.open("pdf", pdf_data)
                for page in doc:
                    content += page.get_text()
            except Exception as e:
                print(f"Error trying to read document pages {document['url']}: {e}")

            query = "INSERT INTO `normativa` (`ciudad`, `date`, `titulo`, `grupo`, `subgrupo`, `url`, `content`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (location, date, document['title'], document['group'], document['subgroup'], document['url'], content))
            conn.commit()


    # Close database connection
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()