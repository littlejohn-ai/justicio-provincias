#!/usr/bin/env python3


import requests
import re
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


    url = 'https://logrono.es/normativa-municipal?delta=1000'
    location = 'Logroño'
    date = datetime.today().strftime('%Y-%m-%d')

    response = requests.get(url, headers=get_default_headers())

    soup = BeautifulSoup(response.content, 'html.parser')


    # Find available taxonomies to classify items
    taxonomies = {}
    for taxonomy in soup.find_all('div', class_='portlet-category-facet'):
        taxonomy_title = taxonomy.find('span', class_='panel-title')
        if taxonomy_title is not None:
            taxonomy_title = taxonomy_title.text.strip()
            if taxonomy_title in ['Tipo de normativa', 'Tema']:
                taxonomies[taxonomy_title] = {}

                for term in taxonomy.find_all('div', class_='tree-item-category'):
                    term_title = term.find('span', class_='custom-control-label-text')
                    term_id = term.find('input', class_='facet-term')

                    if term_title is not None and term_id is not None:
                        term_title = re.sub(r'\(\d+\)', '', clean_string(term_title.text))
                        term_id = term_id.get('data-term-id')

                        taxonomies[taxonomy_title][term_title] = []

                        if term_id is not None:
                            response = requests.get(f"https://logrono.es/normativa-municipal?delta=1000&category={term_id}", headers=get_default_headers())
                            soup_term = BeautifulSoup(response.content, 'html.parser')

                            for list_item in soup_term.find_all('div', class_='detalle-normativa'):
                                item_h2 = list_item.find('h2', class_='titulo')
                                title = clean_string(item_h2.text) if item_h2 is not None else None

                                if title is not None:
                                    taxonomies[taxonomy_title][term_title].append(title)


    for list_item in soup.find_all('div', class_='detalle-normativa'):
        item_h2 = list_item.find('h2', class_='titulo')
        title = clean_string(item_h2.text) if item_h2 is not None else None


        group = None
        subgroup = None
        for category in taxonomies['Tipo de normativa']:
            if title in taxonomies['Tipo de normativa'][category]:
                group = category
                break;
        for category in taxonomies['Tema']:
            if title in taxonomies['Tema'][category]:
                subgroup = category
                break;
        
        
        item_documents = list_item.find('div', class_='documentos-relacionados')
        if item_documents is not None:
            item_documents_a = item_documents.find_all('a')
            for item_document_a in item_documents_a:
                item_documents_a_title = item_document_a.find('span', class_='title')
                if item_documents_a_title is not None:
                    item_documents_a_title = clean_string(item_documents_a_title.text)

                url = f"https://logrono.es{item_document_a.get('href')}"
                content = ''

                if item_document_a.get('data-type') is not None and item_document_a.get('data-type') == 'pdf':
                    response = requests.get(url)
                    if response.status_code == 200:
                        pdf_data = BytesIO(response.content)
                        try:
                            doc = fitz.open("pdf", pdf_data)
                            for page in doc:
                                content  += page.get_text()
                        except Exception as e:
                            print(f"Error trying to read document pages {url}: {e}")
                    else:
                        print(f"Error downloading document {url}: {response.status_code}")

                query = "INSERT INTO `normativa` (`ciudad`, `date`, `titulo`, `grupo`, `subgrupo`, `url`, `content`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(query, (location, date, f"{title}{' - ' + item_documents_a_title if len(item_documents_a) > 1 else ''}", group, subgroup, url, content))


    conn.commit()


    # Close database connection
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()