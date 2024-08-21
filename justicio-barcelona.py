#!/usr/bin/env python3


import requests
from urllib.parse import urlparse, parse_qs
import time
import random
from bs4 import BeautifulSoup
from datetime import datetime
import json
import math
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


def create_working_tables_if_not_exists(conn):
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `normativa_barcelona_doc` (
        `id` int unsigned NOT NULL AUTO_INCREMENT,
        `status` tinyint(1) NOT NULL DEFAULT 1,
        `url` varchar(255) DEFAULT NULL,
        `group` varchar(255) DEFAULT NULL,
        `subgroup` varchar(255) DEFAULT NULL,
        `title` varchar(255) DEFAULT NULL,
        `response` int unsigned DEFAULT NULL,
        `url_from` varchar(255) DEFAULT NULL,
        `data` text DEFAULT NULL,
        PRIMARY KEY (`id`),
        UNIQUE KEY `url` (`url`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
    """

    cursor.execute(create_table_query)
    conn.commit()

    cursor.close()


def remove_working_tables_if_not_exists(conn):
    cursor = conn.cursor()

    cursor.execute(remove_table_query)
    conn.commit()

    remove_table_query = f"""
    DROP TABLE `normativa_barcelona_doc`;
    """

    cursor.execute(remove_table_query)
    conn.commit()

    cursor.close()


def add_doc(conn, url, group = '', subgroup = '', title = '', url_from = '', data = ''):
    cursor = conn.cursor()

    cursor.execute(f'INSERT IGNORE INTO `normativa_barcelona_doc` (`url`, `group`, `subgroup`, `title`, `url_from`, `data`) VALUES (%s, %s, %s, %s, %s, %s);', (url, group, subgroup, title, url_from, data))
    conn.commit()


def remove_doc(conn, detail_id, response = ''):
    cursor = conn.cursor()

    cursor.execute(f'UPDATE `normativa_barcelona_doc` SET `status` = 0, `response` = %s WHERE `id` = %s;', (response, detail_id))
    conn.commit()


def next_doc(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT * FROM `normativa_barcelona_doc` WHERE `status` = 1 ORDER BY `id` ASC LIMIT 1;')
    row = cursor.fetchone()
    if row:
        return row

    return None


def count_pending_docs(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT COUNT(`id`) as c FROM `normativa_barcelona_doc` WHERE `status` = 1;')
    row = cursor.fetchone()
    if row:
        return row['c']

    return 0


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
    create_working_tables_if_not_exists(conn)
    cursor = conn.cursor()


    location = 'Barcelona'
    date = datetime.today().strftime('%Y-%m-%d')


    # Serp pages
    next_page = 1
    total_pages = None
    while next_page is not None:
        page_url = f"https://ajuntament.barcelona.cat/norma-portal-juridic/es/search.json?product_id=WW&content_type=72&bcn_status=r06qhtmt115uo58&bypass_rabl=true&include=parent%2Cabstract%2Csnippet%2Cproperties_with_ids%2Ccitation_counts&per_page=10&page={next_page}&sort=date&include_local_exclusive=true&cbm=6.0%7C361.0%7C5.0%7C9.0%7C4.0%7C2.0%3D0.01%7C400.0%7C1.0%7C0.001%7C1.5%7C0.2&locale=es&hide_ct6=true&t=1722929840&type=document&locale=es&hide_ct6=true&t=1722929840"
        print(f"({next_page}/{total_pages if total_pages is not None else '-'}) Extracting documents from serps...")

        time.sleep(random.uniform(2, 5))
        response = requests.get(page_url, headers=get_default_headers())
        if response.status_code != 200:
            print(f"Error reading {page_url}... Response code: {response.status_code}")
            return
        if 'Request Rejected' in response.text:
            print(f"Error reading {page_url}... Request Rejected")
            return

        data = json.loads(response.text)

        if total_pages is None and data['count']:
            total_pages = math.ceil(data['count'] / 10)

        for item in data['results']:
            item_group = ''
            item_subgroup = ''
            item_title = ''
            
            # Only on force documents
            # - vacatio_legis -> Todavía no vigente
            # - on_force -> Vigente
            # - None
            if 'status' in item and item['status'] == 'on_force':
                if 'properties' in item:
                    for item_property in item['properties']:
                        if item_property['property']['label'] == 'Tipo de Documento':
                            item_group = item_property['values'][0]

                #item_url = f"https://ajuntament.barcelona.cat/norma-portal-juridic/es/vid/{item['id']}.json?include=abstract%2Cparent%2Cmeta%2Cformats%2Cchildren%2Cproperties_with_ids%2Clibrary%2Csource&fat=1&locale=es&hide_ct6=true&t=1722929840"
                #add_detail(conn, item_url, item_group, item_subgroup, item['title'], page_url, json.dumps(item))
                
                item_url = f"https://ajuntament.barcelona.cat/norma-portal-juridic/es/pdf_viewer/{item['id']}"
                add_doc(conn, item_url, item_group, item_subgroup, item['title'], page_url, json.dumps(item))


        if len(data['results']) >= 10:
            next_page += 1
        else:
            next_page = None
    
    
    # Download documents
    doc_info = next_doc(conn)
    while doc_info is not None:
        if doc_info['url'] is not None:
            page_url = doc_info['url']

            # Download document
            print(f"({count_pending_docs(conn)}) Download {doc_info['url']}")

            time.sleep(random.uniform(2, 5))
            response = requests.get(page_url, headers=get_default_headers())
            if response.status_code != 200:
                print(f"Error reading {page_url}... Response code: {response.status_code}")
                remove_doc(conn, doc_info['id'], response.status_code)
                doc_info = next_doc(conn)

                continue
            if 'Request Rejected' in response.text:
                print(f"Error reading {page_url}... Request Rejected")
                remove_doc(conn, doc_info['id'], response.status_code)
                doc_info = next_doc(conn)

                continue

            content = ''
            pdf_data = BytesIO(response.content)
            try:
                doc = fitz.open("pdf", pdf_data)
                for page in doc:
                    content += page.get_text()
            except Exception as e:
                print(f"Error trying to read document pages {page_url}: {e}")

            query = "INSERT INTO `normativa` (`ciudad`, `date`, `titulo`, `grupo`, `subgrupo`, `url`, `content`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (location, date, doc_info['title'], doc_info['group'], doc_info['subgroup'], page_url, content))
            conn.commit()

        remove_doc(conn, doc_info['id'], response.status_code)
        doc_info = next_doc(conn)

    
    cursor.close()
    remove_working_tables_if_not_exists(conn)


    # Close database connection
    conn.close()


    print('Done!')


if __name__ == "__main__":
    main()