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


def create_working_tables_if_not_exists(conn):
    cursor = conn.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `normativa_alava_detail` (
        `id` int unsigned NOT NULL AUTO_INCREMENT,
        `status` tinyint(1) NOT NULL DEFAULT 1,
        `url` varchar(255) DEFAULT NULL,
        `group` varchar(255) DEFAULT NULL,
        `subgroup` varchar(255) DEFAULT NULL,
        `title` varchar(255) DEFAULT NULL,
        `response` int unsigned DEFAULT NULL,
        `url_from` varchar(255) DEFAULT NULL,
        PRIMARY KEY (`id`),
        UNIQUE KEY `url` (`url`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
    """

    cursor.execute(create_table_query)
    conn.commit()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `normativa_alava_doc` (
        `id` int unsigned NOT NULL AUTO_INCREMENT,
        `status` tinyint(1) NOT NULL DEFAULT 1,
        `url` varchar(255) DEFAULT NULL,
        `group` varchar(255) DEFAULT NULL,
        `subgroup` varchar(255) DEFAULT NULL,
        `title` varchar(255) DEFAULT NULL,
        `response` int unsigned DEFAULT NULL,
        `url_from` varchar(255) DEFAULT NULL,
        PRIMARY KEY (`id`),
        UNIQUE KEY `url` (`url`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
    """

    cursor.execute(create_table_query)
    conn.commit()

    cursor.close()


def remove_working_tables_if_not_exists(conn):
    cursor = conn.cursor()

    remove_table_query = f"""
    DROP TABLE `normativa_alava_detail`;
    """

    cursor.execute(remove_table_query)
    conn.commit()

    remove_table_query = f"""
    DROP TABLE `normativa_alava_doc`;
    """

    cursor.execute(remove_table_query)
    conn.commit()

    cursor.close()


def add_detail(conn, url, group = '', subgroup = '', title = '', url_from = ''):
    cursor = conn.cursor()

    cursor.execute(f'INSERT IGNORE INTO `normativa_alava_detail` (`url`, `group`, `subgroup`, `title`, `url_from`) VALUES (%s, %s, %s, %s, %s);', (url, subgroup, group, title, url_from))
    conn.commit()


def remove_detail(conn, detail_id, response = ''):
    cursor = conn.cursor()

    cursor.execute(f'UPDATE `normativa_alava_detail` SET `status` = 0, `response` = %s WHERE `id` = %s;', (response, detail_id))
    conn.commit()


def next_detail(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT * FROM `normativa_alava_detail` WHERE `status` = 1 ORDER BY `id` ASC LIMIT 1;')
    row = cursor.fetchone()
    if row:
        return row

    return None


def count_pending_details(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT COUNT(`id`) as c FROM `normativa_alava_detail` WHERE `status` = 1;')
    row = cursor.fetchone()
    if row:
        return row['c']

    return 0


def add_doc(conn, url, group = '', subgroup = '', title = '', url_from = ''):
    cursor = conn.cursor()

    cursor.execute(f'INSERT IGNORE INTO `normativa_alava_doc` (`url`, `group`, `subgroup`, `title`, `url_from`) VALUES (%s, %s, %s, %s, %s);', (url, subgroup, group, title, url_from))
    conn.commit()


def remove_doc(conn, detail_id, response = ''):
    cursor = conn.cursor()

    cursor.execute(f'UPDATE `normativa_alava_doc` SET `status` = 0, `response` = %s WHERE `id` = %s;', (response, detail_id))
    conn.commit()


def next_doc(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT * FROM `normativa_alava_doc` WHERE `status` = 1 ORDER BY `id` ASC LIMIT 1;')
    row = cursor.fetchone()
    if row:
        return row

    return None


def count_pending_docs(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT COUNT(`id`) as c FROM `normativa_alava_doc` WHERE `status` = 1;')
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


    location = 'Vitoria'
    date = datetime.today().strftime('%Y-%m-%d')


    avoid_urls_patterns = ['mailto', 'contenedorEditorialAction', 'tel:', 'Documents and Settings', 'geovitoria', 'sedeelectronica.vitoria-gasteiz.org', 'twitter', 'g.page', 'facebook', 'goo.gl', 'play.google.com', 'euskotren', 'youtube', 'wa.me', 'autobuseslaunion', 'flickr', 'fundacionvital', 'instagram', 'izenpe.eus', 'kzgunea.eus', 'svisual.org', 'api.whatsapp.com', 'blogs.vitoria-gasteiz.org', 'comentarioAction', 'areaAction', 'we001Action']


    pending_serps = []


    if count_pending_details(conn) <= 0:
        # Current regulations
        pending_serps.append({'url': 'https://www.vitoria-gasteiz.org/we001/was/we001Action.do?idioma=es&accion=arbolNormativas&accionWe001=ficha&normativaVigente=true'})

        # Non-current regulations
        #pending_serps.append({'url': 'https://www.vitoria-gasteiz.org/we001/was/we001Action.do?idioma=es&accionWe001=ficha&accion=arbolNormativas&normativaVigente=false'})


    serps_index = 0
    for serp in pending_serps:
        serps_index += 1

        if serp['url'] is not None:
            page_url = serp['url']

            # Extract detail pages and document urls from current serp
            print(f"({serps_index}/{len(pending_serps)}) Extracting detail pages and document urls from {serp['url']}")

            time.sleep(random.uniform(5, 10))
            response = requests.get(page_url, headers=get_default_headers())
            if response.status_code != 200:
                print(f"Error reading {page_url}... Response code: {response.status_code}")
                continue
            if 'Request Rejected' in response.text:
                print(f"Error reading {page_url}... Request Rejected")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            elements = soup.find('div', class_='main__body')
            if elements is not None:
                last_h2_title = None
                last_h3_title = None
                for element in elements.find_all(recursive=False):
                    if element.name == 'h2':
                        last_h2_title = clean_string(element.text)
                    if element.name == 'h3':
                        last_h3_title = clean_string(element.text)
                    elif element.name == 'ul':
                        for item in element.find_all('li'):
                            item_a = item.find('a')
                            if item_a is not None:
                                item_a_url = f"{'https://www.vitoria-gasteiz.org' if 'http' not in item_a.get('href') else ''}{item_a.get('href')}"
                                item_a_group = last_h2_title if last_h2_title is not None else ''
                                item_a_subgroup = last_h3_title if last_h3_title is not None else ''
                                
                                if item_a_url.lower().endswith('.pdf'):
                                    add_doc(conn, item_a_url, item_a_group, item_a_subgroup, clean_string(item_a.text), page_url)
                                elif all(pattern.lower() not in item_a_url.lower() for pattern in avoid_urls_patterns):
                                    add_detail(conn, item_a_url, item_a_group, item_a_subgroup, clean_string(item_a.text), page_url)

    
    detail = next_detail(conn)
    while detail is not None:
        if detail['url'] is not None:
            page_url = detail['url']

            # Extract document urls from current detail
            print(f"({count_pending_details(conn)}) Extracting document urls from {detail['url']}")

            time.sleep(random.uniform(5, 10))
            response = requests.get(page_url, headers=get_default_headers())
            if response.status_code not in [200, 410]:
                print(f"Error reading {page_url}... Response code: {response.status_code}")
                remove_detail(conn, detail['id'], response.status_code)
                detail = next_detail(conn)

                continue

            if 'Request Rejected' in response.text:
                print(f"Error reading {page_url}... Request Rejected")
                remove_detail(conn, detail['id'], response.status_code)
                detail = next_detail(conn)

                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            all_links = soup.select('div.main a')
            related_links = soup.select('div.main nav#relatedInformation a')

            for item_a in all_links:
                if item_a in related_links:
                    continue

                item_a_url = f"{'https://www.vitoria-gasteiz.org' if 'http' not in item_a.get('href') else ''}{item_a.get('href')}"

                if item_a_url.lower().endswith('.pdf'):
                    add_doc(conn, item_a_url, detail['group'], detail['subgroup'], f"{detail['title']} - {clean_string(item_a.text)}", page_url)
                elif clean_string(item_a.text) == 'Buzón Ciudadano.':
                    continue
                # Only links to direct documents to avoid navigating through non-current regulations
                #elif all(pattern.lower() not in item_a_url.lower() for pattern in avoid_urls_patterns):
                #    add_detail(conn, item_a_url, detail['group'], detail['subgroup'], f"{detail['title']} - {clean_string(item_a.text)}", page_url)

        remove_detail(conn, detail['id'], response.status_code)
        detail = next_detail(conn)
        

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