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
    CREATE TABLE IF NOT EXISTS `normativa_madrid_topic` (
        `id` int unsigned NOT NULL AUTO_INCREMENT,
        `status` tinyint(1) NOT NULL DEFAULT 1,
        `key` varchar(64) DEFAULT NULL,
        `title` varchar(255) DEFAULT NULL,
        `response` int unsigned DEFAULT NULL,
        PRIMARY KEY (`id`),
        UNIQUE KEY `key` (`key`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
    """

    cursor.execute(create_table_query)
    conn.commit()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `normativa_madrid_detail` (
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
    CREATE TABLE IF NOT EXISTS `normativa_madrid_doc` (
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
    DROP TABLE `normativa_madrid_topic`;
    """

    cursor.execute(remove_table_query)
    conn.commit()

    remove_table_query = f"""
    DROP TABLE `normativa_madrid_detail`;
    """

    cursor.execute(remove_table_query)
    conn.commit()

    remove_table_query = f"""
    DROP TABLE `normativa_madrid_doc`;
    """

    cursor.execute(remove_table_query)
    conn.commit()

    cursor.close()


def add_topic(conn, key, title = ''):
    cursor = conn.cursor()

    cursor.execute(f'INSERT IGNORE INTO `normativa_madrid_topic` (`key`, `title`) VALUES (%s, %s);', (key, title))
    conn.commit()


def remove_topic(conn, topic_id, response = ''):
    cursor = conn.cursor()

    cursor.execute(f'UPDATE `normativa_madrid_topic` SET `status` = 0, `response` = %s WHERE `id` = %s;', (response, topic_id))
    conn.commit()


def next_topic(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT * FROM `normativa_madrid_topic` WHERE `status` = 1 ORDER BY `id` ASC LIMIT 1;')
    row = cursor.fetchone()
    if row:
        return row

    return None


def count_pending_topics(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT COUNT(`id`) as c FROM `normativa_madrid_topic` WHERE `status` = 1;')
    row = cursor.fetchone()
    if row:
        return row['c']

    return 0


def add_detail(conn, url, group = '', subgroup = '', title = '', url_from = ''):
    cursor = conn.cursor()

    cursor.execute(f'INSERT IGNORE INTO `normativa_madrid_detail` (`url`, `group`, `subgroup`, `title`, `url_from`) VALUES (%s, %s, %s, %s, %s);', (url, group, subgroup, title, url_from))
    conn.commit()


def remove_detail(conn, detail_id, response = ''):
    cursor = conn.cursor()

    cursor.execute(f'UPDATE `normativa_madrid_detail` SET `status` = 0, `response` = %s WHERE `id` = %s;', (response, detail_id))
    conn.commit()


def next_detail(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT * FROM `normativa_madrid_detail` WHERE `status` = 1 ORDER BY `id` ASC LIMIT 1;')
    row = cursor.fetchone()
    if row:
        return row

    return None


def count_pending_details(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT COUNT(`id`) as c FROM `normativa_madrid_detail` WHERE `status` = 1;')
    row = cursor.fetchone()
    if row:
        return row['c']

    return 0


def add_doc(conn, url, group = '', subgroup = '', title = '', url_from = ''):
    cursor = conn.cursor()

    cursor.execute(f'INSERT IGNORE INTO `normativa_madrid_doc` (`url`, `group`, `subgroup`, `title`, `url_from`) VALUES (%s, %s, %s, %s, %s);', (url, group, subgroup, title, url_from))
    conn.commit()


def remove_doc(conn, detail_id, response = ''):
    cursor = conn.cursor()

    cursor.execute(f'UPDATE `normativa_madrid_doc` SET `status` = 0, `response` = %s WHERE `id` = %s;', (response, detail_id))
    conn.commit()


def next_doc(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT * FROM `normativa_madrid_doc` WHERE `status` = 1 ORDER BY `id` ASC LIMIT 1;')
    row = cursor.fetchone()
    if row:
        return row

    return None


def count_pending_docs(conn):
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f'SELECT COUNT(`id`) as c FROM `normativa_madrid_doc` WHERE `status` = 1;')
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


    location = 'Madrid'
    date = datetime.today().strftime('%Y-%m-%d')

    cibelex_url = 'https://sede.madrid.es/portal/site/tramites/menuitem.274ba9620d68b4f560c44cf5a8a409a0/?vgnextoid=6b3d814231ede410VgnVCM1000000b205a0aRCRD&vgnextchannel=6b3d814231ede410VgnVCM1000000b205a0aRCRD'

    
    # Extract topics from Cibelex index
    page_url = f"{cibelex_url}&vgnextfmt=default"
    print(f"Extracting topics from Cibelex index...")

    time.sleep(random.uniform(2, 5))
    response = requests.get(page_url, headers=get_default_headers())
    if response.status_code != 200:
        print(f"Error reading {page_url}... Response code: {response.status_code}")
        return
    if 'Request Rejected' in response.text:
        print(f"Error reading {page_url}... Request Rejected")
        return

    soup = BeautifulSoup(response.content, 'html.parser')

    topics_selector = soup.find('select', id='tema')
    for topics_selector_option in topics_selector.find_all('option'):
        if topics_selector_option['value'] != '':
            add_topic(conn, topics_selector_option['value'], topics_selector_option.text)

    
    # Extract detail pages from topics serps
    topic = next_topic(conn)
    while topic is not None:
        if topic['key'] is not None:
            topic_page_n = 0
            item_group = topic['title']

            topic_serp_url = f"{cibelex_url}&buscar=true&buscador.normativa.texto1=&optexto1=dentrotitulo&disposicion=&tema={topic['key']}&marginal2=&marginal3=&vigente=vigente&desde=&hasta=&orden=fecha_aprobacion&validaDatos=marginal2%7Enum%C3%A9rico%7EesEntero%7Eopcional%23marginal3%7Enum%C3%A9rico%7EesEntero%7Eopcional"

            while topic_serp_url is not None:
                # Extract document urls from current topic
                print(f"({count_pending_topics(conn)}) Extracting detail pages from {topic['title']} (page {(topic_page_n + 1)})")

                time.sleep(random.uniform(2, 5))
                response = requests.get(topic_serp_url, headers=get_default_headers())
                if response.status_code not in [200, 410]:
                    print(f"Error reading {topic_serp_url}... Response code: {response.status_code}")
                    remove_topic(conn, topic['id'], response.status_code)
                    topic = next_topic(conn)

                    continue

                if 'Request Rejected' in response.text:
                    print(f"Error reading {topic_serp_url}... Request Rejected")
                    remove_topic(conn, topic['id'], response.status_code)
                    topic = next_topic(conn)

                    continue

                soup_serp = BeautifulSoup(response.content, 'html.parser')

                items = soup_serp.select('ul.events-results > li')
                for item in items:
                    item_type = None
                    item_url = None
                    item_subgroup = None
                    item_title = None

                    item_type_p = item.find('p', class_='event-type')
                    if item_type_p is not None:
                        item_type = clean_string(item_type_p.text)

                    item_a = item.find('a', class_='event-link')
                    if item_a is not None:
                        item_url = f"{'https://sede.madrid.es' if 'http' not in item_a.get('href') else ''}{item_a.get('href')}"
                        item_title = f"{item_type} - {clean_string(item_a.text)}" if item_type is not None else clean_string(item_a.text)

                    item_info_lis = item.select('ul.event-list > li')
                    for item_info_li in item_info_lis:
                        item_info_li_span = item_info_li.find('span', class_='event-intro')
                        if item_info_li_span is not None:
                            item_info_li_span_strong = item_info_li_span.find('strong')
                            if item_info_li_span_strong is not None:
                                item_info_type = clean_string(item_info_li_span_strong.text)
                                if item_info_type == 'Tipo de disposición:':
                                    item_subgroup = clean_string(item_info_li_span.text.replace(item_info_type, ''))
                                #elif item_info_type == 'Fecha de disposición:':
                                #    pass

                    if item_url is not None:
                        add_detail(conn, item_url, item_group, item_subgroup, item_title, topic_serp_url)
                
                pagination_next = soup_serp.select_one('ul.pagination > li.next > a.pagination-text')
                if pagination_next is not None:
                    topic_page_n = topic_page_n + 1
                    topic_serp_url = f"{'https://sede.madrid.es' if 'http' not in pagination_next.get('href') else ''}{pagination_next.get('href')}"
                else:
                    topic_serp_url = None

        remove_topic(conn, topic['id'], response.status_code)
        topic = next_topic(conn)

    
    # Extract document urls from detail pages
    detail = next_detail(conn)
    while detail is not None:
        if detail['url'] is not None:
            page_url = detail['url']

            # Extract document urls from current detail
            print(f"({count_pending_details(conn)}) Extracting documents from {detail['url']}")

            time.sleep(random.uniform(2, 5))
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

            items = soup.select('div.asociada-cont:first-child ul.asociada-list > li.asociada-item > a.asociada-link.ico-pdf')
            for item_a in items:
                item_url = f"{'https://sede.madrid.es' if 'http' not in item_a.get('href') else ''}{item_a.get('href')}"

                add_doc(conn, item_url, detail['group'], detail['subgroup'], detail['title'], detail['url'])
        
        remove_detail(conn, detail['id'], response.status_code)
        detail = next_detail(conn)


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