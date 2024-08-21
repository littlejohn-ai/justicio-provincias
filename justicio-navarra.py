#!/usr/bin/env python3


import requests
from urllib.parse import urlsplit, urlunsplit
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


def remove_anchor(url):
    parsed_url = urlsplit(url)
    url_without_anchor = urlunsplit((parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.query, ''))
    return url_without_anchor


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


    location = 'Pamplona'
    date = datetime.today().strftime('%Y-%m-%d')


    pending_jobs = []
    done_jobs = []


    # Ordenanzas y reglamentos
    pending_jobs.append('https://www.pamplona.es/ayuntamiento/normativa/ordenanzas-y-reglamentos-municipales')

    # Ordenanzas y normas fiscales
    pending_jobs.append('https://www.pamplona.es/ayuntamiento/normativa/ordenanzas-y-normas-fiscales-2024')


    while len(pending_jobs):
        job = pending_jobs.pop(0)
        done_jobs.append(job)
        print(f"Current job: {job}")
        

        response = requests.get(job, headers=get_default_headers())
        if response.status_code != 200:
            continue
            
        soup = BeautifulSoup(response.content, 'html.parser')


        # Search sidebar links to subgroup documents
        content_wrapper = soup.find('div', class_='layout--twocol-30-70')
        if content_wrapper is not None:
            sidebar = content_wrapper.find('div', class_='layout__region--first')
            if sidebar is not None:
                for list_item in sidebar.find_all('li'):
                    list_item_a = list_item.find('a')
                    if list_item_a is not None:
                        url = remove_anchor(f"{'https://www.pamplona.es' if 'https' not in list_item_a.get('href') else ''}{list_item_a.get('href')}")

                        if url not in pending_jobs and url not in done_jobs and url not in job:
                            pending_jobs.append(url)


        # Search documents
        for section in soup.find_all('section', class_='block-inline-blockblock-list-documents'):
            section_h2 = section.find('h2', class_='block-title')

            if 'ordenanzas-y-normas-fiscales' in job:
                group = 'Ordenanzas y normas fiscales'
                subgroup = clean_string(section_h2.text) if section_h2 is not None else None
            else:
                group = clean_string(section_h2.text) if section_h2 is not None else None
                subgroup = None        
            
            for list_item in section.find_all('li', class_='field--item'):
                list_item_a = list_item.find('a')
                if list_item_a is not None:
                    title = clean_string(list_item_a.text)
                    url = list_item_a.get('href')
                    content = ''

                    if 'application/pdf' in list_item_a.get('type'):
                        response = requests.get(url)
                        if response.status_code == 200:
                            pdf_data = BytesIO(response.content)
                            try:
                                doc = fitz.open("pdf", pdf_data)
                                for page in doc:
                                    content += page.get_text()
                            except Exception as e:
                                print(f"Error trying to read document pages {url}: {e}")
                        else:
                            print(f"Error downloading document {url}: {response.status_code}")

                    query = "INSERT INTO `normativa` (`ciudad`, `date`, `titulo`, `grupo`, `subgrupo`, `url`, `content`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(query, (location, date, title, group, subgroup, url, content))


                    conn.commit()
    

    # Close database connection
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()