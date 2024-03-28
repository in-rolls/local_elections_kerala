#!/usr/bin/env python
# coding: utf-8

import os
import re
import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup
import mapply
import time

mapply.init(
    n_workers=10,
    chunk_size=2,
    max_chunks_per_worker=40,
    progressbar=False
)

IMAGE_DIR = 'images'


def download_file(url, dir=IMAGE_DIR):
    local_filename = os.path.join(dir, url.split('/')[-1])
    if os.path.exists(local_filename):
        return local_filename
    with requests.get(url, stream=True) as r:
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
    return local_filename


def request_retry(url, num_retries=5, success_list=[200, 404], **kwargs):
    for i in range(num_retries):
        try:
            response = requests.get(url, **kwargs)
            if response.status_code in success_list:
                ## Return response if successful
                return response
        except requests.exceptions.ConnectionError:
            time.sleep((i + 1) * 10)
    return None


def scrape_person(url):
    r = request_retry(url)
    soup4 = BeautifulSoup(r.text, 'html.parser')
    block = soup4.find('div', {'id': 'block-zircon-content'})
    img = block.find('img')
    #print(img)
    dfs = pd.read_html(StringIO(r.text))
    row = dfs[2].set_index(0).T.to_dict('records')[0]
    img_url = 'http:' + img['src']
    row['Image'] = img_url
    try:
        download_file(img_url)
    except Exception as e:
        print(e, img_url)
    return pd.Series(row)


def scrape_common(url, year, district=None, type='District'):
    r = request_retry(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    links = soup.find_all('a', {'href': re.compile('candidateDetails')})
    adf = None
    for l in links:
        url2 = base_url + l['href']
        r = request_retry(url2)
        dfs = pd.read_html(StringIO(r.text))
        df = dfs[2]
        if district is None:
            district = l.text.strip()
        df.insert(0, 'Grama Panchayat', '')
        df.insert(0, 'Corporation', '')
        df.insert(0, 'Municipality', '')
        df.insert(0, 'Block', '')
        df.insert(0, 'District', district)
        df.insert(0, 'LGI Type', type)
        df.insert(0, 'Year', year)
        print(year, type, district, l.text.strip())
        if type != 'District':
            df[type] = l.text.strip()
        if adf is None:
            adf = df
        else:
            adf = pd.concat([adf, df])
        #break
    return adf


if __name__ == "__main__":

    base_url = 'https://lsgkerala.gov.in/election2005/'

    # row_id, district, ward_no, ward_name, elected_member, role, party, reservation, address, phone, mobile, age, male/female, marital_status, education, occupation, photo_file_path

    adf = None

    year = 2005

    try:
        # LGI Type: District Panchayat
        url = 'https://lsgkerala.gov.in/election2005/districtReport.php?t=1'
        df = scrape_common(url, year)
        if adf is None:
            adf = df
        else:
            adf = pd.concat([adf, df])

        # LGI Type: Block Panchayat
        url = 'https://lsgkerala.gov.in/election2005/districtReport.php?t=2'
        r = request_retry(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        links = soup.find_all('a', {'href': re.compile('lbReport')})
        for l in links:
            district = l.text.strip()
            url = base_url + l['href']
            df = scrape_common(url, year, district, 'Block')
            adf = pd.concat([adf, df])
            #break

        # LGI Type: Municipality
        url = 'https://lsgkerala.gov.in/election2005/districtReport.php?t=3'
        r = request_retry(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        links = soup.find_all('a', {'href': re.compile('lbReport')})
        for l in links:
            district = l.text.strip()
            url = base_url + l['href']
            df = scrape_common(url, year, district, 'Municipality')
            adf = pd.concat([adf, df])
            #break

        # LGI Type: Corporation
        url = 'https://lsgkerala.gov.in/election2005/districtReport.php?t=4'
        df = scrape_common(url, year, None, 'Corporation')
        adf = pd.concat([adf, df])

        # LGI Type: Grama Panchayat
        url = 'https://lsgkerala.gov.in/election2005/districtReport.php?t=5'
        r = request_retry(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        links = soup.find_all('a', {'href': re.compile('lbReport')})
        for l in links:
            district = l.text.strip()
            url = base_url + l['href']
            df = scrape_common(url, year, district, 'Grama Panchayat')
            adf = pd.concat([adf, df])
            #break
        #break
    except Exception as e:
        print(e)
        print(url)
        raise
    finally:
        adf.to_csv('lsgi-election-kerala-2005.csv', index=False)
