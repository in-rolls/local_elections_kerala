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
    links = soup.find_all('a', {'href': re.compile('electdmemberdet')})
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
        soup3 = BeautifulSoup(r.text, 'html.parser')
        links3 = soup3.find_all('a', {'href': re.compile('electdmemberpersondet')})
        urls3 = []
        for l3 in links3:
            url3 = base_url + l3['href']
            if url3 not in urls3:
                urls3.append(url3)
        df['person_url'] = urls3
        rows = df.person_url.mapply(lambda c: scrape_person(c))
        df = pd.concat([df, pd.DataFrame(rows)], axis=1)
        del df['person_url']
        if len(df) == 0:
            continue
        if adf is None:
            adf = df
        else:
            adf = pd.concat([adf, df])
        #break
    return adf


if __name__ == "__main__":

    base_url = 'https://lsgkerala.gov.in'

    # row_id, district, ward_no, ward_name, elected_member, role, party, reservation, address, phone, mobile, age, male/female, marital_status, education, occupation, photo_file_path

    adf = None

    try:
        for year in [2010, 2015, 2020]:
            # LGI Type: District Panchayat
            url = 'https://lsgkerala.gov.in/en/lbelection/electdistrict/%d/1' % year
            df = scrape_common(url, year)
            if adf is None:
                adf = df
            else:
                adf = pd.concat([adf, df])

            # LGI Type: Block Panchayat
            url = 'https://lsgkerala.gov.in/en/lbelection/electdistrict/%d/2' % year
            r = request_retry(url)
            soup = BeautifulSoup(r.text, 'html.parser')
            links = soup.find_all('a', {'href': re.compile('electlbrpt')})
            for l in links:
                district = l.text.strip()
                url = base_url + l['href']
                df = scrape_common(url, year, district, 'Block')
                adf = pd.concat([adf, df])
                #break

            # LGI Type: Municipality
            url = 'https://lsgkerala.gov.in/en/lbelection/electdistrict/%d/3' % year
            r = request_retry(url)
            soup = BeautifulSoup(r.text, 'html.parser')
            links = soup.find_all('a', {'href': re.compile('electlbrpt')})
            for l in links:
                district = l.text.strip()
                url = base_url + l['href']
                df = scrape_common(url, year, district, 'Municipality')
                adf = pd.concat([adf, df])
                #break

            # LGI Type: Corporation
            url = 'https://lsgkerala.gov.in/en/lbelection/electdistrict/%d/4' % year
            df = scrape_common(url, year, None, 'Corporation')
            adf = pd.concat([adf, df])

            # LGI Type: Grama Panchayat
            url = 'https://lsgkerala.gov.in/en/lbelection/electdistrict/%d/5' % year
            r = request_retry(url)
            soup = BeautifulSoup(r.text, 'html.parser')
            links = soup.find_all('a', {'href': re.compile('electlbrpt')})
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
        adf.to_csv('lsgi-election-kerala.csv', index=False)
