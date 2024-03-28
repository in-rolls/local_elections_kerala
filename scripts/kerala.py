#!/usr/bin/env python
# coding: utf-8

import os
import re
import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup

IMAGE_DIR = 'images'


def download_file(url, dir=IMAGE_DIR):
    local_filename = os.path.join(dir, url.split('/')[-1])
    with requests.get(url, stream=True) as r:
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
    return local_filename


def scrape_common(url, year, district=None, type='District'):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    links = soup.find_all('a', {'href': re.compile('electdmemberdet')})
    adf = None
    for l in links:
        url2 = base_url + l['href']
        r = requests.get(url2)
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
        rows = []
        for l3 in links3:
            url3 = base_url + l3['href']
            if url3 in urls3:
                continue
            urls3.append(url3)
            #print(url3)
            r = requests.get(url3)
            soup4 = BeautifulSoup(r.text, 'html.parser')
            block = soup4.find('div', {'id': 'block-zircon-content'})
            img = block.find('img')
            #print(img)
            dfs = pd.read_html(StringIO(r.text))
            row = dfs[2].set_index(0).T.to_dict('records')[0]
            img_url = 'http:' + img['src']
            row['Image'] = img_url
            download_file(img_url)
            rows.append(row)
            #break
        df = pd.concat([df, pd.DataFrame(rows)], axis=1)
        if adf is None:
            adf = df
        else:
            adf = pd.concat([adf, df])
        #break
    return adf


if __name__ == "__main__":

    base_url = 'https://lsgkerala.gov.in'

    # row_id, district, ward_no, ward_name, elected_member, role, party, reservation, address, phone, mobile, age, male/female, marital_status, education, occupation, photo_file_path

    for year in [2010, 2015, 2020]:
        # LGI Type: District Panchayat
        url = 'https://lsgkerala.gov.in/en/lbelection/electdistrict/%d/1' % year
        adf = scrape_common(url, year)
        
        # LGI Type: Block Panchayat
        url = 'https://lsgkerala.gov.in/en/lbelection/electdistrict/%d/2' % year
        r = requests.get(url)
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
        r = requests.get(url)
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
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        links = soup.find_all('a', {'href': re.compile('electlbrpt')})
        for l in links:
            district = l.text.strip()
            url = base_url + l['href']
            df = scrape_common(url, year, district, 'Grama Panchayat')
            adf = pd.concat([adf, df])
            #break
        #break

    adf.to_csv('lsgi-election-kerala.csv', index=False)
