''' A tool that automatically downloads and reads data from Opus '''
import requests
from bs4 import BeautifulSoup
import zipfile
import os
import sys
import shutil

# Globals
url = 'http://opus.nlpl.eu/'
data_dir = ''

def get_download_info(src, trg, data_sources):
    ''' Gets the download information from opus '''
    print('Querying Opus...')
    data = {'src': src, 'trg': trg, 'minsize': 'all'}
    r = requests.post('http://opus.nlpl.eu/', data=data)
    if r.status_code != 200:
        print('ERROR: Could not access Opus')
        sys.exit()

    soup = BeautifulSoup(r.text, 'html.parser')
    download_info = {}

    for tr in soup.find_all('div', {'class': 'counts'})[0].find_all('tr')[1:-1]:
        tds = tr.find_all('td')
        if tds[0].get_text().split()[0] in data_sources:
            links = []
            for a in tds[5].find_all('a'):
                links.append(a['href'])
            download_info[tds[0].get_text().split()[0]] = links
    
    return download_info


def download_data(download_info,):
    ''' Downloads all of the data with the download info '''
    for origin in download_info:
        print()
        print(f'Downloading {data_dir}/{origin} data...')
        if not os.path.isdir(f'./{data_dir}'):
            os.mkdir(f'./{data_dir}')
        if not os.path.isdir(f'./{data_dir}/{origin}'):
            os.mkdir(f'./{data_dir}/{origin}')
        for link in download_info[origin]:
            link_name = link.split('/')[-1]
            print(f'    Downloading {link_name}...')
            r = requests.get(link, allow_redirects=True)
            if r.status_code == 200:
                download_path = f'./{data_dir}/{origin}/{link_name}'
                link_file = open(download_path, 'wb')
                link_file.write(r.content)
                link_file.close()

                if link[-4:] == '.zip':
                    print(f'    Unzipping {link_name}...')
                    with zipfile.ZipFile(download_path, 'r') as zip_ref:
                        zip_ref.extractall(f'./{data_dir}/{origin}/')
            else:
                print(f'ERROR: Could not access {origin} download link')
        print('    Done!')

    print()
    print('Finished Downloading Opus Data (Raw Format)...')
    print()


def gzip_data(origin, lng):
    ''' Gzips xml data in expected locations '''
    data_path = f'./{data_dir}/{origin}/{origin}/xml/{lng}'
    if not os.path.isdir(f'./{data_dir}/{origin}/{lng}'):
        os.mkdir(f'./{data_dir}/{origin}/{lng}')
    for f in os.listdir(data_path):
        # print(f'f: {f}')
        new_file_path = f'./{data_dir}/{origin}/{lng}/{f}'
        if os.path.isdir(f'{data_path}/{f}'):
            os.system(f'cp -R {data_path}/{f} {new_file_path}')
            for dir_path, _, file_names in os.walk(new_file_path):
                for file_name in file_names:
                    os.system(f'gzip {dir_path}/{file_name}')
        else:
            os.system(f'cp {data_path}/{f} {new_file_path}')
            os.system(f'gzip {new_file_path}')


def read_opus(download_info, src, trg):
    ''' Uses opus read to read opus '''
    print('Starting Processing Step...')
    for origin in download_info:
        print(f'Gzipping {origin} {src} data...')
        gzip_data(origin, src)
        print(f'Gzipping {origin} {trg} data...')
        gzip_data(origin, trg)
    
        af_file = [file.split('/')[-1] for file in download_info[origin] if file[-7:] == '.xml.gz'][0]
        # os.system(f'cd {origin}')
        print(f'Reading {origin}...')
        os.chdir(f'{data_dir}/{origin}')
        os.system(f'opus_read --directory test --source {src} --target {trg} ' +
                  f'--leave_non_alignments_out -af {af_file} --write ../{origin}.txt')
        os.chdir(f'../..')

        print(f'Finished Processing {origin} Opus data')
        print()

    print()
    print('Finished Processing all Opus data')
    print()


def delete_downloads(download_info):
    ''' Deletes all of the folders created in this process '''
    print('Deleting all created folders...')
    for origin in download_info:
        print(f'Deleting {data_dir}/{origin} folder...')
        shutil.rmtree(f'./{data_dir}/{origin}')

    print()
    print('Finished Deleting all created folders')
    print()


def query_opus(src, trg, data_sources):
    ''' Scrapes Opus given the src and target languages '''
    global data_dir
    data_dir = f'./{src}-{trg}'
    download_info = get_download_info(src, trg, data_sources)
    download_data(download_info)
    read_opus(download_info, src, trg)
    delete_downloads(download_info)


if __name__ == '__main__':
    sources = ["kjh","cjs","en","ru",]
    targets = ["kjh",
                "cjs","en", "ru",]

    for i, src in enumerate(sources):
        for j in range(i+1, len(targets)):
            if src == 'tr' and j < targets.index('en'):
                continue
            if targets[j] != 'ru' and src != 'en':
                query_opus(src, targets[j], ['JW300', 'GoURMET'])
                #ky tr next
