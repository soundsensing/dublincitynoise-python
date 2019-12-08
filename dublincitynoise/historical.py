
"""Historical data from Dublin Ambient Sound Monitoring Network"""

import urllib
import urllib3
import shutil
import os
import hashlib

import requests
import pandas
import dask.dataframe
import lxml.html


def sha256sum(filename):
    h  = hashlib.sha256()
    b  = bytearray(128*1024)
    mv = memoryview(b)
    with open(filename, 'rb', buffering=0) as f:
        for n in iter(lambda : f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()

def download_file(url, out):
    temp_out = out+'.part'
    if os.path.exists(temp_out):
        raise ValueError(f'Temp file "{temp_out}" already exists')

    try:
        with requests.get(url, stream=True) as r:
            with open(temp_out, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
    except (urllib.error.HTTPError, urllib3.exceptions.ProtocolError) as e:
        if os.path.exists(temp_out):
            os.remove(temp_out)
    
    shutil.move(temp_out, out)
    
    return out

def fetch_urls():
    """Get list of dataurls from dataset webpage"""

    index_url = 'https://data.smartdublin.ie/dataset/ambient-sound-monitoring-network'
    url_xpath = '//*[@id="dataset-resources"]/ul/li/div[2]/div/a[2]'
    
    page = requests.get(index_url)
    tree = lxml.html.fromstring(page.content)
    a_elements = tree.xpath(url_xpath)
    urls = [ e.attrib['href'] for e in a_elements ]
    return urls



def load_data(path):
    columns = ('A_Leq', 'A_L10', 'A_L95',
               'C_Leq', 'C_L10', 'C_L95')
    # TODO: 
    df = dask.dataframe.read_csv(path,
            header=None, skiprows=9,
            sep=',', delim_whitespace=False,
            names=columns,
    )
    # FIXME: index time
    return df

def url_filename(url):
    return url.split('/')[-1]

def files_df(urls):
    files = pandas.DataFrame({
        'filename': list(map(url_filename, urls)),
        'canonical_url': urls,
    })

    files.index = files.filename

    return files

ignore = [
    'dccambientsoundmonitoring2012.zip',
    'dccambientsoundmonitoring2011.zip', # much larger, does not seem to follow same format
]

def main():
    data_dir = 'data'

    known = pandas.read_csv(os.path.join(data_dir, 'files.csv'))
    known.index = known.filename

    # check for new files
    website = files_df(fetch_urls())
    new = set(website.filename).difference(known.filename)
    print('New files:', new)
    if new:
        return 1

    def file_loc(p):
        return os.path.join(data_dir, p)

    files = known

    chosen = known[~known.filename.isin(ignore)].copy()
    missing = [ not os.path.exists(file_loc(p)) for p in chosen.filename ]
    #print('m', list(missing_files))
    print('uu', chosen[missing])
    for f, p in chosen[missing].iterrows():
        out = file_loc(f)
        url = p.canonical_url
        print('Downloading', url, out) 
        download_file(url, out)
 
    def try_sha256(f):
        f = file_loc(f)
        if os.path.exists(f):
            return sha256sum(f)
        else:
            return None

    files['sha256'] = files.filename.apply(try_sha256)
    del files['filename']
    files.to_csv(os.path.join('data', 'files2.csv'))

    return 0

if __name__ == '__main__':
    main()    

