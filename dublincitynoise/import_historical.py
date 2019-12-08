
"""Import soundlevel data from the Dublin"""

import os.path
import re
import glob

import pandas
import requests

#from . import historical as dublin

# FIXME: duplicated
ignore = [
    'dccambientsoundmonitoring2012.zip',
    'dccambientsoundmonitoring2011.zip', # much larger, does not seem to follow same format

    # URL gives not a ZIP but an HTML error page
    'navanroad2015.zip',
    'navanroad2014.zip',
    'navanroad2013.zip',
    'navanroad2012.zip',
    'chancerypark2014.zip',
    'chancerypark2015.zip',
    'dolphinsbarn2013.zip',
    'mellowspark-2015.zip',
    'mellowspark2014.zip',
]

def load_csv(path):
    columns = ('A_Leq', 'A_L10', 'A_L95',
               'C_Leq', 'C_L10', 'C_L95')

    dtypes = { n: 'float64' for n in columns }

    df = pandas.read_csv(path,
            header=None, skiprows=9,
            sep=',', delim_whitespace=False,
            names=columns, dtype=dtypes,
    )
 
    #for c in df.columns:
    #    print(df[c].dtype)

    df['time'] = df.index
    df.time = pandas.to_datetime(df.time, format='%d/%m/%Y %H:%M:%S')
    df.index = df.time
    df = df.drop('time', axis=1)
    return df


def load_data(path, site=None):

    matches = glob.glob(path)

    dfs = [] 

    for m in matches:
        try:
            df = load_csv(m)
            if site is not None:
                df['site'] = site

            dfs.append(df)
            #print('l', len(df), df.columns)
        except ValueError as e:
            print('Error', e)

    df = pandas.concat(dfs)

    return df


def parse():
    import argparse

    parser = argparse.ArgumentParser(description='Import Dublin sound-level data')
    a = parser.add_argument

    parsed = parser.parse_args()

    return parsed

def main():
    args = parse()

    data_dir = 'analysis/dublin-asm-1'
    files = pandas.read_csv(os.path.join(data_dir, 'files.csv'))


    def try_unzip(filename):
        print('try unzip', filename)
        assert filename.endswith('.zip'), filename
        zip_path = os.path.join(data_dir, filename)
        dir_path = os.path.join(data_dir, filename.replace('.zip', ''))
        if not os.path.exists(dir_path):
            print('z', zip_path)
            import zipfile
            try:
                zip_ref = zipfile.ZipFile(zip_path, 'r')
                zip_ref.extractall(dir_path)
                zip_ref.close()
                # FIXME: get valid data for all ZIP files
                # some of them return 502 bad gateway from Python/curl, but work in Firefox
            except zipfile.BadZipFile as e:
                print('UNZIP ERROR', e)
                return False

        else:
            pass
            #print('got it', p, d)

        return True # success

    def extract_name_year(filename):
        filename = filename.replace('.zip', '')
        m = re.match(r'(\D+)(\d*)', filename)
        assert m is not None, filename
        name, year = m.groups()
        name = name.strip('-')
        s = pandas.Series({'name': name, 'year': year})
        return s

    # Only get the ZIP files on same format
    zips = files[files.filename.str.endswith('zip')]
    zips = zips[~zips.filename.isin(ignore)]

    name_year = zips.filename.apply(extract_name_year)
    zips = zips.join(name_year)

    # Unzip the data
    success = zips.filename.apply(try_unzip)
    print('s\n', success)
    with_data = zips[success].sort_values('filename')

    print('failed\n', '\n'.join(zips[~success].canonical_url))

    print('success\n', with_data[['name', 'year']])

    #print('n', zips)
    
    def csv_path(archive):
        p, _ = os.path.splitext(archive.filename)
        pp = os.path.join(data_dir, p, '*/*/*.txt')
        return pp

    print('foo', with_data.iloc[0])
    out_dir = csv_path(with_data.iloc[0])

    dds = []

    for idx, d in with_data.iterrows():
        p = csv_path(d)
        df = load_data(p, site=d['name'])
        print('p', p, d['name'])
        dds.append(df)
        print('l', len(df))

    joined = pandas.concat(dds)
    joined.to_hdf('joined.h5', 'soundlevels', format='table')
    print(joined)

    
if __name__ == '__main__':
    main()
