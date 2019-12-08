
"""Import soundlevel data from the Dublin"""

import os.path
import re

import pandas
import requests

from . import historical as dublin


def load_data(path):

    d = dublin.load_data(path)
    d = d.compute() # get pandas.DataFrame

    # index on time
    d['time'] = d.index
    d.time = pandas.to_datetime(d.time, format='%d/%m/%Y %H:%M:%S')
    d.index = d.time

    return d

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
        #print('try unzip', filename)
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
    zips = zips[~zips.filename.isin(dublin.ignore)]

    name_year = zips.filename.apply(extract_name_year)
    zips = zips.join(name_year)

    # Unzip the data
    success = zips.filename.apply(try_unzip)
    with_data = zips[success].sort_values('filename')

    print('failed', '\n'.join(zips[~success].canonical_url))

    print('s', with_data[['name', 'year']])

    #print('n', zips)


    def import_it(filename):
        d = filename.replace('.zip', '')
        place, year = extract_name_year(filename) 

        sensor_name = f'Dublin {place}'
        path = os.path.join(data_dir, d, '/*/*/*.txt')

        data = load_data(path)
        import_data(data, sensor_name=sensor_name, api_key=api_key)


    with_data.filename.apply(import_it)
    

    
if __name__ == '__main__':
    main()
