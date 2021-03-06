
import os
import datetime
import argparse
import uuid

import pandas
import requests
import numpy

from . import sensors

sensor_sites = sensors.api_order

def get_data(location, start, end):

    """
    Fetch data from Dublin City Noise using their API
    http://dublincitynoise.sonitussystems.com/applications/api/api-doc.html
    """

    try:
        location_id = sensor_sites.index(location)
    except ValueError as e:
        raise ValueError(f"Unknown sensor location {location}")

    start = start.timestamp()
    end = end.timestamp()

    params = {
        'location': location_id,
        'start': start ,
        'end': end,
    }
    u = f'http://dublincitynoise.sonitussystems.com/applications/api/dublinnoisedata.php'

    r = requests.get(u, params=params, timeout=1.0)
    # content-type is always text/html :(
    assert r.status_code == 200, f'API returned {r.status_code}: {r.body}'

    # sometimes invalid JSON is returned
    # status-code is not 404 for missing data
    if '{' not in r.text:
        raise Exception(r.text)

    data = r.json()

    missing_fields = set(['aleq', 'dates', 'times']) - set(data.keys())
    assert not missing_fields, f'Response missing fields: {missing_fields}'

    # 05/05/2013 22:45:00
    date_format = '%d/%m/%Y %H:%M:%S'
    out = []
    for leq, date, time in zip(data['aleq'], data['dates'], data['times']):
        dt = datetime.datetime.strptime(date + ' ' + time, date_format)
        leq = float(leq)
        out.append({'time': dt, 'A_leq': leq})
   
    # check postconditions
    first = out[0]['time']
    for idx, d in enumerate(out[1:]):
        t = d['time']
        assert t > first, f'times are not sorted {idx}. {t} < {first}'

    # convert to pandas DataFrame
    df = pandas.DataFrame.from_records(out)
    df.index = df.time
    df = df.drop('time', axis=1)
    df['site'] = location
    df['site_id'] = location_id

    return df



def parse():
    # TODO: add commandline options for time, period, location

    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("-l", "--location", action="append",
        help="Specify location to fetch data for. Can be specified multiple times. Default: All locations"
    )

    parser.add_argument("--start",
        help="Starting time")
    parser.add_argument("--end",
        help="End time")


    args = parser.parse_args()

    # default to all
    if not args.locations:
        args.locations = sensor_sites

    return args


def fetch_locations(sites):

    dfs = [] 
    for site in sensor_sites:
        data = get_data(location=location, start=start, end=end)
        dfs.append(dfs)

    return data


def main():

    args = parse()

    start = 1367362800
    end = 1367794800

    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(seconds=24*60*60)
    
    fetch_locations


    print(info)
    #d = get_data()
    #print('d', d)


if __name__ == '__main__':
    main()
