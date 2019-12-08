
from dublincitynoise import sensors

import pandas


def main():

    from_api = set(sensors.api_order)
    from_hist = set(n for n in sensors.archive_names.values() if n)

    print(from_api, from_hist)

    missing = from_api - from_hist

    print(missing)

    return

    df = pandas.read_hdf('second.h5', 'soundlevels')

    print(df.site.unique())

    def pp(df):

        print(len(df)) 

    df.groupby('site').apply(pp)


if __name__ == '__main__':
    main()
