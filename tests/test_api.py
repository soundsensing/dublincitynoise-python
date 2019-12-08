
import datetime

import pytest

from dublincitynoise import fetch, import_historical, historical

api = fetch

# basic happy case
def test_api_fetch():

    start = datetime.datetime.fromtimestamp(1568815675.854366)
    end = datetime.datetime.fromtimestamp(1568902075.854366)
    location = "Bull Island"

    # TODO: use a mock to avoid hitting their API
    data = api.get_data(location=location, start=start, end=end)
    print('d', len(data), data[0])

    assert len(data), 571

