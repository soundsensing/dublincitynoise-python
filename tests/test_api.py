
import datetime
import pandas

import pytest

from dublincitynoise import api


# basic happy case
def test_api_fetch():
    start = datetime.datetime.fromtimestamp(1568815675.854366)
    end = datetime.datetime.fromtimestamp(1568902075.854366)
    location = "Bull Island"

    # TODO: use a mock to avoid hitting their API also
    data = api.get_data(location=location, start=start, end=end)

    assert len(data), 571
    assert str(data.index.dtype) == 'datetime64[ns]'

    assert 'A_leq' in data.columns
    assert data.A_leq.iloc[0] == 49.67

    assert 'site' in data.columns
    assert data.site.unique()[0] == location    
    
    assert 'site_id' in data.columns
    assert data.site_id.unique()[0] == 1


# basic failing case
def test_api_period_without_data():
    location = api.sensor_sites[0]
    start = datetime.datetime.fromtimestamp(1367362800)
    end = datetime.datetime.fromtimestamp(1367794800)

    with pytest.raises(Exception) as e_info:
        data = api.get_data(location=location, start=start, end=end)
        assert 'No data' in str(e_info.value)
