import pytest
from scripts.app import parse_apache_time
from scripts.app import parse_apache_log_line
import datetime
from pyspark.sql import Row


def test_parse_apache_time():
    """
    Testing test_parse_apache_time
    """
    s = '01/Aug/1995:00:36:30 -0400'
    assert parse_apache_time(s) == datetime.datetime(1995, 8, 1, 0, 36, 30)

def test_parse_apache_log_line():
    """
    Testing parse_apache_log_line
    """
    logline = 'dialup-2-139.gw.umn.edu - - [01/Aug/1995:00:36:30 -0400] "GET /facilities/mlp.html HTTP/1.0" 200 2653'
    assert parse_apache_log_line(logline) == (Row(client_identd='-', content_size=2653, date_time=datetime.datetime(1995, 8, 1, 0, 36, 30), endpoint='/facilities/mlp.html', host='dialup-2-139.gw.umn.edu', method='GET', protocol='HTTP/1.0', response_code=200, user_id='-'), 1)