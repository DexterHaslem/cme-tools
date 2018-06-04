from csv import DictReader
from urllib.request import urlopen
# use clock over time on windows, will use RTC hardware
from time import clock

from netutil import get_network_lines

FTP_ROOT = 'ftp://ftp.cmegroup.com/pub/settle/'
OPTS_CSV_FILES = ['comex_option.csv', 'nymex_option.csv']


def get_all_options_gen():
    for f in OPTS_CSV_FILES:
        url = FTP_ROOT + f
        with urlopen(url) as resp:
            reader = DictReader(get_network_lines(resp))
            for row in reader:
                yield row



if __name__ == "__main__":
    pass