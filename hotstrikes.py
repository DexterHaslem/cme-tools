from csv import DictReader
from urllib.request import urlopen
from netutil import get_network_lines

FTP_ROOT = 'ftp://ftp.cmegroup.com/pub/settle/'
OPTS_CSV_FILES = ['comex_option.csv', 'nymex_option.csv']


def get_all_options_gen():
    for f in OPTS_CSV_FILES:
        url = FTP_ROOT + f
        with urlopen(url) as resp:
            reader = DictReader(get_network_lines(resp))
            for row in reader:
                # we do all our processing by volume, so parse it here
                volstr = row['EST. VOL']
                vol = int(volstr)
                row['EST. VOL'] = vol
                yield row


if __name__ == '__main__':
    print('fetching all options for COMEX and NYMEX, this will take a few seconds..')
    options = list(get_all_options_gen())
    print('got {} options and strikes'.format(len(options)))

    print('filtering active')

    # grab only strikes with trading activity of decent mention

    # TODO: process these in one loop. pre-processing active is small enough it doesnt effect perf currently ...
    # but pulling in equity products would be devastating
    active = [o for o in options if o['EST. VOL'] >= 100]
    calls = [o for o in active if o['PUT/CALL'] == 'C']
    puts = [o for o in active if o['PUT/CALL'] == 'P']

    sorted_calls = sorted(calls, key=lambda v: v['EST. VOL'], reverse=True)
    sorted_puts = sorted(puts, key=lambda v: v['EST. VOL'], reverse=True)


    def print_opt(o):
        print('\tPROD={:<4} STRIKE={:<6} VOL={:<6} LAST={:<6} SETTLE={:<8}'.format(o['PRODUCT SYMBOL'], o['STRIKE'],
                                                                                   o['EST. VOL'],
                                                                                   o['LAST'],
                                                                                   o['SETTLE']))


    # map(print_opt, sorted_calls[:10])

    print('** top calls by vol**')
    for v in sorted_calls[:10]:
        print_opt(v)

    print('** top puts by vol**')
    for v in sorted_puts[:10]:
        print_opt(v)
