from csv import DictReader
from urllib.request import urlopen
# use clock over time on windows, will use RTC hardware
from time import clock

from netutil import get_network_lines

FTP_ROOT = 'ftp://ftp.cmegroup.com/pub/settle/'
FUTURES_CSV_FILES = ['comex_future.csv', 'nymex_future.csv']

# futures columns:
# PRODUCT SYMBOL,CONTRACT MONTH,CONTRACT YEAR,CONTRACT DAY,
# CONTRACT,PRODUCT DESCRIPTION,OPEN,HIGH,HIGH AB INDICATOR,
# LOW,LOW AB INDICATOR,LAST,LAST AB INDICATOR,SETTLE,PT CHG,
# EST. VOL,PRIOR SETTLE,PRIOR VOL,PRIOR INT,TRADEDATE
# SI,05,2019,,SIK19,Silver Futures,16.880,16.880,,16.880,,16.880,,16.891,-.005,144,16.896,,150,06/01/2018
# put any column values we care about that we want to parse as numbers
FUTURES_INT_COLS = ['CONTRACT MONTH', 'CONTRACT YEAR', 'CONTRACT DAY', 'EST. VOL', 'PRIOR VOL', 'PRIOR INT']
FUTURES_FLOAT_COLS = ['OPEN', 'HIGH', 'LOW', 'LAST', 'SETTLE']

# calculated value keys
TOTAL_VOLUME_KEY = 'TOTAL VOL'
TOTAL_OI_KEY = 'TOTAL OI'
TOP_CONTRACT = 'TOPCONTRACT'
ROWS_KEY = 'rows'


def parse_row_vals(row):
    # beware values can be empty, particularly previous open interest on mondays
    for c in FUTURES_INT_COLS:
        if row[c]:
            row[c] = int(row[c])

    for c in FUTURES_FLOAT_COLS:
        if row[c]:
            row[c] = float(row[c])


def get_active_futures_gen(mv):
    for f in FUTURES_CSV_FILES:
        url = FTP_ROOT + f
        with urlopen(url) as resp:
            reader = DictReader(get_network_lines(resp))
            for row in reader:
                # while we're going through these, parse values
                parse_row_vals(row)
                if row['EST. VOL'] >= mv:
                    yield row


def group_by_product(rows):
    # we get list of raw rows from csv, each contract is broken down,
    # lets group by product so its easier to display in a summarized form
    ret = {}

    sorted_vol = sorted(rows, key=lambda v: v['EST. VOL'])
    for r in sorted_vol:
        sym = r['PRODUCT SYMBOL']
        # key by product
        if sym in ret:
            ret[sym][ROWS_KEY].append(r)
        else:
            ret[sym] = {ROWS_KEY: [r]}

    return ret


def calc_totals(settlements):
    for product, val in settlements.items():
        val[TOTAL_VOLUME_KEY] = 0
        val[TOTAL_OI_KEY] = 0

        for r in val[ROWS_KEY]:
            if r['EST. VOL']:
                val[TOTAL_VOLUME_KEY] += r['EST. VOL']
            if r['PRIOR INT']:
                val[TOTAL_OI_KEY] += r['PRIOR INT']

        # find the highest volume contract. its not always front month contract, eg on eurodollar packs
        byvol = sorted(val[ROWS_KEY], key=lambda i: i['EST. VOL'], reverse=True)
        val[ROWS_KEY] = byvol
        val[TOP_CONTRACT] = byvol[0]


if __name__ == '__main__':
    print('getting active CME daily settlements, this takes a moment...')
    start = clock()

    # the daily settlement outputs have all contracts, even nontrading (off board settlements)
    # so filter by some with feasible trading since we will only display the top anyway
    active_futures = list(get_active_futures_gen(1000))

    finish = clock()
    print('got {} active futures contracts in {} seconds'.format(len(active_futures), round(finish - start, 2)))

    settlements = group_by_product(active_futures)
    calc_totals(settlements)

    by_tcv = sorted(settlements.items(), key=lambda kvt: kvt[1][TOTAL_VOLUME_KEY], reverse=True)

    for (k, v) in by_tcv:
        print('** {:<4} TOTAL VOL={:<8} OI={:<6}'.format(k, v[TOTAL_VOLUME_KEY], v[TOTAL_OI_KEY]))

        # show top contracts for product
        for t in v[ROWS_KEY][:5]:
            vol = t['EST. VOL']
            volpct = round(vol / v[TOTAL_VOLUME_KEY] * 100, 2)
            print('\t{:<6} SETTLE={:<8} LAST={:<8} %CHG={:<8} VOL={:<8} ({}%) OI={:<12}'.format(t['CONTRACT'],
                                                                                                t['SETTLE'],
                                                                                                t['LAST'], t['PT CHG'],
                                                                                                vol, volpct,
                                                                                                t['PRIOR INT']))
