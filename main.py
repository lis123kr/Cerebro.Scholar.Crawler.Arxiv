import logging
import optparse
import sys
import re
from crawl_machine_learning import crawl_machine_learning
# from generate_json import generate_json

LOGGING_LEVELS = {'critical': logging.CRITICAL,
                  'error': logging.ERROR,
                  'warning': logging.WARNING,
                  'info': logging.INFO,
                  'debug': logging.DEBUG}

parser = optparse.OptionParser()
parser.add_option('-l', '--logging-level', help='Logging level')
parser.add_option('-f', '--logging-file', help='Logging file name')
(options, args) = parser.parse_args()
logging_level = LOGGING_LEVELS.get(options.logging_level, logging.NOTSET)
logging.basicConfig(level=logging_level, filename=options.logging_file,
                    format='%(asctime)s %(levelname)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

start_index = 0
f = open("start_index.txt", "r")
start_index = int(f.readline())
f.close()
print("start_index = {}".format(start_index))
sort_order = 'ascending'

for arg in sys.argv:
    start_index_search = re.compile('start_index=([0-9a-zA-Z]+)').search(arg)
    try:
        if start_index_search is not None:
            try:
                start_index = int(start_index_search.group(1))
            except ValueError:
                if start_index_search.group(1) == 'auto':
                    start_index = -1
                else:
                    pass
        if re.compile('sort_order=desc').match(arg):
            sort_order = 'descending'
    except IndexError:
        pass
    except ValueError:
        pass

start_index, i, u = crawl_machine_learning(start_index=start_index, sort_order=sort_order)
print("insertd : {}, updated : {}".format(i, u))

f = open("start_index.txt", "w")
f.write(str(start_index))
f.close()
# generate_json()
