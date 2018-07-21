import logging
import optparse
import sys
import re
from crawl_machine_learning import crawl_machine_learning
from generate_json import generate_json

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
sort_order = 'ascending'

for arg in sys.argv:
    try:
        if re.compile('start_index=[0-9]+').match(arg):
            start_index = int(arg[12:])
        if re.compile('sort_order=desc').match(arg):
            sort_order = 'descending'
    except IndexError:
        pass
    except ValueError:
        pass

logging.info('start index: ' + str(start_index))

crawl_machine_learning(start_index=start_index, sort_order=sort_order)
generate_json()
