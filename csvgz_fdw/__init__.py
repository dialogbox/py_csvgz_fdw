from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

import logging
import csv
import gzip

class CSVGZForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(CSVGZForeignDataWrapper, self).__init__(options, columns)
        log_to_postgres("[CSVGZ_FDW] %s %s" % (options, columns), logging.DEBUG)

        self.columns = columns
        self.options = options

        if 'file_name' not in options:
            log_to_postgres('[CSVGZ_FDW] "file_name" is required', logging.ERROR)

        self.file_name = options['file_name'];

    def execute(self, quals, columns):
        log_to_postgres("[CSVGZ_FDW] %s %s" % (quals, columns), logging.DEBUG)
        log_to_postgres("[CSVGZ_FDW] %s" % (self.file_name), logging.DEBUG)

        with gzip.open(self.file_name, 'rt') as f:
            reader = csv.reader(f)
            try:
                for row in reader:
                    line = {}
                    i = 0
                    for column_name in self.columns:
                        if column_name in columns:
                            line[column_name] = row[i]
                        i = i + 1
                    yield line
            except OSError as e:
                log_to_postgres("[CSVGZ_FDW] OS Error (%s)" % (e), logging.ERROR)
            except csv.Error as e:
                log_to_postgres("[CSVGZ_FDW] Error {}" % (e), logging.ERROR)
            except:
                log_to_postgres("[CSVGZ_FDW] Unexpected error: %s" % (sys.exc_info()[0]), logging.ERROR)
