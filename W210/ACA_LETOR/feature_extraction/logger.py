from datetime import datetime

class logger:
    '''
    '''

    log = None
    log_name = None

    def __init__(self, log_type):
        # if self.log: self.log.close()
        self.log_name = '%s_%s.txt' %(log_type, datetime.now().strftime('%Y%m%d%H%M%S'))
        self.log = open(self.log_name, 'w')

    def close(self):
        self.log.close()

    def trace(self, msg):
        self.log.write('%s: DEBUG - %s\n' %(datetime.now().strftime('%Y%m%d%H%M%S'), msg))

    def warning(self, msg):
        self.log.write('%s: WARNING - %s\n' %(datetime.now().strftime('%Y%m%d%H%M%S'), msg))

    def error(self, msg):
        self.log.write('%s: ERROR - %s\n' %(datetime.now().strftime('%Y%m%d%H%M%S'), msg))

    def log_handler(self):
        return self.log

    def log_file(self):
        return self.log_name
