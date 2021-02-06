from config import Config
import logging
import os
from logging import log
from logging.handlers import RotatingFileHandler,TimedRotatingFileHandler


class MyLog:
    def __init__(self,log_dir='logs',loggername=None,level=logging.INFO) -> None:
        self.logdir = log_dir
        self.loggername = loggername
        if not os.path.isdir(self.logdir):
            os.mkdir(self.logdir)
        self.logfile = os.path.join(log_dir,loggername + '.log')
        self.rootlogfile = os.path.join(log_dir,loggername+'-root.log')
        conf = Config()
        self.config = conf.load_config()
        if 'log_level' in self.config.keys():
            if self.config['log_level'] == "DEBUG":
                self.log_level = logging.DEBUG

    def setup_logging(self):
        file_handler = TimedRotatingFileHandler(self.logfile, when='d',backupCount=0)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
        logger = logging.getLogger(self.loggername)
        logger.setLevel(self.log_level)
        logger.addHandler(file_handler)

        file_handler2 = RotatingFileHandler(self.rootlogfile, maxBytes=1024000,
                                           backupCount=3)
        file_handler2.setFormatter(logging.Formatter(
                '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))

        rootlogger = logging.getLogger()
        rootlogger.setLevel(self.log_level)
        rootlogger.addHandler(file_handler2)
        return (logger, rootlogger)
