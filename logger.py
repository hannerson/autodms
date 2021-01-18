# -*- coding=utf-8 -*-

import logging
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler
import sys
import threading

reload(sys)
sys.setdefaultencoding('utf-8')

def get_logger():
	logger = logging.getLogger()
	LOG_FILE = "log/" + sys.argv[0].split("/")[-1].replace(".py","") + '.log'
	#hdlr = TimedRotatingFileHandler(LOG_FILE,when='D',backupCount=30)
	hdlr = RotatingFileHandler(LOG_FILE,maxBytes=1024*1024*1024,backupCount=30)
	formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s[line:%(lineno)d]:%(message)s')
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr)
	logger.setLevel(logging.NOTSET)
	return logger

logging = get_logger()
