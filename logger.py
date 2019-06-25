import logging


logger = logging.getLogger("Sender")
logger.setLevel(logging.DEBUG)
logfile = 'sender.log'
fh = logging.FileHandler(logfile, encoding='utf-8')
formatter = logging.Formatter(u"%(asctime)s : %(levelname)-5s : %(filename)s : %(name)s logger : %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)


def debug(msg):
    logger.debug(msg)


def info(msg):
    logger.info(msg)


def error(msg):
    logger.error(msg)


def exception(msg):
    logger.exception(msg)
