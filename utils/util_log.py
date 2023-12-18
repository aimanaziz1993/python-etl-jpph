# -*- coding: utf-8 -*-
# -------------------------------------------------------
# __author__ = 'Aiman Aziz'
# date: 5/Dec/2023
# -------------------------------------------------------

import logging
import os.path
import sys


def logger():
    """
    Function returns logger instance
    :return: log
    :rtype: object
    """
    program = os.path.basename(sys.argv[0])
    log = logging.getLogger(program)
    # logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s')
    logging.basicConfig(format='%(asctime)s : [%(filename)s:%(lineno)d] : %(levelname)s : %(message)s')
    logging.root.setLevel(level=logging.INFO)
    log.info("running %s" % ' '.join(sys.argv))
    return log