# -*- coding: utf-8 -*-
# -------------------------------------------------------
# __author__ = 'Aiman Aziz'
# date: 5/Dec/2023
# -------------------------------------------------------

import arrow
from random import randint
from multiprocessing import cpu_count
import re
import uuid


# ---------------------------------------
# PARALLEL processing manipulations
# -------------------------------------

def pool_processor():
    """
    Get number of processors available
    :return:
    :rtype: integer
    """
    num_processors = cpu_count()
    return num_processors


def get_workers():
    """
    Get number of worker threads. default: os.cpu_count() * 5
    :return:
    :rtype: integer
    """
    num_cpu = pool_processor()
    myworkers = (int(num_cpu) // 2) + 1
    if myworkers < 1:
        myworkers = (int(num_cpu) // 2)
    return myworkers


def split_list(alist, wanted_parts=1):
    """
    # split list into number of parts
    :param alist:
    :type alist:
    :param wanted_parts:
    :type wanted_parts:
    :return:
    :rtype:
    """
    length = len(alist)
    return [alist[i * length // wanted_parts: (i + 1) * length // wanted_parts]
            for i in range(wanted_parts)]


def gen_sublist(posts, num_cpu, no_of_posts):
    """
    # data sublist  Manipulation
    :param posts:
    :type posts:
    :param num_cpu:
    :type num_cpu:
    :param no_of_posts:
    :type no_of_posts:
    :return:
    :rtype:
    """
    if no_of_posts < num_cpu:
        parts = no_of_posts
    else:
        if no_of_posts >= num_cpu:  # limit:
            parts = num_cpu  # limit
        else:
            parts = no_of_posts
    # -------------------------------
    # split list into number of parts
    sublists = split_list(posts, parts)
    return sublists


# ---------------------------------------
# TEXT manipulations
# ---------------------------------------
def strip_only_digit(strval):
    key = re.sub("\D", "", strval)
    return key


def get_uuid():
    """
    get 8 digit unique id
    :return:
    :rtype:
    """
    unique_id = uuid.uuid4()
    return unique_id.node


# ---------------------------------------
# TIME manipulations
# ---------------------------------------
def gen_curr_local_date():
    utc = arrow.utcnow()
    local = utc.to('Asia/Kuala_Lumpur')
    local_date = local.format('YYYY_MM_DD')
    return local_date


def gen_curr_epoch():
    """
    Function returns time in epoch
    :return: unix_time
    :rtype: integer
    """
    utc = arrow.utcnow()
    local = utc.to('Asia/Kuala_Lumpur')
    unix_time = local.timestamp()
    return unix_time


def getcurrdt_es():
    """
    get current server utc time for elasticsearch
    :rtype: object
    """
    dt = arrow.utcnow()
    return str(dt)


def getcurrdt_year():
    """
    get current year
    :rtype: object
    """
    dt = arrow.utcnow()
    return dt.year


def get_sleep_time(one, two):
    """
    # randomise sleep
    :param one:
    :type one:
    :param two:
    :type two:
    :return:
    :rtype:
    """
    timer = randint(one, two)
    return timer