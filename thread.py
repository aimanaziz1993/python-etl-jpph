
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from json import dumps

try:
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    sys.path.append(BASE_DIR)
    from utils import util_gen, util_log
except ImportError as err:
    print(err)
    raise Exception("import util files failed")


def process_tv_trx_lot_sublist(log, sublist):
    """
    main function to process wifi points
    :param log:
    :type log:
    :param sublist:
    :type sublist:
    """
    # log.info("length: %s" % len(sublist))
    # data_out = []

    insert_rows = sublist[sublist['_merge'] == 'right_only'].drop('_merge', axis=1)
    filtered_df = insert_rows[insert_rows.columns.drop(list(insert_rows.filter(regex='_target')))]

    print("#---------------------------------------------------------")
    print("\n")
    print("Filtered Rows to show only _source suffix")
    print("\n")
    print(len(filtered_df), filtered_df)
    print("\n")
    print("#---------------------------------------------------------")

    # exit()
    return filtered_df

# --------------------------------------
# functions for parallel processing
# --------------------------------------
def process_user_ctrl(log, sublists):
    """
    process user data for data out
    :param log:
    :type log:
    :param sublists:
    :type sublists:
    :return:
    :rtype:
    """
    data_out = []
    # --------------------------------------
    # check params
    # log.info("len sublists: %s" % len(sublists))
    workers = util_gen.get_workers()
    workers = workers * 2
    # log.info("myworkers: %s" % workers)
    # --------------------------------------
    # launch parallel threads
    with ThreadPoolExecutor(max_workers=workers) as executor:
        fn = partial(process_tv_trx_lot_sublist, log)
        futures = executor.map(fn, sublists)
        # --------------------------------------
        # compile results
        print('futures', futures)
        exit()
        try:
            for r in futures:
                for s in r:
                    data_out.append(s)
        except Exception as e:
            log.error(e)
        pass
    return data_out


def get_sublists(log, result):
    """
    generate and split list into multiple sublists in preparation for parallel processes
    :param log:
    :type log:
    :param result:
    :type result:
    :return:
    :rtype:
    """
    # ------------------------------------
    # get number of processors
    workers = util_gen.get_workers()
    no_of_items = len(result)
    # -------------------------------
    # get no of parts
    parts = get_no_of_parts(workers, no_of_items)
    # log.info(parts)
    # -------------------------------
    # split list into number of parts
    sublists = util_gen.split_list(result, parts)
    return sublists


def get_no_of_parts(workers, no_of_items):
    """
    get number of parts
    :param workers:
    :type workers:
    :param no_of_items:
    :type no_of_items:
    :return:
    :rtype:
    """
    if no_of_items < workers:
        parts = no_of_items
    else:
        if no_of_items >= workers:
            parts = workers
        else:
            parts = no_of_items
    return parts