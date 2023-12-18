
try:
    import oracledb
except ImportError as err:
    print(err)
    raise Exception("import util files failed")



def oracle_conn_init(log, oracle_dict):
    """
    Initiate connection to Oracle SQL
    :param log: logger object
    :type log:  object
    :param oracle_dict: oracle credentials
    :type oracle_dict: dictionary
    :return: oracle connection
    :rtype: object

    """
    # log.info(dumps(oracle_dict))
    con = None
    try:
        connection = oracledb.connect(
            user=oracle_dict["USER"],
            password=oracle_dict["PASSWORD"],
            dsn=oracle_dict["DSN"]
        )

        return connection
    except Exception as err:
        # print(err.message)
        raise Exception("init_oracle(): %s" % err.message)


def oracle_conn_close(connection):
    """
    Close oracle object
    :param conn: oracle connection object
    :type conn: object
    """
    cursor = connection.cursor()

    cursor.close()
    connection.close()

    return 'closed'