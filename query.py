
import settings
from utils.util_oracle import oracle_conn_init, oracle_conn_close
from pprint import pprint
import oracledb
from json import dumps

def insert(log, sql, data):
    try:
        connection  = oracle_conn_init(log, settings.CONNECTION_DICT)
        cursor = connection.cursor()

        # try:
        #     cursor.execute(sql, data)
        # except oracledb.DataError as e:
        #     pprint('dataerr', e)
        # except oracledb.DatabaseError as e:
        #     pprint('dberr', e)
        #     log.error(e)

        # Execute the insert operation
        cursor.executemany(sql, [data])

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        oracle_conn_close(connection)
        # cursor.close()
        # connection.close()
        d = dict()
        d['status'] = True
        d['type'] = 'ok'
        return d
    except oracledb.IntegrityError as e:
        d = dict()
        error_obj, = e.args
        print("Error Code:", error_obj.code)
        print("Error Full Code:", error_obj.full_code)
        print("Error Message:", error_obj.message)
        d['status'] = False
        d['type'] = 'integrity_err'
        return d
    except oracledb.DatabaseError as e:
        d = dict()
        error_obj, = e.args
        print("Database error")
        print("Error Code:", error_obj.code)
        print("Error Full Code:", error_obj.full_code)
        print("Error Message:", error_obj.message)
        d['status'] = False
        d['type'] = 'database_err'
        return d
    except Exception as e:
        pprint(e)
        return False
    
def insert_with_spatial(log, sql, data, spatial):
    """
    Function to insert with SQL query with a spatial geometry column
    :param log: logger object
    :param sql: query string
    :param data: data array
    :param spatial: spatial object
    :return: boolean
    """

    try:
        # Connect to databases
        connection = oracle_conn_init(log, settings.CONNECTION_DICT)

        # prepare sdo_geometry object
        typeObj = connection.gettype("SDO_GEOMETRY")
        elementInfoTypeObj = connection.gettype("SDO_ELEM_INFO_ARRAY")
        ordinateTypeObj = connection.gettype("SDO_ORDINATE_ARRAY")
        obj = typeObj.newobject()
        obj.SDO_GTYPE = int(spatial["SDO_GTYPE"])
        obj.SDO_SRID = int(spatial["SDO_SRID"])
        obj.SDO_ELEM_INFO = elementInfoTypeObj.newobject()
        obj.SDO_ELEM_INFO.extend(spatial["SDO_ELEM_INFO"])
        obj.SDO_ORDINATES = ordinateTypeObj.newobject()
        obj.SDO_ORDINATES.extend(spatial["SDO_ORDINATES"])

        # append spatial obj into data
        data.append(obj)

        # Setup cursors
        cursor = connection.cursor()

        # Execute statement
        cursor.execute(sql, data)

        connection.commit()
        oracle_conn_close(connection)

        d = dict()
        d['status'] = True
        d['type'] = 'ok'
        return d
    except oracledb.IntegrityError as e:
        d = dict()
        error_obj, = e.args
        print("Error Code:", error_obj.code)
        print("Error Full Code:", error_obj.full_code)
        print("Error Message:", error_obj.message)
        d['status'] = False
        d['type'] = 'integrity_err'
        return d
    except oracledb.DatabaseError as e:
        d = dict()
        error_obj, = e.args
        print(error_obj)
        d['status'] = False
        d['type'] = 'database_err'
        return d
    except Exception as e:
        print(e)
        return False
    

def delete(log, sql, upi):
    try:
        # Connect to databases
        connection = oracle_conn_init(log, settings.CONNECTION_DICT)

        # Setup cursors
        cursor = connection.cursor()

        # Execute statement
        cursor.execute(sql, [upi])

        connection.commit()
        oracle_conn_close(connection)

        d = dict()
        d['status'] = True
        return d
    except oracledb.IntegrityError as e:
        d = dict()
        error_obj, = e.args
        print("Error Code:", error_obj.code)
        print("Error Full Code:", error_obj.full_code)
        print("Error Message:", error_obj.message)
        d['status'] = False
        d['type'] = 'integrity_err'
        return d
    except oracledb.DatabaseError as e:
        d = dict()
        error_obj, = e.args
        print(error_obj)
        d['status'] = False
        d['type'] = 'database_err'
        return d
    except Exception as e:
        print(e)
        return False