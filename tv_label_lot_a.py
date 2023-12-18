import pandas as pd
from time import time
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from multiprocessing import pool

try:
    import settings
    from utils import util_log
    from query import insert
    # instantiate logger
    log = util_log.logger()
except ImportError:
    raise Exception("import util files failed")

thick_mode = False
results = []
status = False

try:
    start = time()
    start_trx = time()
    print('start process at: %s' % start_trx)

    gompb_engine = sqlalchemy.create_engine(
    f'oracle+oracledb://{settings.GOMPB_DICT["USER"]}:{settings.GOMPB_DICT["PASSWORD"]}@{settings.GOMPB_DICT["HOST"]}:{settings.GOMPB_DICT["PORT"]}/?service_name={settings.GOMPB_DICT["SERVICE_NAME"]}',
    thick_mode=thick_mode, pool_pre_ping=True)
    print('gompb engine creation...', gompb_engine)

    sql_tv_label_lot_a = """SELECT * from tv_label_lot_a WHERE ID_KES IS NULL"""

    df_trx_lot_newt = pd.read_sql(sql_tv_label_lot_a, gompb_engine)
    print('Row count:', len(df_trx_lot_newt))
    print('Read_sql time for TV_LABEL_LOT_A: {:.3f}s'.format(time() - start_trx))

    start_scm = time()

    sql_scm_label_lot = """
                SELECT * FROM v_scm_label_lot_a WHERE ID_KES IS NULL and rn = 1
                """

    df_scm = pd.read_sql(sql_scm_label_lot, gompb_engine)
    print('Row count:', len(df_scm))
    print('Read_sql time for V_SCM_LABEL_LOT_A: {:.3f}s'.format(time() - start_scm))

    exit()

    if len(df_scm) != len(df_trx_lot_newt) and len(df_scm) > len(df_trx_lot_newt):

        # -----------------------------------
        # process data
        merge_columns = ['upi', 'case_id_new']

        merged_df = pd.merge(df_trx_lot_newt, df_scm, how='outer', left_on=merge_columns, right_on=merge_columns, suffixes=('_target', '_source'), indicator=True)
        insert_rows = merged_df[merged_df['_merge'] == 'right_only'].drop('_merge', axis=1)
        filtered_df = insert_rows[insert_rows.columns.drop(list(insert_rows.filter(regex='_target')))]

        print("#---------------------------------------------------------")
        print("\n")
        print("Filtered Rows to show only _source suffix")
        print("\n")
        print(len(filtered_df), filtered_df)
        print("\n")
        print("#---------------------------------------------------------")

        # col = list(filtered_df.columns.values)
        # print(len(col), col)

        # -----------------------------------
        # insert data to tv_trx_lot
        data_to_insert = filtered_df.values.tolist()

        if len(filtered_df) > 0:    

            sql_insert_new = """
                INSERT INTO tv_trx_lot_newt (
                    upi, 
                    case_id_new,

                    branch_code,
                    case_id,
                    val_date, 
                    feree, 
                    feree_id,
                    address,
                    scheme_code,
                    scheme_name
                ) 
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)
                """
            
            data_to_insert = filtered_df.values.tolist()

            for d in data_to_insert:
                print(d)
                insert(log, sql_insert_new, d)
                status = True

            # result = insert(log, sql_insert_new, data_to_insert)
            print(status)

    if status:
        end = time()
        elapsed_time = end - start
        print('process completed for: {0:3f}s'.format(elapsed_time))
    else:
        end = time()
        elapsed_time = end - start
        print('process completed for: {0:3f}s'.format(elapsed_time))
        print('DB up to date.')

except SQLAlchemyError as e:
    print(e)