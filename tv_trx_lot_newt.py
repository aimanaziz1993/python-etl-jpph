import pandas as pd
from time import time
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from multiprocessing import pool

try:
    import settings
    from utils import util_log
    from query import insert, delete
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

    gompb_engine = sqlalchemy.create_engine(settings.CONNECTION_STRING)
    print('gompb engine creation...', gompb_engine)

    sql_trx_lot_newt = """SELECT * from tv_trx_lot_newt WHERE CASE_ID IS NULL"""
                        # AND VAL_DATE >= add_months(sysdate, -12)

    df_trx_lot_newt = pd.read_sql(sql_trx_lot_newt, gompb_engine)
    print('Row count:', len(df_trx_lot_newt))
    print('Read_sql time for TV_TRX_LOT_NEWT: {:.3f}s'.format(time() - start_trx))

    start_scm = time()

    sql_scm_lot = """
                SELECT 
                    t.BRANCH_CODE, 
                    t.UPI, 
                    t.CASE_ID, 
                    t.CASE_ID_NEW, 
                    t.VAL_DATE, 
                    t.FEREE, 
                    t.FEREE_ID, 
                    t.ADDRESS, 
                    t.SCHEME_CODE, 
                    t.SCHEME_NAME
                FROM (
                    SELECT BRANCH_CODE, UPI, CASE_ID, CASE_ID_NEW, VAL_DATE, FEREE, FEREE_ID, ADDRESS, SCHEME_CODE, SCHEME_NAME,
                            ROW_NUMBER() OVER (PARTITION BY UPI ORDER BY VAL_DATE DESC) AS rn
                    FROM GOMPB.V_SCM_LOT_NEW
                    WHERE UPI IN (
                        SELECT UPI
                        FROM GOMPB.T_LOT_JPPH_A
                )
                    AND BRANCH_CODE IS NOT NULL
                    AND CASE_ID_NEW IS NOT NULL
                    AND VAL_DATE IS NOT NULL
                    AND DATA_SOURCE = 'DATA_NVIS'
                ) t
                WHERE rn = 1
                """

    df_scm = pd.read_sql(sql_scm_lot, gompb_engine)
    print('Row count:', len(df_scm))
    print('Read_sql time for V_SCM_LOT: {:.3f}s'.format(time() - start_scm))

    if len(df_scm) != len(df_trx_lot_newt) and len(df_scm) > len(df_trx_lot_newt):

        # -----------------------------------
        # process data
        merge_columns = ['upi', 'case_id_new']

        merged_df = pd.merge(df_trx_lot_newt, df_scm, how='outer', left_on=merge_columns, right_on=merge_columns, suffixes=('_target', '_source'), indicator=True)
        insert_rows = merged_df[merged_df['_merge'] == 'right_only'].drop('_merge', axis=1)
        filtered_df = insert_rows[insert_rows.columns.drop(list(insert_rows.filter(regex='_target')))]

        print("#-----------------------------------------------------------------------------")
        print("\n")
        print("Filtered Rows to show only _source suffix")
        print("\n")
        print(len(filtered_df), filtered_df)
        print("\n")
        print("#-----------------------------------------------------------------------------")

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
            
            sql_insert_swap = """
                INSERT INTO tv_trx_lot_newt (
                    branch_code,
                    upi, 
                    case_id,
                    case_id_new,

                    val_date, 
                    feree, 
                    feree_id,
                    address,
                    scheme_code,
                    scheme_name
                ) 
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)
                """
            
            sql_delete_existing = ('delete from tv_trx_lot_newt '
            'where upi = :upi')
            
            data_to_insert = filtered_df.values.tolist()

            for d in data_to_insert:
                print(d)
                pass_test = insert(log, sql_insert_new, d)
                print(pass_test['status'], pass_test['type'])

                if not pass_test['status']:
                    if pass_test['type'] == 'database_err':

                        try:
                            print("Attempt insert by swapping to original column")
                            insert(log, sql_insert_swap, d)
                        except:
                            print("Attempt insert after exception")
                            insert(log, sql_insert_new, d)
                            
                    elif pass_test['type'] == 'integrity_err':
                        # Often scenario for updating a new case and override existing row
                        try:
                            print(f"This row with UPI of {d[0]} already existed. We should delete first and then insert back the new one ;)")
                            print("Deleting...")
                            test_deleting = delete(log, sql_delete_existing, d[0])
                            print("test deleting", test_deleting)

                            pass_test2 = insert(log, sql_insert_new, d)

                        except Exception as e:
                            print(e)
                status = True
            # result = insert(log, sql_insert_new, data_to_insert)

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