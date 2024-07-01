import pandas as pd
from time import time
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

try:
    import settings
    from utils import util_log
    from query import insert, delete
    # instantiate logger
    log = util_log.logger()
except ImportError:
    raise Exception("import util files failed")

def run():

    thick_mode = False
    results = []
    status = False

    try:
        start = time()
        start_trx = time()
        print('start process at: %s' % start_trx)

        gompb_engine = sqlalchemy.create_engine(settings.CONNECTION_STRING, pool_pre_ping=True)
        print('gompb engine creation...', gompb_engine)

        # When CASE_ID is NULL indicator for new cases entry
        sql_trx_strata = """SELECT * from tv_trx_strata WHERE CASE_ID IS NULL"""

        df_trx = pd.read_sql(sql_trx_strata, gompb_engine)
        print('count tv_trx_strata', len(df_trx))
        print('Read_sql time for TV_TRX_STRATA: {:.3f}s'.format(time() - start_trx))

        start_scm = time()

        sql_scm_strata = """
                        SELECT
                            v.UPI,
                            v.UNIT_NO_ROOMS,
                            v.BRANCH_CODE,
                            v.CASE_ID,
                            v.CASE_ID_NEW,
                            v.VAL_DATE,
                            v.VAL_PURPOSE,
                            v.VAL_PURPOSE_CODE,
                            v.ADDRESS,
                            v.TITLE_CODE,
                            v.TITLE_NAME,
                            v.TITLE_NO,
                            v.SCHEME_CODE,
                            v.SCHEME_NAME,
                            v.UNIT_AREA_DISP,
                            v.UNIT_AREA,
                            v.UNIT_AREA_UNIT,
                            v.UNIT_AREA_UNIT_CODE,
                            v.HARGA_BALASAN_DISP,
                            v.HARGA_BALASAN,
                            v.NILAIAN_JPPH_DISP,
                            v.NILAIAN_JPPH,
                            v.VAL_CATEGORY,
                            v.VAL_CATEGORY_CODE,
                            v.SYER,
                            v.ANALISIS_B_DISP,
                            v.ANALISIS_B,
                            v.ANALISIS_N_DISP,
                            v.ANALISIS_N,
                            v.FEREE,
                            v.FEROR,
                            v.RELATIONSHIP,
                            v.REMARKS,
                            v.RESTRICTIONS,
                            v.TERMS,
                            v.TENURE_TYPE
                        FROM v_scm_strata_new v, t_strata s
                        WHERE v.UPI = s.UPI_PETAK
                        AND v.CASE_ID IS NULL
                        """

        df_scm = pd.read_sql(sql_scm_strata, gompb_engine)
        print('count scm,', len(df_scm))
        print('Read_sql time for V_SCM_STRATA: {:.3f}s'.format(time() - start_scm))

        if len(df_scm) != len(df_trx) and len(df_scm) > len(df_trx):

            # -----------------------------------
            # process data
            merge_columns = ['upi', 'unit_no_rooms']

            merged_df = pd.merge(df_trx, df_scm, how='outer', left_on=merge_columns, right_on=merge_columns, suffixes=('_target', '_source'), indicator=True)
            insert_rows = merged_df[merged_df['_merge'] == 'right_only'].drop('_merge', axis=1)
            filtered_df = insert_rows[insert_rows.columns.drop(list(insert_rows.filter(regex='_target')))]

            print("#---------------------------------------------------------")
            print("\n")
            print("Filtered Rows to show only _source suffix")
            print("\n")
            print(len(filtered_df), filtered_df)
            print("\n")
            print("#---------------------------------------------------------")

            col = list(filtered_df.columns.values)
            print(len(col), col)

            # -----------------------------------
            # insert data to tv_trx_lot
            data_to_insert = filtered_df.values.tolist()

            if len(filtered_df) > 0:    

                sql_insert_new = """
                    INSERT INTO tv_trx_strata (
                        upi,
                        unit_no_rooms,
                        branch_code,
                        case_id,
                        case_id_new,
                        val_date,
                        val_purpose,
                        val_purpose_code,
                        address,
                        title_code,
                        title_name,
                        title_no,
                        scheme_code,
                        scheme_name,
                        unit_area_disp,
                        unit_area,
                        unit_area_unit,
                        unit_area_unit_code,
                        harga_balasan_disp,
                        harga_balasan,
                        nilaian_jpph_disp,
                        nilaian_jpph,
                        val_category,
                        val_category_code,
                        syer,
                        analisis_b_disp,
                        analisis_b,
                        analisis_n_disp,
                        analisis_n,
                        feree,
                        feror,
                        relationship,
                        remarks,
                        restrictions,
                        terms,
                        tenure_type
                    ) 
                    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36)
                    """
                
                sql_delete_existing = ('delete from tv_trx_strata '
                    'where upi = :upi and unit_no_rooms = :unit_no_rooms')

                data_to_insert = filtered_df.values.tolist()

                for d in data_to_insert:
                    cleaned = [0 or None if str(x)=='nan' else x for x in d]

                    print(cleaned)
                    pass_test = insert(log, sql_insert_new, cleaned)
                    print(pass_test['status'], pass_test['type'])

                    if not pass_test['status']:
                        if pass_test['type'] == 'database_err':
                            try:
                                print("Attempt insert by swapping to original column")
                                insert(log, sql_insert_new, cleaned)
                            except:
                                print("Attempt insert after exception")
                                insert(log, sql_insert_new, cleaned)
                                
                        elif pass_test['type'] == 'integrity_err':
                            # Often scenario for updating a new case and override existing row
                            try:
                                print(f"This row with UPI of {cleaned[0]} already existed. We should delete first and then insert back the new one ;)")
                                print("Deleting...")
                                test_deleting = delete(log, sql_delete_existing, [cleaned[0], cleaned[1]])
                                print("deleting status", test_deleting)

                                pass_test2 = insert(log, sql_insert_new, cleaned)

                            except Exception as e:
                                print(e)
                    status = True
        if status:
            end = time()
            elapsed_time = end - start
            print('process completed for: {0:3f}s'.format(elapsed_time))
        else:
            print('DB up to date.')

    except SQLAlchemyError as e:
        print(e)

run()