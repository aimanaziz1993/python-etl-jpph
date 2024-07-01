import pandas as pd
from time import time
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import json
import datetime

try:
    import oracledb
    import settings
    from utils import util_log, util_spatial
    from query import insert_with_spatial, delete
    # instantiate logger
    log = util_log.logger()
except ImportError:
    raise Exception("import util files failed")

thick_mode = True
results = []
status = False

try:
    start = time()
    start_trx = time()
    print('start process at: %s' % start_trx)

    gompb_engine = sqlalchemy.create_engine(settings.CONNECTION_STRING, pool_pre_ping=True)
    print('gompb engine creation...', gompb_engine)

    sql_tv_tematik_a = """SELECT * from tv_tematik_a WHERE CASE_ID IS NULL"""

    df_tematik_a = pd.read_sql(sql_tv_tematik_a, gompb_engine)
    print('SQL read time: TV_TEMATIK_A, {:.3f}s.'.format(time() - start_trx))
    print(f'{len(df_tematik_a)} rows.')

    start_scm = time()

    sql_scm_tematik_a = """
        SELECT
            UPI,
            BCODE,
            CASE_ID,
            PRP_CATEG,
            PRP_AREA,
            REPORTED_VALUE,
            AREA,
            AREA_GEO,
            MP,
            HECTARE,
            RN,
            VALUATION_DATE,
            GEOMETRY,
            STATUS
        FROM v_scm_tematik_a
        WHERE RN = 1 AND CASE_ID IS NULL"""

    df_scm = pd.read_sql(sql_scm_tematik_a, gompb_engine)
    print('SQL read time: V_SCM_TEMATIK_A, {:.3f}s.'.format(time() - start_scm))
    print(f'{len(df_scm)} rows.')

    if len(df_scm) != len(df_tematik_a) and len(df_scm) > len(df_tematik_a):

        # -----------------------------------
        # process data
        merge_columns = ['upi']

        merged_df = pd.merge(df_tematik_a, df_scm, how='outer', left_on=merge_columns, right_on=merge_columns, suffixes=('_target', '_source'), indicator=True)
        insert_rows = merged_df[merged_df['_merge'] == 'right_only'].drop('_merge', axis=1)
        filtered_df = insert_rows[insert_rows.columns.drop(list(insert_rows.filter(regex='_target')))]

        print("#---------------------------------------------------------")
        print("\n")
        print("Total filtered rows to be inserted:")
        print("\n")
        print(len(filtered_df))
        print("\n")
        print("#---------------------------------------------------------")

        # col = list(filtered_df.columns.values)
        # print(len(col), col)

        # -----------------------------------
        # insert data to tv_trx_lot
        data_to_insert = filtered_df.values.tolist()

        if len(filtered_df) > 0:    

            sql_insert_new = """
                INSERT INTO tv_tematik_a (
                    upi,
                    bcode,
                    case_id,
                    prp_categ,
                    prp_area,
                    reported_value,
                    area,
                    area_geo,
                    mp,
                    hectare,
                    rn,
                    valuation_date,
                    status,
                    geometry
                ) 
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)
                """
            
            sql_insert_swap = """
                INSERT INTO tv_tematik_a (
                    upi,
                    bcode,
                    case_id,
                    prp_categ,
                    prp_area,
                    reported_value,
                    area,
                    area_geo,
                    mp,
                    hectare,
                    rn,
                    valuation_date,
                    status,
                    geometry
                ) 
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14)
                """
            
            sql_delete_existing = ('delete from tv_tematik_a '
                'where upi = :upi')
            
            data_to_insert = filtered_df.values.tolist()

            for d in data_to_insert:
                cleaned = [None if str(x)=='nan' else x for x in d]
                spatial = cleaned.pop(12)
                
                print("\n")
                log.info(cleaned)
                print("\n")

                if isinstance(spatial, oracledb.Object):
                    spatial = util_spatial.construct_geometry(spatial)

                # cleaned.append(repr(f"sdo_geometry({int(spatial["SDO_GTYPE"])}, {int(spatial["SDO_SRID"])}, null, sdo_elem_info_array({int(spatial["SDO_ELEM_INFO"][0])}, {int(spatial["SDO_ELEM_INFO"][1])}, {spatial["SDO_ELEM_INFO"][2]}), sdo_ordinate_array({spatial["SDO_ORDINATES"]}))"))
                pass_test = insert_with_spatial(log, sql_insert_new, cleaned, spatial)
                print(pass_test['status'], pass_test['type'])

                if not pass_test['status']:
                    if pass_test['type'] == 'database_err':

                        try:
                            print("Attempt insert by swapping to original column")
                            insert_with_spatial(log, sql_insert_swap, cleaned, spatial)
                        except:
                            print("Attempt insert after exception")
                            insert_with_spatial(log, sql_insert_new, cleaned, spatial)
                            
                    elif pass_test['type'] == 'integrity_err':
                        # Often scenario for updating a new case and override existing row
                        try:
                            print(f"This row with UPI of {cleaned[0]} already existed. We should delete first and then insert back the new one ;)")
                            print("Deleting...")
                            test_deleting = delete(log, sql_delete_existing, cleaned[0])
                            print("test deleting", test_deleting)

                            pass_test2 = insert_with_spatial(log, sql_insert_new, cleaned, spatial)

                        except Exception as e:
                            print(e)
                status = True

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