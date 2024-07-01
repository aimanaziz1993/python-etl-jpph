import pandas as pd
from time import time
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import datetime
import oracledb

try:
    import settings
    from utils import util_log, util_spatial, util_gen
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
    print('Initiated process at %s' % util_gen.gen_local_datetime())

    gompb_engine = sqlalchemy.create_engine(settings.CONNECTION_STRING, pool_pre_ping=True)
    print('gompb engine creation...', gompb_engine)

    sql_tv_label_lot_a = """SELECT * from tv_label_lot_a WHERE ID_KES IS NULL"""

    df_label_lot_a = pd.read_sql(sql_tv_label_lot_a, gompb_engine)
    print('SQL read time: TV_LABEL_LOT_A, {:.3f}s.'.format(time() - start_trx))
    print(f'{len(df_label_lot_a)} rows.')

    start_scm = time()

    sql_scm_label_lot = """SELECT 
                            BCODE,
                            NO_LOT,
                            ID_KES,
                            MAKSUD_TARIKH,
                            LUAS_TANAH,
                            HARGA_BALASAN,
                            ANALISA_BALASAN,
                            HARGA_NILAIAN,
                            ANALISA_NILAIAN,
                            SYER,
                            PRP_RESTRICT,
                            UPI,
                            RN,
                            STATUS,
                            GEOMETRY
                        FROM v_scm_label_lot_a 
                        WHERE ID_KES IS NULL 
                        AND rn = 1"""

    df_scm = pd.read_sql(sql_scm_label_lot, gompb_engine)
    print('SQL read time: V_SCM_LABEL_LOT_A, {:.3f}s.'.format(time() - start_scm))
    print(f'{len(df_scm)} rows.')

    if len(df_scm) != len(df_label_lot_a) and len(df_scm) > len(df_label_lot_a):

        # -----------------------------------
        # process data
        merge_columns = ['upi']

        merged_df = pd.merge(df_label_lot_a, df_scm, how='outer', left_on=merge_columns, right_on=merge_columns, suffixes=('_target', '_source'), indicator=True)
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
        # print("\n")

        # -----------------------------------
        # insert data to tv_trx_lot
        data_to_insert = filtered_df.values.tolist()

        if len(filtered_df) > 0:    

            sql_insert_new = """
                INSERT INTO tv_label_lot_a (
                    upi,
                    fme_rejection_code,
                    bcode,
                    no_lot,
                    id_kes,
                    maksud_tarikh,
                    luas_tanah,
                    harga_balasan,
                    analisa_balasan,
                    harga_nilaian,
                    analisa_nilaian,
                    syer,
                    prp_restrict,
                    rn,
                    status,
                    geometry
                ) 
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)
                """
            
            # sdo_geometry(2003, null, null, sdo_elem_info_array(1, 1003, 3), sdo_ordinate_array(1, 1, 5, 7))

            sql_insert_swap = """
                INSERT INTO tv_label_lot_a (
                    upi,
                    fme_rejection_code,
                    bcode,
                    no_lot,
                    id_kes,
                    maksud_tarikh,
                    luas_tanah,
                    harga_balasan,
                    analisa_balasan,
                    harga_nilaian,
                    analisa_nilaian,
                    syer,
                    prp_restrict,
                    rn,
                    status,
                    geometry
                ) 
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16)
                """
            
            sql_delete_existing = ('delete from tv_label_lot_a '
                'where upi = :upi')
            
            data_to_insert = filtered_df.values.tolist()

            for d in data_to_insert:
                cleaned = [None if str(x)=='nan' else x for x in d]
                spatial = cleaned.pop(-1)

                # print(cleaned)

                # reformat date for column: MaksudTarikh
                split = cleaned[5].split()
                reformatted_date = str or None

                if len(split) == 2:
                    dt = split[1]
                    print("dt", dt)
                    split_dt_string = dt.split("-")
                    # change month representation
                    split_dt_string[1] = split_dt_string[1].title()
                    print("split dt", split_dt_string)

                    cleaned_date_string = datetime.datetime.strptime(f"{split_dt_string[0]}-{split_dt_string[1]}-{split_dt_string[2]}", '%d-%b-%y')
                    reformatted_date = datetime.date.strftime(cleaned_date_string, "%d/%m/%Y")
                elif len(split) > 2:
                    dt = split[2]
                    print("dt", dt)
                    split_dt_string = dt.split("-")
                    # change month representation
                    split_dt_string[1] = split_dt_string[1].title()
                    print("split dt", split_dt_string)

                    cleaned_date_string = datetime.datetime.strptime(f"{split_dt_string[0]}-{split_dt_string[1]}-{split_dt_string[2]}", '%d-%b-%y')
                    reformatted_date = datetime.date.strftime(cleaned_date_string, "%d/%m/%Y")


                if reformatted_date is not None:
                    cleaned[5] = split[0] + " " + reformatted_date
                
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
        print('Finished process in {0:3f}s.'.format(elapsed_time))
    else:
        end = time()
        elapsed_time = end - start
        print('Finished process in {0:3f}s.'.format(elapsed_time))
        print('DB up to date.')

except SQLAlchemyError as e:
    print(e)