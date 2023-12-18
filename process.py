try:
    from time import time
    import pandas as pd
    from sqlalchemy.exc import SQLAlchemyError

    from query import insert
    import thread
except ImportError:
    raise Exception("import util files failed")


def process_tv_trx_lot(log, title, engine):
    """
    Extract, transform and load data from V_SCM_LOT_NEW into table TV_TRX_LOT
    :param title -> to write log in case of error or succcessful transaction
    :param engine -> connection creation using SQLAlchemy **(To be enhanced)
    """

    status = False

    try:
        start = time()
        start_trx = time()
        print('start process at: %s' % start_trx)

        sql_trx_lot_prev_six_month = """SELECT * from tv_trx_lot 
                                WHERE DATA_SOURCE = 'DATA_NVIS'
                                """
                            # AND VAL_DATE >= add_months(sysdate, -12)

        df_trx = pd.read_sql(sql_trx_lot_prev_six_month, engine)
        print('count trx', len(df_trx))
        print('Read_sql time for TV_TRX_LOT: {:.3f}s'.format(time() - start_trx))

        start_scm = time()

        sql_scm_lot = """SELECT
                            v.BRANCH_CODE,
                            v.UPI,
                            v.CASE_ID,
                            v.CASE_ID_NEW,
                            v.VAL_PURPOSE,
                            v.VAL_PURPOSE_CODE,
                            v.LOT,
                            v.TITLE_CODE,
                            v.TITLE_NAME,
                            v.TITLE_NO,
                            v.LAND_AREA_DISP,
                            v.LAND_AREA,
                            v.LAND_AREA_UNIT,
                            v.LAND_AREA_UNIT_CODE,
                            v.VAL_DATE,
                            v.HARGA_BALASAN_DISP,
                            v.HARGA_BALASAN,
                            v.NILAIAN_JPPH_DISP,
                            v.NILAIAN_JPPH,
                            v.VAL_CATEGORY,
                            v.VAL_CATEGORY_CODE,
                            v.SYER,
                            v.ANALISIS_B_DISP,
                            v.ANALISIS_N_DISP,
                            v.FEROR,
                            v.FEREE,
                            v.RELATIONSHIP,
                            v.TENURE_TYPE,
                            v.RESTRICTIONS,
                            v.TERMS,
                            v.ZONING,
                            v.ZONING_CODE,
                            v.LAND_CATEGORY,
                            v.LAND_CATEGORY_CODE,
                            v.AGRI_CATEGORY,
                            v.AGRI_CATEGORY_CODE,
                            v.CROP_TYPE,
                            v.CROP_TYPE_CODE,
                            v.PLOT_RATIO,
                            v.DENSITY,
                            v.BLD_AGL,
                            v.BLD_AREA,
                            v.BLD_NO_ROOMS,
                            v.BLD_PART,
                            v.BLD_PART_CODE,
                            v.BLD_TYPE,
                            v.BLD_TYPE_CODE,
                            v.DATA_SOURCE
                        FROM v_scm_lot_new v, t_lot_jpph_a l
                        WHERE v.UPI = l.UPI
                        AND v.DATA_SOURCE = 'DATA_NVIS'
                        AND v.BRANCH_CODE IS NOT NULL
                        AND v.CASE_ID_NEW IS NOT NULL
                        AND v.VAL_DATE IS NOT NULL"""

        df_scm = pd.read_sql(sql_scm_lot, engine)
        print('count scm,', len(df_scm))
        print('Read_sql time for V_SCM_LOT: {:.3f}s'.format(time() - start_scm))

        if len(df_scm) != len(df_trx) and len(df_scm) > len(df_trx):

            # -----------------------------------
            # process data
            merge_columns = ['upi', 'case_id_new', 'bld_agl', 'bld_part_code', 'bld_type_code']

            merged_df = pd.merge(df_trx, df_scm, how='outer', left_on=merge_columns, right_on=merge_columns, suffixes=('_target', '_source'), indicator=True)

            # test = thread.get_sublists(log, merged_df)
            # test2 = thread.process_user_ctrl(log, test)
            # print('test2', test2)
            # exit()

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
                    INSERT INTO tv_trx_lot (
                        upi, 
                        case_id_new,
                        bld_agl,
                        bld_part_code,
                        bld_type_code,

                        branch_code,
                        case_id,
                        val_purpose, 
                        val_purpose_code, 
                        lot, 
                        title_code, 
                        title_name, 
                        title_no, 
                        land_area_disp, 
                        land_area, 
                        land_area_unit, 
                        land_area_unit_code, 
                        val_date, 
                        harga_balasan_disp, 
                        harga_balasan, 
                        nilaian_jpph_disp, 
                        nilaian_jpph, 
                        val_category, 
                        val_category_code, 
                        syer, 
                        analisis_b_disp, 
                        analisis_n_disp, 
                        feror, 
                        feree, 
                        relationship, 
                        tenure_type, 
                        restrictions, 
                        terms, 
                        zoning, 
                        zoning_code, 
                        land_category, 
                        land_category_code, 
                        agri_category, 
                        agri_category_code, 
                        crop_type, 
                        crop_type_code, 
                        plot_ratio, 
                        density, 
                        bld_area, 
                        bld_no_rooms, 
                        bld_part, 
                        bld_type,
                        data_source
                    ) 
                    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42, :43, :44, :45, :46, :47, :48)
                    """
                
                data_to_insert = filtered_df.values.tolist()

                for d in data_to_insert:
                    cleaned = [0 if str(x)=='nan' else x for x in d]
                    print(cleaned)
                    insert(log, sql_insert_new, cleaned)
                    status = True

            if status:
                end = time()
                elapsed_time = end - start
                print('New data has inserted. Process completed for: {0:3f}s'.format(elapsed_time))
            else:
                end = time()
                elapsed_time = end - start
                print('Something went wrong. Process completed for: {0:3f}s'.format(elapsed_time))
        else:
            status = True
            end = time()
            elapsed_time = end - start
            print('DB already up to date. Process completed for: {0:3f}s'.format(elapsed_time))

        return status

    except Exception as e:
        print(e)
        status = False
        return status
    
def process_tv_trx_lot_newt(log, title, engine):
    """
    Extract, transform and load data from V_SCM_LOT_NEW into table TV_TRX_LOT
    :param lot any -> to write log in case of error or succcessful transaction
    :param engine -> connection creation using SQLAlchemy **(To be enhanced)
    """
    status = False

    try:
        start = time()
        start_trx = time()
        print('start process at: %s' % start_trx)

        sql_trx_lot_newt = """SELECT * from tv_trx_lot_newt WHERE CASE_ID IS NULL"""
                            # AND VAL_DATE >= add_months(sysdate, -12)

        df_trx_lot_newt = pd.read_sql(sql_trx_lot_newt, engine)
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

        df_scm = pd.read_sql(sql_scm_lot, engine)
        print('Row count:', len(df_scm))
        print('Read_sql time for V_SCM_LOT: {:.3f}s'.format(time() - start_scm))

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

            if status:
                end = time()
                elapsed_time = end - start
                print('New data has inserted. Process completed for: {0:3f}s'.format(elapsed_time))
            else:
                end = time()
                elapsed_time = end - start
                print('Something went wrong. Process completed for: {0:3f}s'.format(elapsed_time))
        else:
            status = True
            end = time()
            elapsed_time = end - start
            print('DB already up to date. Process completed for: {0:3f}s'.format(elapsed_time))

        return status

    except SQLAlchemyError as e:
        print(e)
        status = False
        return status
