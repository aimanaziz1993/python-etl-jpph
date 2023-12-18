
import settings
from utils.util_oracle import oracle_conn_init, oracle_conn_close
from pprint import pprint
import oracledb
from json import dumps

def insert(log, sql, data):

    try:
        connection  = oracle_conn_init(log, settings.GOMPB_DICT)
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

        return 'OK'
    except Exception as e:
        pprint(e)
        return e


def get_query_v_scm_lot(log):

    connection = oracle_conn_init(log, settings.NVIS_DICT)
    outdata = {}
    data = []

    sql = """
            SELECT
                BRANCH_CODE,
                UPI,
                CASE_ID,
                CASE_ID_NEW,
                VAL_DATE,
                VAL_PURPOSE,
                VAL_PURPOSE_CODE,
                MAKSUD_TARIKH,
                ADDRESS,
                CROP_AGE,
                CROP_TYPE_CODE,
                CROP_TYPE,
                NEGERI_CODE,
                DISTRICT_CODE,
                MUKIM_CODE,
                SECTION_CODE,
                LOT,
                TITLE_CODE,
                TITLE_NAME,
                TITLE_NO,
                GRID,
                SCHEME_CODE,
                SCHEME_NAME,
                LAND_AREA_DISP,
                LAND_AREA,
                LAND_AREA_UNIT,
                LAND_AREA_UNIT_CODE,
                HARGA_BALASAN_DISP,
                HARGA_BALASAN,
                NILAIAN_JPPH_DISP,
                NILAIAN_JPPH,
                VAL_CATEGORY,
                VAL_CATEGORY_CODE,
                SYER,
                ANALISIS_B_DISP,
                ANALISIS_B,
                ANALISIS_N_DISP,
                ANALISIS_N,
                FEREE,
                FEREE_ID,
                FEROR,
                RELATIONSHIP,
                JENIS_TANAH,
                LAND_CATEGORY,
                LAND_CATEGORY_CODE,
                LAND_CLASS,
                LAND_ONLY_ANALYSIS_B,
                LAND_ONLY_ANALYSIS_N,
                LAND_USED,
                LAND_USED_CODE,
                BLD_ANALYSIS_B,
                BLD_ANALYSIS_N,
                BLD_VALUE,
                BLD_CONDITION,
                BLD_CONDITION_CODE,
                BLD_TYPE,
                BLD_TYPE_CODE,
                BLD_AGL,
                BLD_AREA,
                BLD_NO_ROOMS,
                BLD_PART,
                BLD_PART_CODE,
                BLD_USAGE,
                BLD_USAGE_CODE,
                LOT_SHAPE,
                LOT_STATUS,
                LOT_TYPE,
                PRP_CONDITION,
                DENSITY,
                PLOT_RATIO,
                PRP_REMARKS,
                RESTRICTIONS,
                TERMS,
                STRUCTURE_CONDITION,
                TENURE_TYPE,
                ZONING,
                ZONING_CODE,
                STATUS,
                REMARKS,
                AGRI_CATEGORY,
                AGRI_CATEGORY_CODE,
                DATA_SOURCE
            FROM
            (
                -- NEW CASES --
                SELECT
                v1.BRANCH_CODE,
                v1.UPI,
                v1.CASE_ID,
                v1.CASE_ID_NEW,
                v1.VAL_DATE,
                v1.VAL_PURPOSE,
                v1.VAL_PURPOSE_CODE,
                CASE WHEN v1.VAL_PURPOSE_CODE = '01' THEN 'BP ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '02' THEN 'SMK ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '03' THEN 'CKHT ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '04' THEN 'HP ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '05' THEN 'DS ' || TO_CHAR (v1.VAL_DATE) WHEN v1.VAL_PURPOSE_CODE = '06' THEN 'GS ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '07' THEN 'HLLS ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '08' THEN 'HK ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '09' THEN 'Ins ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '10' THEN 'Iz ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '11' THEN 'JPA ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '12' THEN 'JPA ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '13' THEN 'Kad ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '14' THEN 'KT ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '15' THEN 'MT ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '16' THEN 'NASI ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '17' THEN 'NA ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '18' THEN 'JB ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '19' THEN 'AR ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '20' THEN 'PT ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '21' THEN 'Swasta ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '22' THEN 'PP ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '23' THEN 'Sew Thn ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '24' THEN 'Sew ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '25' THEN 'TS ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '26' THEN 'BPKT ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '27' THEN 'PH ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '28' THEN 'LJ ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '29' THEN 'BMAL ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '30' THEN 'HPK ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '31' THEN 'CAG ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '32' THEN 'PAM ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '33' THEN 'UPT ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '34' THEN 'KK ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '35' THEN 'PA ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '36' THEN 'PMT ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '37' THEN 'PKBI ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '38' THEN 'GSR ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '39' THEN 'LPS ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '40' THEN 'AKR ' || v1.VAL_DATE WHEN v1.VAL_PURPOSE_CODE = '99' THEN 'Tidak Digunapakai Lagi ' || v1.VAL_DATE END AS MAKSUD_TARIKH,
                v1.ADDRESS,
                v1.CROP_AGE,
                v1.CROP_CODE AS CROP_TYPE_CODE,
                v1.CROP_TYPE,
                v1.NEGERI_CODE,
                v1.DISTRICT_CODE,
                v1.MUKIM_CODE,
                v1.SECTION_CODE,
                v1.PRP_LOT_NO AS LOT,
                v1.OWNER_TYPE_CODE AS TITLE_CODE,
                t.SINGKATAN || ' - ' || v1.OWNER_TYPE AS TITLE_NAME,
                v1.OWNER_NO AS TITLE_NO,
                v1.GRID,
                v1.SCHEME_CODE,
                v1.SCHEME_NAME,
                CASE WHEN v1.LAND_AREA_UNIT_CODE = '1' THEN TRIM(
                    (TO_CHAR ((v1.LAND_AREA), '999,999,990.0000'))
                ) || ' DEPA' WHEN v1.LAND_AREA_UNIT_CODE = '2' THEN TRIM(
                    (TO_CHAR ((v1.LAND_AREA), '999,999,990.0000'))
                ) || ' EKAR (ACRE)' WHEN v1.LAND_AREA_UNIT_CODE = '3' THEN TRIM(
                    (TO_CHAR ((v1.LAND_AREA), '999,999,990.0000'))
                ) || ' hek' WHEN v1.LAND_AREA_UNIT_CODE = '4' THEN TRIM(
                    (TO_CHAR ((v1.LAND_AREA), '999,999,990.0000'))
                ) || ' JEMBA' WHEN v1.LAND_AREA_UNIT_CODE = '5' THEN TRIM(
                    (TO_CHAR ((v1.LAND_AREA), '999,999,990.0000'))
                ) || ' kp' WHEN v1.LAND_AREA_UNIT_CODE = '6' THEN TRIM(
                    (TO_CHAR ((v1.LAND_AREA), '999,999,990.0000'))
                ) || ' mp' WHEN v1.LAND_AREA_UNIT_CODE = '7' THEN TRIM(
                    (TO_CHAR ((v1.LAND_AREA), '999,999,990.0000'))
                ) || ' POLE' WHEN v1.LAND_AREA_UNIT_CODE = '8' THEN TRIM(
                    (TO_CHAR ((v1.LAND_AREA), '999,999,990.0000'))
                ) || ' RELONG' WHEN v1.LAND_AREA_UNIT_CODE = '9' THEN TRIM(
                    (TO_CHAR ((v1.LAND_AREA), '999,999,990.0000'))
                ) || ' ROOD' END AS LAND_AREA_DISP,
                v1.LAND_AREA,
                v1.LAND_AREA_UNIT,
                v1.LAND_AREA_UNIT_CODE,
                CASE WHEN V1.DECLARED_VALUE > 0 THEN TRIM(
                    (
                    TO_CHAR (v1.DECLARED_VALUE, '999,999,999,999.00')
                    )
                ) -- CASE
                -- WHEN V1.VAL_CATEGORY_CODE = '1' THEN TRIM( TO_CHAR (V1.DECLARED_VALUE, '999,999,999.00') ) || ' NQH'
                -- WHEN V1.VAL_CATEGORY_CODE = '2' THEN TRIM( TO_CHAR (V1.DECLARED_VALUE, '999,999,999.00') ) || ' NQF'
                -- WHEN v1.VAL_CATEGORY_CODE = '3' THEN TRIM( TO_CHAR (v1.DECLARED_VALUE, '999,999,999.00') ) || ' PENILAIAN'
                -- WHEN v1.VAL_CATEGORY_CODE = '4' THEN TRIM( TO_CHAR (v1.DECLARED_VALUE, '999,999,999.00') ) || ' KASIH SAYANG'
                -- ELSE TRIM( TO_CHAR (v1.DECLARED_VALUE, '999,999,999.00') )
                -- END
                ELSE '0.00' END AS HARGA_BALASAN_DISP,
                v1.DECLARED_VALUE AS HARGA_BALASAN,
                CASE WHEN V1.NILAIAN_JPPH > 0 THEN TRIM(
                    (TO_CHAR (v1.NILAIAN_JPPH, '999,999,999,999.00'))
                ) ELSE '0.00' END AS NILAIAN_JPPH_DISP,
                v1.NILAIAN_JPPH,
                v1.VAL_CATEGORY,
                v1.VAL_CATEGORY_CODE,
                v1.SHARE_NUM || '/' || v1.SHARE_DENOM AS SYER,
                CASE WHEN v1.LAND_ANALYSIS_B > 0 THEN CASE WHEN v1.LAND_AREA_UNIT_CODE = '1' THEN TRIM(v1.LAND_ANALYSIS_B) || ' DEPA' WHEN v1.LAND_AREA_UNIT_CODE = '2' THEN TRIM(v1.LAND_ANALYSIS_B) || ' EKAR (ACRE)' WHEN v1.LAND_AREA_UNIT_CODE = '3' THEN TRIM(
                    (
                    TO_CHAR ((v1.LAND_ANALYSIS_B), '999,999,999.00')
                    )
                ) || ' shek' WHEN v1.LAND_AREA_UNIT_CODE = '4' THEN TRIM(v1.LAND_ANALYSIS_B) || ' JEMBA' WHEN v1.LAND_AREA_UNIT_CODE = '5' THEN TRIM(v1.LAND_ANALYSIS_B) || ' kp' WHEN v1.LAND_AREA_UNIT_CODE = '6' THEN TRIM(
                    (
                    TO_CHAR ((v1.LAND_ANALYSIS_B), '999,999,999.00')
                    )
                ) || ' smp' WHEN v1.LAND_AREA_UNIT_CODE = '7' THEN TRIM(v1.LAND_ANALYSIS_B) || ' POLE' WHEN v1.LAND_AREA_UNIT_CODE = '8' THEN TRIM(v1.LAND_ANALYSIS_B) || ' RELONG' WHEN v1.LAND_AREA_UNIT_CODE = '9' THEN TRIM(v1.LAND_ANALYSIS_B) || ' ROOD' END ELSE CASE WHEN v1.LAND_AREA_UNIT_CODE = '3' THEN '0.00 shek' WHEN v1.LAND_AREA_UNIT_CODE = '6' THEN '0.00 smp' END END AS ANALISIS_B_DISP,
                v1.LAND_ANALYSIS_B AS ANALISIS_B,
                CASE WHEN v1.LAND_ANALYSIS_N > 0 THEN CASE WHEN v1.LAND_AREA_UNIT_CODE = '1' THEN TRIM(v1.LAND_ANALYSIS_N) || ' DEPA' WHEN v1.LAND_AREA_UNIT_CODE = '2' THEN TRIM(v1.LAND_ANALYSIS_N) || ' EKAR (ACRE)' WHEN v1.LAND_AREA_UNIT_CODE = '3' THEN TRIM(
                    (
                    TO_CHAR ((v1.LAND_ANALYSIS_N), '999,999,999.00')
                    )
                ) || ' shek' WHEN v1.LAND_AREA_UNIT_CODE = '4' THEN TRIM(v1.LAND_ANALYSIS_N) || ' JEMBA' WHEN v1.LAND_AREA_UNIT_CODE = '5' THEN TRIM(v1.LAND_ANALYSIS_N) || ' kp' WHEN v1.LAND_AREA_UNIT_CODE = '6' THEN TRIM(
                    (
                    TO_CHAR ((v1.LAND_ANALYSIS_N), '999,999,999.00')
                    )
                ) || ' smp' WHEN v1.LAND_AREA_UNIT_CODE = '7' THEN TRIM(v1.LAND_ANALYSIS_N) || ' POLE' WHEN v1.LAND_AREA_UNIT_CODE = '8' THEN TRIM(v1.LAND_ANALYSIS_N) || ' RELONG' WHEN v1.LAND_AREA_UNIT_CODE = '9' THEN TRIM(v1.LAND_ANALYSIS_N) || ' ROOD' END ELSE CASE WHEN v1.LAND_AREA_UNIT_CODE = '3' THEN '0.00 shek' WHEN v1.LAND_AREA_UNIT_CODE = '6' THEN '0.00 smp' END END AS ANALISIS_N_DISP,
                v1.LAND_ANALYSIS_N AS ANALISIS_N,
                v1.FEREE,
                CAST (
                    '' AS VARCHAR2 (20 BYTE)
                ) AS FEREE_ID, 
                --  to add later
                v1.FEROR,
                CASE WHEN v1.RELATIONSHIP = 'T' THEN 'TIADA HUBUNGAN' WHEN v1.RELATIONSHIP = 'Y' THEN 'ADA HUBUNGAN' END AS RELATIONSHIP,
                v1.JENIS_TANAH,
                v1.CATEGORY_CODE || ' - ' || v1.LAND_CATEGORY AS LAND_CATEGORY,
                v1.CATEGORY_CODE AS LAND_CATEGORY_CODE,
                v1.LAND_CLASS,
                v1.LAND_ONLY_ANALYSIS_B,
                v1.LAND_ONLY_ANALYSIS_N,
                v1.LAND_USED,
                v1.LAND_USED_CODE,
                v1.BUILD_ANALYSIS_B AS BLD_ANALYSIS_B,
                v1.BUILD_ANALYSIS_N AS BLD_ANALYSIS_N,
                v1.BUILDING_VALUE AS BLD_VALUE,
                v1.KEADAAN_BANGUNAN AS BLD_CONDITION,
                v1.KOD_KEADAAN_BANGUNAN AS BLD_CONDITION_CODE,
                v1.BLD_TYPE,
                v1.BLD_TYPE_CODE,
                v1.AGL AS BLD_AGL,
                v1.BUILDING_AREA AS BLD_AREA,
                v1.NO_OF_ROOMS AS BLD_NO_ROOMS,
                v1.BUILDING_PART AS BLD_PART,
                v1.BUILDING_PART_CODE AS BLD_PART_CODE,
                v1.BLD_USAGE,
                v1.BLD_USAGE_CODE,
                v1.LOT_SHAPE,
                v1.LOT_STATUS,
                v1.LOT_TYPE,
                v1.PHY_COND AS PRP_CONDITION,
                v1.DENSITY,
                v1.PLAN_RATIO AS PLOT_RATIO,
                v1.PRP_REMARKS,
                v1.RESTRICTIONS,
                v1.TERMS,
                v1.STRUCTURE_CONDITION,
                v1.TENURE_TYPE,
                v1.ZONING,
                v1.ZONING_CODE,
                CAST ('SIAP' AS VARCHAR2 (8 BYTE)) AS STATUS,
                CAST ('' AS VARCHAR2 (200 BYTE)) AS REMARKS,
                CAST (
                    '' AS VARCHAR2 (30 BYTE)
                ) AS AGRI_CATEGORY,
                CAST ('' AS NUMBER) AS AGRI_CATEGORY_CODE,
                v1.DATA_SOURCE
                FROM
                    NVIS.T_SCM_NVIS_PYAN v1
                    LEFT JOIN NVIS.T_REF_TITLE_ABBR T ON V1.OWNER_TYPE_CODE = T.KOD -- Only for LOT
                WHERE v1.UPI NOT LIKE '%(B)%' AND v1.DATA_SOURCE = 'DATA_NVIS'
            )
        """
    
    with connection.cursor() as cursor:
        
        cursor.execute(sql)
        result = cursor.fetchall()

        # for r in cursor.execute(sql):
        #     outdata = {
        #         "BRANCH_CODE": r[0],
        #         "UPI": r[1],
        #         "CASE_ID": r[2],
        #         "CASE_ID_NEW": r[3],
        #         "VAL_DATE": r[4],
        #         "VAL_PURPOSE": r[5],
        #         "VAL_PURPOSE_CODE": r[6],
        #         "MAKSUD_TARIKH": r[7],
        #         "ADDRESS": r[8],
        #         "CROP_AGE": r[9],
        #         "CROP_TYPE_CODE": r[10],
        #         "CROP_TYPE": r[11],
        #         "NEGERI_CODE": r[12],
        #         "DISTRICT_CODE": r[13],
        #         "MUKIM_CODE": r[14],
        #         "SECTION_CODE": r[15],
        #         "LOT": r[16],
        #         "TITLE_CODE": r[17],
        #         "TITLE_NAME": r[18],
        #         "TITLE_NO": r[19],
        #         "GRID": r[20],
        #         "SCHEME_CODE": r[21],
        #         "SCHEME_NAME": r[22],
        #         "LAND_AREA_DISP": r[23],
        #         "LAND_AREA": r[24],
        #         "LAND_AREA_UNIT": r[25],
        #         "LAND_AREA_UNIT_CODE": r[26],
        #         "HARGA_BALASAN_DISP": r[27],
        #         "HARGA_BALASAN": r[28],
        #         "NILAIAN_JPPH_DISP": r[29],
        #         "NILAIAN_JPPH": r[30],
        #         "VAL_CATEGORY": r[31],
        #         "VAL_CATEGORY_CODE": r[32],
        #         "SYER": r[33],
        #         "ANALISIS_B_DISP": r[34],
        #         "ANALISIS_B": r[35],
        #         "ANALISIS_N_DISP": r[36],
        #         "ANALISIS_N": r[37],
        #         "FEREE": r[38],
        #         "FEREE_ID": r[39],
        #         "FEROR": r[40],
        #         "RELATIONSHIP": r[41],
        #         "JENIS_TANAH": r[42],
        #         "LAND_CATEGORY": r[43],
        #         "LAND_CATEGORY_CODE": r[44],
        #         "LAND_CLASS": r[45],
        #         "LAND_ONLY_ANALYSIS_B": r[46],
        #         "LAND_ONLY_ANALYSIS_N": r[47],
        #         "LAND_USED": r[48],
        #         "LAND_USED_CODE": r[49],
        #         "BLD_ANALYSIS_B": r[50],
        #         "BLD_ANALYSIS_N": r[51],
        #         "BLD_VALUE": r[52],
        #         "BLD_CONDITION": r[53],
        #         "BLD_CONDITION_CODE": r[54],
        #         "BLD_TYPE": r[55],
        #         "BLD_TYPE_CODE": r[56],
        #         "BLD_AGL": r[57],
        #         "BLD_AREA": r[58],
        #         "BLD_NO_ROOMS": r[59],
        #         "BLD_PART": r[60],
        #         "BLD_PART_CODE": r[61],
        #         "BLD_USAGE": r[62],
        #         "BLD_USAGE_CODE": r[63],
        #         "LOT_SHAPE": r[64],
        #         "LOT_STATUS": r[65],
        #         "LOT_TYPE": r[66],
        #         "PRP_CONDITION": r[67],
        #         "DENSITY": r[68],
        #         "PLOT_RATIO": r[69],
        #         "PRP_REMARKS": r[70],
        #         "RESTRICTIONS": r[71],
        #         "TERMS": r[72],
        #         "STRUCTURE_CONDITION": r[73],
        #         "TENURE_TYPE": r[74],
        #         "ZONING": r[75],
        #         "ZONING_CODE": r[76],
        #         "STATUS": r[77],
        #         "REMARKS": r[78],
        #         "AGRI_CATEGORY": r[79],
        #         "AGRI_CATEGORY_CODE": r[80]
        #     }

        #     data.append(outdata)
        
    return result