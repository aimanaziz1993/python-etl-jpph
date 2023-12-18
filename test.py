# import getpass
# import oracledb

# """
# connection params
# un -> username
# cs -> connection string e.g hostname:port/servicename
# pw -> password
# """

# NVIS_USER = 'NVIS'
# NVIS_CONNECTION_STRING = '10.23.170.51/nvisprod.jpphvis.com'
# # pw = getpass.getpass(f'Enter password for {un}@{cs}: ')
# NVIS_PWD = 'Welcome1'

# GOMPB_USER = 'GOMPB'
# GOMPB_CONNECTION_STRING = '10.23.170.51/nvisprod.jpphvis.com'
# GOMPB_PWD = 'Welcome1'

# with oracledb.connect(user=GOMPB_USER, password=GOMPB_PWD, dsn=GOMPB_CONNECTION_STRING) as connection:
#     with connection.cursor() as cursor:

#         sql = """select * from t_scm_nvis_pyan"""
#         # sql = """select sysdate from dual"""
#         for r in cursor.execute(sql):
#             print(r)

"""
Test logic condition
"""

import pandas as pd

# Create two sample dataframes
source_df = pd.DataFrame({
    'upi': ['1006470000052886', '1006470000052886', '1006470000052886', '1006470000052886', '1006470000052999'],
    'case_id': ['1002-122022-21-000005', '1002-122022-21-000005', '1002-122022-21-000005', '1002-122022-21-000005', '6002-122322-99-009999'],
    'bld_type': ['pencawang', 'pondok', 'lain-lain', 'tiang', 'baru'],
    'bld_code': [198, 204, 299, 199, 999.9],
    'bld_type2': ['test1', 'test2', 'test3', 'test4', 'test5']
})

target_df = pd.DataFrame({
    'upi': ['1006470000052886', '1006470000052886', '1006470000052886', '1006470000052886'],
    'case_id': ['1002-122022-21-000005', '1002-122022-21-000005', '1002-122022-21-000005', '1002-122022-21-000005'],
    'bld_type': ['pencawang', 'pondok', 'lain-lain', 'tiang'],
    'bld_code': [198, 204, 299, 199],
    'bld_type2': ['test1', 'test2', 'test3', 'test4']
})

# Specify columns to use for merging
merge_columns = ['upi', 'case_id', 'bld_type', 'bld_code']

# Merge DataFrames on specified columns
merged_df = pd.merge(target_df, source_df, how='outer', left_on=merge_columns, right_on=merge_columns, suffixes=('_target', '_source'), indicator=True)
"""
CASE: If row count not the same, to check and combine rows for updating and new rows for inserting
"""
# # Create a mask for rows that need to be updated
# update_mask = (merged_df['_merge'] == 'both')

# # Update existing rows and insert new rows
# result_df = (
#     target_df[merge_columns].combine_first(source_df[merge_columns])
#     .merge(merged_df[update_mask][['upi', 'case_id', 'bld_type', 'bld_code']], how='left', on=merge_columns)
# )


# # Display the result
# print("#---------------------------------------------------------")
# print("\n")
# print("Initial Merge Results")
# print("\n")
# print(merged_df)
# print("#---------------------------------------------------------")


# print("#---------------------------------------------------------")
# print("\n")
# print("Final DataFrame to include in insert or update query:")
# print("\n")
# print(result_df)
# print("#---------------------------------------------------------")


"""
CASE: insert new row only for kes siap
"""
# Separate into update and insert DataFrames
non_match_rows = merged_df[merged_df['_merge'] != 'both']
update_rows = merged_df[merged_df['_merge'] == 'both']
insert_rows = merged_df[merged_df['_merge'] == 'right_only'].drop('_merge', axis=1)

filtered_df = insert_rows[insert_rows.columns.drop(list(insert_rows.filter(regex='_target')))]

data_to_insert = filtered_df.values.tolist()
print('data', data_to_insert)


print("#---------------------------------------------------------")
print("\n")
print("Non matching rows to be inserted")
print("\n")
print(non_match_rows)
print("#---------------------------------------------------------")

print("#---------------------------------------------------------")
print("\n")
print("To be updated rows")
print("\n")
print(update_rows)
print("#---------------------------------------------------------")

print("#---------------------------------------------------------")
print("\n")
print("To be inserted rows")
print("\n")
print(insert_rows)
print("#---------------------------------------------------------")

# print("#---------------------------------------------------------")
# print("\n")
# print("To be inserted filtered rows")
# print("\n")
# print(filtered_rows)
# print("#---------------------------------------------------------")

# Update existing rows
target_df.update(update_rows[['upi', 'case_id', 'bld_type', 'bld_code']])

# Insert new rows
target_df = pd.concat([target_df, insert_rows[['upi', 'case_id', 'bld_type', 'bld_code']]], ignore_index=True)

# Display the result
# print("\nTarget DataFrame:")
# print(target_df)