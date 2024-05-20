import pandas as pd
from fancyimpute import IterativeImputer

THRESHOLD = 0.95


def filter_out_data(df, columns):
    df_columns = df.loc[:, columns]
    is_nan_df = df_columns.isna()
    row_threshold = []
    for nan_data in is_nan_df.values:
        row_data_useful = [data for data in nan_data if not data]
        row_threshold.append(len(row_data_useful) / len(columns))
    useful_row_index = [index for index, element in enumerate(row_threshold) if element >= THRESHOLD]
    return df.iloc[useful_row_index]


def multiple_imputation_imputer(df, imputer_columns, random_state=0):
    df = df.reset_index(drop=True)
    df_column = df.loc[:, imputer_columns]
    # 创建 IterativeImputer 对象
    iterative_imputer = IterativeImputer(max_iter=50, verbose=3, tol=0.2, n_nearest_features=3,
                                         random_state=random_state)
    # 对数据集进行多重插补
    imputed_data = iterative_imputer.fit_transform(df_column)
    if df_column.size != imputed_data.size:
        print("multiple_imputation error.")
        raise ValueError("multiple_imputation failed.")
    imputed_df = pd.DataFrame(imputed_data, columns=imputer_columns)
    df.update(imputed_df)
    return df


data = pd.read_excel('heparin_new.xlsx')

# column_names = list(data.columns)
# start_index = column_names.index('po2_min')
# end_index = column_names.index('inr_avg')
# column_names = column_names[start_index:end_index + 1]
# column_names.remove('gender')
# print(column_names)




full_columns = ([
                    'subject_id',
                    'hadm_id',
                    'stay_id',
                    'stay_day'
                ] +
                impute_columns +
                [
                    'gender',
                    'itemid_225975_dose',
                    'itemids'
                ])

data = filter_out_data(data, impute_columns)
data = data.loc[:, full_columns]
data = data[data['itemids'].isna()]

data = multiple_imputation_imputer(data, impute_columns, random_state=0)

# data = data[[
#                 'subject_id',
#                 'hadm_id',
#                 'stay_id',
#             ] + impute_columns + [
#                 'age',
#                 'gender',
#                 'itemid_225975_dose',
#                 'itemids'
#             ]]

# data['gender'].replace({'M': 1, 'F': 0}, inplace=True)
data['heparin'] = data['itemid_225975_dose'].apply(lambda x: True if pd.notna(x) else False)
data['per_day'] = data['itemid_225975_dose'] / data['stay_day']

data.to_excel('heparin_225975.xlsx', index=False, engine='openpyxl')
print(data)
