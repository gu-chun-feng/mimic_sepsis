import math
import os
import random

import pandas as pd
from fancyimpute import IterativeImputer

from configuration import OUTPUT_DIR, \
    LEGAL_COLUMNS, PREDICT_TARGET, THRESHOLD


def mprint(info):
    print(info)


def read_excel(file_name):
    file_name = PREDICT_TARGET + "-" + file_name
    return pd.read_excel(os.path.join(OUTPUT_DIR, file_name))


def write_excel(df, file_name, sheet):
    file_name = PREDICT_TARGET + "-" + file_name
    with pd.ExcelWriter(os.path.join(OUTPUT_DIR, file_name)) as writer:
        df.to_excel(writer, sheet_name=sheet, index=False)


def filter_out_data(df, columns):
    df_columns = df.loc[:, columns]
    is_nan_df = df_columns.isna()
    row_threshold = []
    for nan_data in is_nan_df.values:
        row_data_useful = [data for data in nan_data if not data]
        row_threshold.append(len(row_data_useful) / len(columns))
    useful_row_index = [index for index, element in enumerate(row_threshold) if element >= THRESHOLD]
    if PREDICT_TARGET == 'sepsis':
        row_index = [index for index, row in df.iterrows() if not math.isnan(row[PREDICT_TARGET])]
    elif PREDICT_TARGET == 'death28':
        row_index = [index for index, row in df.iterrows() if row['sepsis'] == 1 and row['sepsis_ali'] == 1
                     and (not math.isnan(row[PREDICT_TARGET]))]
    else:
        row_index = [index for index, row in df.iterrows() if row['sepsis'] == 1
                     and (not math.isnan(row[PREDICT_TARGET]))]
    row_index = [index for index, row in df.iterrows()
                 if (index in row_index and index in useful_row_index)]
    return df.iloc[row_index]


def df_convert_number(df):
    if df.get('gender') is not None:
        df['gender'] = df['gender'].replace({'F': 1, 'M': 0})
    if df.get('death28') is not None:
        df['death28'] = df['death28'].replace({'t': 1, 'f': 0})
    if df.get('sepsis') is not None:
        df['sepsis'] = df['sepsis'].replace({'t': 1, 'f': 0})
    if df.get('sepsis_aki') is not None:
        df['sepsis_aki'] = df['sepsis_aki'].replace({'t': 1, 'f': 0})
    if df.get('sepsis_li') is not None:
        df['sepsis_li'] = df['sepsis_li'].replace({'t': 1, 'f': 0})
    if df.get('sepsis_co') is not None:
        df['sepsis_co'] = df['sepsis_co'].replace({'t': 1, 'f': 0})
    if df.get('sepsis_ali') is not None:
        df['sepsis_ali'] = df['sepsis_ali'].replace({'t': 1, 'f': 0})
    return df


def get_column_names(df, start_name, end_name):
    columns = df.columns.tolist()
    start_index = columns.index(start_name)
    end_index = columns.index(end_name) + 1
    columns = df.columns.tolist()
    return [name for name in columns[start_index:end_index] if name in LEGAL_COLUMNS]


def multiple_imputation_imputer(df, imputer_columns, random_state=0):
    df = df.reset_index(drop=True)
    df_column = df.loc[:, imputer_columns]
    # 创建 IterativeImputer 对象
    iterative_imputer = IterativeImputer(max_iter=100, verbose=3, tol=0.1, n_nearest_features=3,
                                         random_state=random_state)
    # 对数据集进行多重插补
    imputed_data = iterative_imputer.fit_transform(df_column)
    if df_column.size != imputed_data.size:
        mprint("multiple_imputation error.")
        raise ValueError("multiple_imputation failed.")
    imputed_df = pd.DataFrame(imputed_data, columns=imputer_columns)
    df.update(imputed_df)
    return df


def balance_df(df, random_state=0):
    df = df.reset_index(drop=True)
    more, less = [], []
    for index, row in df.iterrows():
        if row[PREDICT_TARGET] > 0.5:
            less.append(index)
        else:
            more.append(index)
    if len(less) > len(more):
        less, more = more, less
    multiple = math.ceil(len(more) / len(less))
    random.seed(random_state)
    less = random.sample(less * multiple, len(more))
    return df.iloc[less + more, :]
