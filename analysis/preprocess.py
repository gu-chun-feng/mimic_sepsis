import pandas as pd

from configuration import INPUT_EXCEL, VERIFY_TYPE, TRAIN_RATIO, VERIFY_EXCEL, TARGET_COLUMNS, PREDICT_TARGET
from utils import df_convert_number, write_excel, filter_out_data, multiple_imputation_imputer, balance_df


def split_data(df, i=1987):
    train_df = df.sample(frac=TRAIN_RATIO, random_state=i)
    verify_df = df.drop(train_df.index)
    return train_df, verify_df


def add_empty_column(df, names):
    columns = df.columns.tolist()
    for name in names:
        if name not in columns:
            df[name] = ""


def output_train_verify():
    df = df_convert_number(pd.read_excel(INPUT_EXCEL.get("file_name")))
    df = filter_out_data(df, TARGET_COLUMNS)
    if VERIFY_TYPE:
        train_df, verify_df = split_data(df)
    else:
        train_df = df
        verify_df = df_convert_number(pd.read_excel(VERIFY_EXCEL.get("file_name")))
        verify_df = filter_out_data(verify_df, TARGET_COLUMNS)
    train_df = train_df.reset_index(drop=True)
    verify_df = verify_df.reset_index(drop=True)

    extra_column = INPUT_EXCEL.get("extra_column") + VERIFY_EXCEL.get("extra_column")
    full_volume_names = extra_column + TARGET_COLUMNS + [PREDICT_TARGET]
    add_empty_column(train_df, full_volume_names)
    add_empty_column(verify_df, full_volume_names)
    train_df = train_df.loc[:, full_volume_names]
    verify_df = verify_df.loc[:, full_volume_names]
    train_df = train_df.reset_index(drop=True)
    verify_df = verify_df.reset_index(drop=True)
    train_df['model'] = 1
    verify_df['model'] = 0
    model_df = pd.concat([train_df, verify_df])
    write_excel(model_df, "clean_data.xlsx", 'clean_data')
    return model_df


def multiple_imputation(df):
    df = df.reset_index(drop=True)
    # 取出要插补的列
    df = multiple_imputation_imputer(df, TARGET_COLUMNS)
    write_excel(df, "multiple_imputation.xlsx", "multiple_imputation")
    return df


def split_train_verify_xlsx(df):
    train_df = df[df['model'] == 1]
    train_df = balance_df(train_df)
    train_df = train_df.drop('model', axis=1)
    write_excel(train_df, "train.xlsx", "train")
    verify_df = df[df['model'] == 0]
    verify_df = verify_df.drop('model', axis=1)
    verify_df = balance_df(verify_df)
    write_excel(verify_df, "verify.xlsx", "verify")
    return train_df, verify_df
