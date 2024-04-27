from collections import Counter

import numpy as np
import pandas as pd
from sklearn.linear_model import LassoCV, Lasso

from analysis.configuration import PREDICT_TARGET, MAX_COLUMN_NUMBER, INPUT_EXCEL
from utils import mprint, df_convert_number, get_column_names, multiple_imputation_imputer, filter_out_data


def lasso_corr(df, columns, random_state=0):
    df = df.reset_index(drop=True)
    df_column = df.loc[:, columns]
    x_input = df_column.values.tolist()
    y_input = df[PREDICT_TARGET].values.tolist()
    lasso_cv = LassoCV(max_iter=500000, tol=0.005, random_state=random_state)
    lasso_cv.fit(x_input, y_input)

    # 输出选择的最佳alpha参数
    mprint("Best alpha using built-in LassoCV: " + str(lasso_cv.alpha_))

    # 选择最相关的参数
    cv_mse = np.mean(lasso_cv.mse_path_, axis=1)
    alphas = lasso_cv.alphas_
    best_alpha = alphas[np.argmin(cv_mse)]
    lasso_model = Lasso(alpha=best_alpha, max_iter=1000000, random_state=random_state)
    lasso_model.fit(x_input, y_input)

    abs_coef = abs(lasso_model.coef_)
    sorted_index_list = np.argsort(-abs_coef)
    index_list = [(columns[index], abs_coef[index]) for index in sorted_index_list if abs_coef[index] > 0]
    mprint("Lasso index number: " + str(len(index_list)))

    lasso_like_map = {}
    for column_name, abs_coef_value in index_list:
        corr = df[column_name].corr(df[PREDICT_TARGET])
        lasso_like_map[column_name] = {
            "lasso": abs_coef_value,
            "corr": corr
        }

    sorted_dict_items = sorted(lasso_like_map.items(), key=lambda x: abs(x[1].get("corr")), reverse=True)
    columns = [(k, v.get("corr")) for k, v in sorted_dict_items]
    column_names = [k for k, _ in columns]
    mprint("lasso_corr: " + str(columns))
    mprint("lasso_corr_select: " + str(columns))
    return column_names[0:MAX_COLUMN_NUMBER]


if __name__ == '__main__':
    train_df = df_convert_number(pd.read_excel(INPUT_EXCEL.get("file_name")))
    train_columns = get_column_names(train_df, INPUT_EXCEL.get("start_column"), INPUT_EXCEL.get("end_column"))
    train_filter_df = filter_out_data(train_df, train_columns)
    print("lasso size: ", train_filter_df.shape)
    result_target = []
    for i in range(10):
        train_imputation_df = multiple_imputation_imputer(train_filter_df, train_columns, i)
        # train_balance_df = balance_df(train_imputation_df, i)
        train_balance_df = train_imputation_df
        result_target.extend(lasso_corr(train_balance_df, train_columns, i))
    target_counter = Counter(result_target)
    target_sorted = [k for k, v in (sorted(target_counter.items(), key=lambda x: x[1], reverse=True))]
    target = target_sorted[:MAX_COLUMN_NUMBER]
    mprint("Last lasso target: " + str(target))
