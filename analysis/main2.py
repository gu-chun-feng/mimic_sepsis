from analysis.utils import mprint
from model import train_and_predict

from analysis.configuration import TARGET_COLUMNS
from preprocess import output_train_verify, multiple_imputation, split_train_verify_xlsx


def generate_xlsx():
    model_df = output_train_verify()
    mprint("1. output_train_verify finished")
    imputer_df = multiple_imputation(model_df)
    mprint("2. multiple_imputation finished")
    train_df, _ = split_train_verify_xlsx(imputer_df)
    mprint("3. split_train_verify_xlsx finished")


if __name__ == '__main__':
    if not any(TARGET_COLUMNS):
        print("TARGET_COLUMNS must not empty")
        exit()
    generate_xlsx()
    train_and_predict()
