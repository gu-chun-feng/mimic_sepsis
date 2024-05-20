import os

import numpy as np
import torch
import torch.nn as nn
import xgboost as xgb
from matplotlib import pyplot as plt
from sklearn import svm
from sklearn.metrics import confusion_matrix
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier

from analysis.configuration import TARGET_COLUMNS, PREDICT_TARGET, OUTPUT_DIR
from utils import read_excel, mprint, write_excel

import seaborn as sns


def dnn_model(x_train, x_predict, y_train, y_predict):
    model = nn.Sequential(
        nn.Linear(np.size(x_train, 1), 96),
        nn.Linear(96, 64),
        nn.Dropout(p=0.01),
        nn.Sigmoid(),
        nn.Linear(64, 16),
        nn.Dropout(p=0.01),
        nn.Sigmoid(),
        nn.Linear(16, 2)
    )
    # 定义损失函数和优化器
    loss_fn = nn.MSELoss(reduction='sum')
    optimizer = torch.optim.Adam(model.parameters(), lr=0.0002)

    train_input = torch.Tensor(x_train)
    train_output = torch.Tensor([[1, 0] if y >= 0.5 else [0, 1] for y in y_train])
    for i in range(12000):
        optimizer.zero_grad()
        y_pred = model(train_input)
        loss = loss_fn(y_pred, train_output)
        loss.backward()
        optimizer.step()
        if (i + 1) % 1000 == 0:
            print("dnn_model: ", i + 1, ' Current Loop: ', loss.item())

    predicts = model(torch.Tensor(x_predict)).detach().numpy()
    y_pred = [0 if np.linalg.norm(predict - [1, 0]) > np.linalg.norm(predict - [0, 1]) else 1
              for predict in predicts]
    rights = [y == p for y, p in zip(y_predict, y_pred)]
    print("dnn %s: correct ratio: %.4f%%." % (
        PREDICT_TARGET, 100 * len([right for right in rights if right]) / len(y_predict)))
    return y_pred


def train_and_predict():
    target = TARGET_COLUMNS
    train_df = read_excel("train.xlsx")
    verify_df = read_excel("verify.xlsx")
    x_train = train_df.loc[:, target]
    y_train = train_df.loc[:, PREDICT_TARGET]
    x_predict = verify_df.loc[:, target]
    y_predict = verify_df.loc[:, PREDICT_TARGET]
    x_train, x_predict, y_train, y_predict = x_train.values, x_predict.values, y_train.values, y_predict.values

    models = [{
        "knn_model": KNeighborsClassifier(n_neighbors=16),
        "xgb_model": xgb.XGBRegressor(objective='reg:squarederror', colsample_bytree=0.3, learning_rate=0.02,
                                      max_depth=5, alpha=10, n_estimators=20),
        "svm_model": svm.SVC(kernel='rbf', degree=5),
        "tree_model": DecisionTreeClassifier(random_state=i)} for i in range(10)]

    for model_name in ["knn_model", "xgb_model", "svm_model", "tree_model"]:
        correct_list = []
        for i, model in enumerate(models):
            model_bin = model[model_name]
            model_bin.fit(x_train, y_train)
            # 预测新样本类别
            predict_result_real = model_bin.predict(x_predict)
            predict_result = np.round(predict_result_real)
            rights = [y == p for y, p in zip(y_predict, predict_result)]
            correct = 100 * len([right for right in rights if right]) / len(y_predict)
            correct_list.append(correct)
            mprint("%s %s %d: correct ratio %.4f%%." % (model_name, PREDICT_TARGET, i, correct))
            if i == 0:
                verify_df["model_" + PREDICT_TARGET] = predict_result
                verify_df["model_real_" + PREDICT_TARGET] = predict_result_real
                write_excel(verify_df, model_name + "_verify.xlsx", "verify")

                plt.figure(figsize=(8, 6))
                cm = confusion_matrix(y_predict, predict_result)
                sns.heatmap(cm, fmt='d', cmap='Blues', annot=True, cbar=False)
                plt.title(model_name)
                plt.xlabel('Predicted')
                plt.ylabel('Truth')
                plt.savefig(os.path.join(OUTPUT_DIR, PREDICT_TARGET + "_" + model_name) + '.png', dpi=300)
                plt.show()

        mprint("%s %s Average: correct ratio %.4f%%." %
               (model_name, PREDICT_TARGET, sum(correct_list) / len(correct_list)))
        mprint("#############################################")

    y_pred = dnn_model(x_train, x_predict, y_train, y_predict)
    verify_df["model_" + PREDICT_TARGET] = y_pred
    write_excel(verify_df, "dnn_model_verify.xlsx", "verify")

    plt.figure(figsize=(8, 6))
    cm = confusion_matrix(y_predict, y_pred)
    sns.heatmap(cm, fmt='d', cmap='Blues', annot=True, cbar=False)
    plt.title("dnn_model")
    plt.xlabel('Predicted')
    plt.ylabel('Truth')
    plt.savefig(os.path.join(OUTPUT_DIR, PREDICT_TARGET + '_dnn_model') + '.png', dpi=300)
    plt.show()
